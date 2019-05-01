/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/executor/eplan.h"
#include "io/executor/estep.h"
#include "io/executor/estep_conn.h"
#include "io/executor/estep_run.h"
#include "io/executor/exec.h"
#include "io/executor/xact.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/plan/plan.h"
#include "io/plan/pstmt_funcs.h"
#include "io/utils/SList.h"

static void exec_fill_missing_popts(PlannerOpts *popts);
static void exec_set_base_popts(PlannerOpts *popts);

ExecResult
vh_exec_node(Node node)
{
	PlannerOpts popts = { };
	ExecPlan ep;
	ExecResult er;

	exec_set_base_popts(&popts);
	ep = vh_plan_node_opts(node, popts);

	if (ep)
	{
		er = vh_exec_ep(ep);

		return er;
	}
	
	elog(ERROR1,
			emsg("Planner fatal error, unable to plan query [%p] as "
				 "requested",
				 node));

	
	return 0;
}

ExecResult
vh_exec_node_opts(Node root, PlannerOpts popts)
{
	ExecPlan ep;
	ExecResult er;

	exec_fill_missing_popts(&popts);

	ep = vh_plan_node_opts(root, popts);

	if (ep)
	{
		//if (popts.bec)
		//	ep->conns_put = true;

		er = vh_exec_ep(ep);
		vh_exec_eplan_destroy(ep);

		return er;		
	}

	elog(ERROR1,
			emsg("Planner fatal error, unable to plan query [%p] as "
				 "requested",
				 root));

	return 0;
}

ExecResult
vh_exec_ep(ExecPlan ep)
{
	bool release_conns = false;
	ExecState es;
	ExecResult result;
	SList conns_to_release;
	BackEndConnection *nconn_head, nconn;
	uint32_t nconn_sz, i;
	int32_t err;

	es = vh_es_open();

	if (!es)
	{
		elog(ERROR2,
			 emsg("Unable to create an eExecState!"));

		return 0;
	}

	if (!ep->conns_put)
	{
		conns_to_release = vh_exec_eplan_putconns(ep, es->cc,  0);
	
		if (ep->conns_put && conns_to_release)
		{
			release_conns = true;
		}
		else if (!ep->conns_put)
		{
			vh_exec_eplan_relconns(ep, es->cc, 0);

			elog(ERROR1,
				 emsg("Unable to obtain required connections to run the "
					  "ExecPlan.  Check to make sure no connection deadlock "
					  "exists!"));

			return 0;
		}
	}

	es->mctx_result = ep->mctx_result;
	err = vh_es_runtree(es, ep->plan);

	if (err == 0)
	{
		result = es->er;

		if (ep->on_commit)
		{
			vh_es_reset(es);
			vh_es_runtree(es, ep->on_commit);
		}
	}
	else
	{
		result = 0;

		if (ep->on_rollback)
		{
			vh_es_reset(es);
			vh_es_runtree(es, ep->on_rollback);
		}
	}

	if (release_conns && conns_to_release)
	{
		nconn_sz = vh_SListIterator(conns_to_release, nconn_head);

		for (i = 0; i < nconn_sz; i++)
		{
			nconn = nconn_head[i];
			vh_ConnectionReturn(es->cc, nconn);
		}
	}

	vh_es_close(es);

	return result;
}

/*
 * vh_exec_query_str
 *
 * Bypass the planner by generating an ExecStepFetch.  Use the command the
 * user passed in with no parameters to form the planned statement.
 */

ExecResult
vh_exec_query_str_param(BackEndConnection bec, const char *query,
						TypeVarSlot *params, int32_t n_params)
{
	CatalogContext cc = vh_ctx();
	ExecPlan ep;
	ExecState es;
	ExecResult er;
	struct ExecStepFetchData* esf;
	MemoryContext mctx_old;
	bool pstmt_create;
	PlannedStmt pstmt = 0;
	PlannedStmtShard pstmtshd = 0;


	/*
	 * Let's check a few things with the Back End Connection to make sure someone
	 * didn't try to yank this out of the transaction manager.
	 */

	if (bec->intx)
		elog(ERROR2,
			 emsg("Unable to use back end connection provided to vh_exec_query_str "
				  "with the query (%s), the backend is being used by a transaction "
				  "manager.",
				  query));

	if (bec->in2pc)
		elog(ERROR2,
			 emsg("Unable to use back end connection provided to vh_exec_query_str "
				  "with query (%s), the back end is in a two phase transaction commit "
				  "with the transaction manager.",
				  query));

	pstmt_create = vh_pstmt_generate_from_query_str(bec, query,
			   										&pstmt,
		   											&pstmtshd,
													params,
													n_params);

	if (pstmt_create)
		elog(ERROR2,
			 emsg("Unable to generate a PlannedStmt from the query (%s) for the backend %s",
				  query, bec->be->name));

	ep = vh_exec_eplan_create();
	es = vh_es_open();

	assert(es->mctx_work);

	mctx_old = vh_mctx_switch(es->mctx_work);
	es->mctx_result = mctx_old;
	ep->mctx_result = mctx_old;

	esf = vh_es_create(EST_Fetch, 0);
	esf->indexed = false;
	esf->returning = false;
	esf->pstmt = pstmt;
	esf->pstmtshd = pstmtshd;

	if (cc->xactCurrent)
	{
		esf->hbno = vh_xact_hbno(cc->xactCurrent);
	}
	else
	{
		esf->hbno = cc->hbno_general;
	}

	assert(esf->hbno);

	ep->plan = &esf->es;
	ep->conns_put = true;

	if (vh_es_runtree(es, ep->plan))
	{
		vh_es_close(es);
		vh_exec_eplan_destroy(ep);

		elog(ERROR2,
		 	 emsg("Execution of the raw query string (%s) failed against back end "
			 	  "[%s]!",
				  query,
				  bec->be->name));
	}

	er = es->er;

	vh_es_close(es);	
	vh_exec_eplan_destroy(ep);

	vh_mctx_switch(mctx_old);

	return er;
}

static void 
exec_set_base_popts(PlannerOpts *popts)
{
	CatalogContext cc = vh_ctx();

	if (cc)
	{
		popts->hbno = cc->hbno_general;
		popts->mctx_result = vh_mctx_current();
	}
}

static void 
exec_fill_missing_popts(PlannerOpts *popts)
{
	CatalogContext cc = vh_ctx();
	
	if (cc)
	{
		if (!popts->hbno)
			popts->hbno = cc->hbno_general;
		
		if (!popts->mctx_result)
			popts->mctx_result = vh_mctx_current();
	}
}



