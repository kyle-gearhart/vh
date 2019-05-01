/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/plan/pstmt.h"
#include "io/executor/eplan.h"
#include "io/executor/eresult.h"
#include "io/executor/estep.h"
#include "io/executor/estepfwd.h"
#include "io/executor/estep_run.h"
#include "io/executor/htc.h"
#include "io/executor/htc_idx.h"
#include "io/executor/htc_returning.h"
#include "io/executor/htc_slist.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/nodes/NodeFrom.h"
#include "io/utils/SList.h"

static void es_runstep(ExecStep es, void* esd);
static void es_finishstep(ExecStep es, void* esd);

static void es_discard_run(ExecStepDiscard, ExecState);

static void es_funnel_run(ExecStepFunnel, ExecState);
static void es_funnel_finish(ExecStepFunnel, ExecState);
static void es_fetch_run(ExecStepFetch, ExecState);
static void es_fetch_run_slist(ExecStepFetch, ExecState);
static void es_fetch_run_returning(ExecStepFetch, ExecState);
static void es_fetch_finish(ExecStepFetch, ExecState);

static void es_transfer_tups(ExecState estate, SList tups);

int32_t
vh_es_runtree(ExecState estate, ExecStep root)
{
	vh_es_visit_tree_func es_func[2];
	void *es_data[2];

	es_func[0] = es_runstep;
	es_func[1] = es_finishstep;
	es_data[0] = estate;
	es_data[1] = estate;

	vh_es_visit_tree(root, es_func, es_data);

	return 0;
}

ExecState 
vh_es_open(void)
{
	ExecState estate;
	CatalogContext cc;

	cc = vh_ctx();

	estate = vhmalloc(sizeof(struct ExecStepData));

	if (cc)
		estate->cc = cc->catalogConnection;

	estate->mctx_work = vh_MemoryPoolCreate(vh_mctx_current(),
											8192,
											"Executor Working context");
	estate->mctx_result = 0;
	estate->er = 0;

	return estate;
}


void 
vh_es_reset(ExecState es)
{
	vh_mctx_destroy(es->mctx_work);
	
	es->mctx_work = vh_MemoryPoolCreate(es->mctx_result,
										8192,
										"Executor working context after reset");
}

void
vh_es_close(ExecState estate)
{
	vh_mctx_destroy(estate->mctx_work);
	vhfree(estate);
}

static void 
es_runstep(ExecStep es, void* esd)
{
	ExecState estate = esd;

	switch (es->tag)
	{
	case EST_Discard:
		es_discard_run((ExecStepDiscard) es, estate);
		break;

	case EST_Fetch:
		es_fetch_run((ExecStepFetch) es, estate);
		break;
	
	case EST_Funnel:
		es_funnel_run((ExecStepFunnel) es, estate);
		break;

	default:
		break;
	}
}

static void 
es_finishstep(ExecStep es, void* esd)
{
	PlannedStmt pstmt = 0;
	PlannedStmtShard pstmtshd = 0;

	switch (es->tag)
	{
		case EST_Fetch:
			es_fetch_finish((ExecStepFetch)es, esd);
			break;

	default:
		break;
	}

	/*
	 * Finalize the PlannedStmt at the end of each step if the
	 * step has one.
	 */

	if (vh_es_pstmt(es, &pstmt, &pstmtshd))
	{
		assert(pstmt);
		assert(pstmtshd);

		vh_pstmt_finalize(pstmt);
		vh_pstmts_finalize(pstmtshd);
	}
}


/*
 * Funnel Operations
 */
static void 
es_funnel_run(ExecStepFunnel esf, ExecState estate)
{
	if (!esf->tups)
		esf->tups = vh_SListCreate_ctx(estate->mctx_result);	
}

static void 
es_funnel_finish(ExecStepFunnel esf, ExecState estate)
{
	es_transfer_tups(estate, esf->tups);
}

static void
es_fetch_run(ExecStepFetch esf, ExecState estate)
{
	if (esf->returning)
		es_fetch_run_returning(esf, estate);
	else
		es_fetch_run_slist(esf, estate);
}

/*
 * es_fetch_run_returning
 */

static void
es_fetch_run_returning(ExecStepFetch esfetch, ExecState estate)
{
	struct HTC_ReturningCtx htc = { };
	struct BackEndExecPlanData beep = { };
	PlannedStmt pstmt;
	PlannedStmtShard pstmtshd;
	NodeQueryInsert nqins;
	MemoryContext mctx_execnode, mctx_old;

	assert(esfetch->pstmt);
	assert(esfetch->pstmt->be);

	pstmt = esfetch->pstmt;
	pstmtshd = esfetch->pstmtshd;
	nqins = (NodeQueryInsert)pstmt->nquery;
	
	mctx_execnode = vh_MemoryPoolCreate(estate->mctx_work,
										1024,
										"Executor Collect Node Working Context");

	/*
	 * Make sure our HTC Returning allocates into the executor working context
	 * so that it just gets blasted away when we're done executing.
	 */

	mctx_old = vh_mctx_switch(mctx_execnode);
	vh_htc_returning_init(&htc, nqins->into->htps, nqins->rfields);
	vh_mctx_switch(mctx_old);
	
	htc.htci.htc_cb = vh_htc_returning;
	htc.htci.hbno = esfetch->hbno;
	htc.htci.result_ctx = estate->mctx_result;

	beep.pstmt = pstmt;
	beep.pstmtshd = esfetch->pstmtshd;
	beep.mctx_work = mctx_execnode;
	beep.mctx_result = estate->mctx_result;
	beep.htc_info = &htc.htci;
	beep.stat_wait_count = 0;
	beep.discard = false;

	if (vh_be_exec(pstmtshd->nconn, &beep))
	{
		/*
		 * We had an error that we should probably propagate up!
		 */

		vh_mctx_destroy(mctx_execnode);

		elog(ERROR,
				emsg("Back end query exection failed against the query (%s)",
					 vh_str_buffer(esfetch->pstmtshd->command)));

		return;
	}

	printf("\n=================================================\n"
		   "EXECUTOR QUERY STATISTICS FOR:\n%s\n\n"
		   " query execution:\t%'d ms\n"
		   " heaptuple:\t\t%'d ms\n"
		   " total:\t\t\t%'d ms\n"
		   " records processed:\t%'d\n"
		   " wait count:\t\t%ld\n"
		   "=================================================\n\n",
		   vh_str_buffer(esfetch->pstmtshd->command),
		   beep.stat_qexec,
		   beep.stat_htform,
		   beep.stat_qexec + beep.stat_htform,
		   beep.htc_info->nrows,
		   beep.stat_wait_count);

	vh_mctx_destroy(mctx_execnode);
}

/*
 * es_fetch_run_slist
 *
 * We know at a minimum we're going to spinning up an SList HTC.  We may also
 * use an Index HTC and pipeline the results from the Index to the SList.  So
 * we always setup the SList HTC and then optionally setup the Index HTC.  By
 * default, the calling convention for vh_htc_idx_init requires a pipeline and
 * info structure for the pipeline.  For the Fetch node, this will be an SList.
 */
static void 
es_fetch_run_slist(ExecStepFetch esfetch, ExecState estate)
{
	struct HTC_SListCtx htc_slist = { };
	struct HTCIndexData htc_idx = { };
	struct BackEndExecPlanData beep = { };
	HeapTupleCollectorInfo htc_info;
	ExecStep esparent;
	PlannedStmt pstmt;
	PlannedStmtShard pstmtshd;
	MemoryContext mctx_execnode;
	int32_t i;
	
	assert(esfetch->pstmt);
	assert(esfetch->pstmt->be);
	assert(esfetch->pstmtshd);

	pstmt = esfetch->pstmt;
	pstmtshd = esfetch->pstmtshd;
	
	htc_slist.esz = pstmt->qrp_ntables;

	if ((esparent = esfetch->es.parent))
	{
		if (esparent->tag == EST_Funnel)
		{
			esfetch->tups = ((ExecStepFunnel)esparent)->tups;
		}
	}
	else
	{
		vh_htp_SListCreate_ctx(esfetch->tups, estate->mctx_result);
	}

	htc_slist.tups = esfetch->tups;
	htc_slist.htci.htc_cb = vh_htc_slist;
	htc_slist.htci.hbno = esfetch->hbno;
	htc_slist.htci.result_ctx = estate->mctx_result;


	mctx_execnode = vh_MemoryPoolCreate(estate->mctx_work,
										1024,
										"Executor Collect Node Working Context");
	if (esfetch->indexed)
	{
		vh_htc_idx_init(&htc_idx, pstmt->qrp_ntables, vh_htc_slist, &htc_slist);

		for (i = 0; i < pstmt->qrp_ntables; i++)
			vh_htc_idx_add(&htc_idx, i, esfetch->hfs[i], esfetch->nhfs[i]);

		htc_info = &htc_idx.htci;
	}
	else
	{
		htc_info = &htc_slist.htci;
	}

	beep.pstmt = esfetch->pstmt;
	beep.pstmtshd = esfetch->pstmtshd;
	beep.mctx_work = mctx_execnode;
	beep.mctx_result = estate->mctx_result;
	beep.htc_info = htc_info;
	beep.stat_wait_count = 0;
	beep.discard = false;

	if (vh_be_exec(pstmtshd->nconn, &beep))
	{
		/*
		 * We had an error
		 */
	}

/*
	printf("\n=================================================\n"
		   "EXECUTOR QUERY STATISTICS FOR:\n%s\n\n"
		   " query execution:\t%'d ms\n"
		   " heaptuple:\t\t%'d ms\n"
		   " total:\t\t\t%'d ms\n"
		   " records processed:\t%'d\n"
		   " wait count:\t\t%ld\n"
		   "=================================================\n\n",
		   vh_str_buffer(esfetch->pstmtshd->command),
		   beep.stat_qexec,
		   beep.stat_htform,
		   beep.stat_qexec + beep.stat_htform,
		   beep.htc_info->nrows,
		   beep.stat_wait_count);
*/
	if (esfetch->indexed)
	{
		vh_htc_idx_destroy(&htc_idx, true);
	}

	vh_mctx_destroy(mctx_execnode);
}

static void
es_fetch_finish(ExecStepFetch esfetch, ExecState estate)
{
	ExecResult er;
	int i = 0, qrp_ntables = esfetch->pstmt->qrp_ntables;
	bool rel_tabledef = esfetch->pstmt->latebinding;
	TableDefSlot *slots = 0;
	QrpTableProjection qrp_table = esfetch->pstmt->qrp_table;
	MemoryContext mctx_old;

	mctx_old = vh_mctx_switch(estate->mctx_result);

	er = estate->er = vh_exec_result_create(esfetch->pstmt->qrp_ntables);
	er->er_shouldreltups = true;

	slots = &er->slots[0];

	for (i = 0; i < qrp_ntables; i++)
		vh_slot_td_store(&slots[i],
						 qrp_table[i].rtdv,
						 rel_tabledef, false);

	if (!esfetch->es.parent)
		es_transfer_tups(estate, esfetch->tups);

	vh_mctx_switch(mctx_old);
}

static void 
es_transfer_tups(ExecState estate, SList tups)
{
	estate->er->tups = tups;
}

/*
 * es_discard_run
 *
 */
static void 
es_discard_run(ExecStepDiscard esdiscard, ExecState estate)
{
	struct BackEndExecPlanData beep = { };
	HeapTupleCollectorInfo htc_info = 0;
	PlannedStmt pstmt;
	BackEnd be;
	vh_beat_exec be_exec;
	MemoryContext mctx_execnode;
	
	assert(esdiscard->pstmt);
	assert(esdiscard->pstmt->be);

	pstmt = esdiscard->pstmt;
	be = pstmt->be;
	be_exec = be->at.exec;
	
	if (be_exec)
	{
		mctx_execnode = vh_MemoryPoolCreate(estate->mctx_work,
											1024,
											"Executor Collect Node Working Context");
		beep.pstmt = esdiscard->pstmt;
		beep.pstmtshd = esdiscard->pstmtshd;
		beep.mctx_work = mctx_execnode;
		beep.mctx_result = estate->mctx_result;
		beep.htc_info = htc_info;
		beep.stat_wait_count = 0;
		beep.discard = true;

		be_exec(&beep);

		vh_mctx_destroy(mctx_execnode);
	}
}

