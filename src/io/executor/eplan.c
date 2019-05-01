/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/executor/eplan.h"
#include "io/executor/estep_conn.h"
#include "io/utils/SList.h"

struct EplanPutConnsContext
{
	SList conns;
};

static void exec_eplan_putconns(void *context, PlannedStmt pstmt,
	 							PlannedStmtShard pstmtshd, BackEndConnection nconn,
	 							bool from_connection_catalog);


ExecPlan
vh_exec_eplan_create(void)
{
	MemoryContext mctx_old, mctx_ep;
	ExecPlan ep;

	mctx_old = vh_mctx_current();
	mctx_ep = vh_MemoryPoolCreate(mctx_old, 8192,
								  "Execution Plan context");

	vh_mctx_switch(mctx_ep);

	ep = vhmalloc(sizeof(struct ExecPlanData));
	memset(ep, 0, sizeof(struct ExecPlanData));

	ep->mctx_ep = mctx_ep;
	ep->mctx_result = 0;
	ep->shards = vh_SListCreate();

	vh_mctx_switch(mctx_old);

	return ep;
}

void
vh_exec_eplan_destroy(ExecPlan ep)
{
	vh_mctx_destroy(ep->mctx_ep);
}

/*
 * vh_exec_eplan_ready
 *
 * After the planner has run, we need to do some cleanup work to get the
 * ExecPlan in a ready state.  A ready state means it can be run by the
 * executor.  At the moment, all we do is pull out the unique shards accessed
 * in the tree and shove them into the |shards| SList.
 */
bool
vh_exec_eplan_ready(ExecPlan ep)
{
	if (ep->plan)
	{
		ep->shards = vh_es_shard_unique(ep->plan);

		if (ep->shards)
			return true;
	}

	return false;
}

/*
 * vh_exec_eplan_putconns
 *
 * Traverses the ExecStep tree pulling all PlannedStmtShard and obtaining
 * NodeConnection objects from the ConnectionCatalog.
 *
 * |kvm| has a ShardAccess as the key and a NodeConnection as the value.
 *
 * Returns an SList of NodeConnection this call gathered from the provided
 * ConnectionCatalog |cc|.
 */

SList
vh_exec_eplan_putconns(ExecPlan ep, ConnectionCatalog cc,
					   KeyValueMap kvm)
{
	MemoryContext mctx_old;
	struct EplanPutConnsContext epcc = { };
	bool conns_put = true;

	mctx_old = vh_mctx_switch(ep->mctx_ep);

	VH_TRY();
	{
		if (ep->plan)
			conns_put &= vh_es_putconns(ep->plan, cc, &kvm, 
										exec_eplan_putconns, &epcc, true);

		if (ep->on_commit)
			conns_put &= vh_es_putconns(ep->on_commit, cc, &kvm, 
										exec_eplan_putconns, &epcc, true);

		if (ep->on_rollback)
			conns_put &= vh_es_putconns(ep->on_rollback, cc, &kvm, 
										exec_eplan_putconns, &epcc, true);
	}
	VH_CATCH();
	{
		vh_mctx_switch(mctx_old);
		ep->conns_put = false;

		return 0;
	}
	VH_ENDTRY();

	ep->conns_put = conns_put;

	if (conns_put)
		return epcc.conns;

	return 0;
}

void vh_exec_eplan_relconns(ExecPlan ep, ConnectionCatalog cc,
							KeyValueMap kvm_exclude)
{
	if (ep->plan)
		vh_es_relconns(ep->plan, cc, kvm_exclude); 

	if (ep->on_commit)
		vh_es_relconns(ep->on_commit, cc, kvm_exclude);

	if (ep->on_rollback)
		vh_es_relconns(ep->on_rollback, cc, kvm_exclude);

	ep->conns_put = false;
}

static void 
exec_eplan_putconns(void *context, PlannedStmt pstmt,
	 				PlannedStmtShard pstmtshd, BackEndConnection nconn,
	 				bool from_connection_catalog)
{
	struct EplanPutConnsContext *epcc = context;

	if (from_connection_catalog)
	{
		if (!epcc->conns)
			epcc->conns = vh_SListCreate();

		vh_SListPush(epcc->conns, nconn);
	}
}

