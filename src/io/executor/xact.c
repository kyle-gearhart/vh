/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/buffer/BuffMgr.h"
#include "io/catalog/BackEnd.h"
#include "io/executor/eplan.h"
#include "io/executor/estep_conn.h"
#include "io/executor/estep_run.h"
#include "io/executor/xact.h"
#include "io/plan/plan.h"
#include "io/plan/pstmt.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/shard/Shard.h"
#include "io/utils/kvmap.h"
#include "io/utils/kset.h"
#include "io/utils/SList.h"

typedef struct SavePointData *SavePoint;

struct PutConnsContext
{
	SavePoint sp;
	XAct xact;
	KeyValueMap nconn_read;			/* key: ShardAccess, value: BackEndConnection */
	bool begin_tx;

	uint32_t sp_conn_attached;
	uint32_t sp_conn_attach_fail;
	uint32_t sp_conn_no_support;
	uint32_t sp_conn_no_2pc_support;

	uint32_t sp_conn_no_tx_support;
	uint32_t sp_conn_tx_begin_fail;

	uint32_t general_error;
};

struct SavePointData
{
	MemoryContext mctx;
	uint32_t idx;
	String name;
	XAct owner;
	SList plans;
	KeySet shards;					/* key: ShardAccess */
	uint32_t sp_flushedthru_idx;
	bool sp_flushed;
	bool sp_committed;
	bool sp_rolled_back;
};

struct XActData
{
	XAct xact_top;
	XAct xact_parent;
	HeapBufferNo hbno;
	MemoryContext mctx;

	SavePoint sp_local;
	uint32_t sp_flushedthru_idx;
	bool sp_flushed;

	
	bool xactopen;
	bool sxactopen;
	bool twophase;
	
	/* Only populated when xact_top == self AND
	 * xact_parent == 0 */
	XAct xact_child;
	XAct xact_bottom;
	XActMode mode;
	
	SList sps;				/* SavePoint */
	SavePoint sp_current;
	SavePoint sp_lastflushed;

	ConnectionCatalog cat_conn;
	KeyValueMap nconns;		/* key: ShardAccess; value: BackEndConnection */
};

static SavePoint xact_sp_get_by_idx(XAct xact, uint32_t idx);
static SavePoint xact_sp_create(XAct xact);
static SavePoint xact_sp_preceeding(XAct xact);

/*
 * ExecPlan Creation Functions
 */
static ExecPlan xact_sp_create_ep_from_node(SavePoint sp, NodeQuery nq);
static ExecPlan xact_create_ep_from_node(XAct sxact, NodeQuery nq);
static void xact_plan_popts_init(XAct sxact, PlannerOpts *popts);

/*
 * ExecPlan Execution Functions
 */

static bool xact_es_runtree_plan(SavePoint sp_target, ExecState es);
static ExecResult xact_es_runtree_read(XAct sxact, ExecState es);
static bool xact_sp_rollbackto(SavePoint sp, bool xact_local);
static bool xact_sp_flushthru(SavePoint sp);

/*
 * Control functions
 */
static bool xact_commit_all(XAct txact);
static bool xact_rollback_all(XAct txact);

struct XActRunTreeContext
{
	ExecState es;
	BackEndConnection nconn;
	bool tree_commit;
	bool success;
};

static bool xact_runtree_rollback(XAct txact, BackEndConnection nconn);
static bool xact_runtree_commit(XAct sxact, BackEndConnection nconn);
static void xact_runtree_cb(XAct txact, SavePoint sp, ExecPlan ep,
							uint32_t sp_idx, uint32_t ep_idx,
							void *data);

struct XActMarkActionFor
{
	KeySet ks_nconns;
	bool action_commit;
};

static void xact_mark_actionfor_commit(XAct txact, KeySet ks_processed, 
		   							   BackEndConnection nconn);
static void xact_mark_actionfor_rollback(XAct txact, KeySet ks_processed,
										 BackEndConnection nconn);
static void xact_mark_actionfor_cb(XAct txact, SavePoint sp,
									 uint32_t sp_idx, void *cb_data);

/*
 * Connection Maintenance Functions
 */
static void xact_retconns(XAct txact);
static void xact_retconns_read(XAct txact, KeyValueMap readers);
static void xact_putconns_cb(void *putconnsctx, PlannedStmt pstmt,
							 PlannedStmtShard pstmtshd, BackEndConnection nconn,
							 bool from_connection_catalog);


/*
 * Helper Functions
 */

typedef void (*xact_iterate_ep_cb)(XAct txact, SavePoint sp, ExecPlan ep,
	 							   uint32_t sp_idx, uint32_t ep_idx,
	 							   void *cb_data);
static void xact_iterate_ep(XAct txact, xact_iterate_ep_cb cb, void *cb_data);


typedef void (*xact_iterate_sp_cb)(XAct txact, SavePoint sp, 
								   uint32_t sp_idx, void *cb_data);
static void xact_iterate_sp(XAct txact, xact_iterate_sp_cb cb, void *cb_data);
static void xact_iterate_sp_filter_nconn(XAct txact, xact_iterate_sp_cb cb,
										 void *cb_data,
										 BackEndConnection filter_nconn);



#define IsTopXAct(xact)	(xact->xact_top == xact && xact->xact_parent == 0)
#define TopXAct(xact) (xact ? xact->xact_top : 0)

XAct
vh_xact_create(XActMode mode)
{
	CatalogContext cc = vh_ctx();
	XAct txact, xact;
	MemoryContext mctx_old, mctx_xact;

	if (cc)
	{
		txact = cc->xactTop;

		if (txact)
		{
			mctx_xact = vh_MemoryPoolCreate(txact->mctx, 8192, "Sub XAct context");
			mctx_old = vh_mctx_switch(mctx_xact);

			xact = vhmalloc(sizeof(struct XActData));
			xact->xact_top = txact;
			xact->xact_parent = txact->xact_child;
			xact->mctx = mctx_xact;

			xact->xact_child = 0;
			xact->sps = 0;
			xact->sp_current = 0;
			xact->twophase = false;

			xact->nconns = 0;
			cc->xactCurrent->xact_child = xact;
			cc->xactCurrent = xact;
			txact->xact_bottom = xact;
		}
		else
		{
			mctx_xact = vh_MemoryPoolCreate(cc->memoryTop, 8192, "Top XAct context");
			mctx_old = vh_mctx_switch(mctx_xact);

			xact = vhmalloc(sizeof(struct XActData));
			xact->xact_top = xact;
			xact->xact_parent = 0;
			xact->mctx = mctx_xact;

			xact->xact_child = 0;
			xact->xact_bottom = 0;

			xact->sps = vh_SListCreate();
			xact->sp_current = 0;
			xact->sp_lastflushed = 0;

			xact->twophase = false;

			xact->cat_conn = cc->catalogConnection;

			xact->mode = mode;

			xact->nconns = vh_kvmap_create();

			cc->xactTop = xact;
			cc->xactCurrent = xact;
		}

		xact->hbno = vh_hb_open(xact->mctx);
		xact->sp_local = 0;
		xact->sp_flushed = false;
		xact->xactopen = false;
		xact->sxactopen = false;

		mctx_xact = vh_mctx_switch(mctx_old);

		return xact;
	}
	else
	{
		elog(ERROR2,
			 emsg("A transaction cannot be started when the CatalogContext "
				  "is unavailable.  Review the vh_ctx() "
				  "implementation."));

		return 0;
	}

	return 0;
}

void
vh_xact_destroy(XAct xact)
{
	CatalogContext cc = vh_ctx();
	XAct txact, cxact;

	txact = TopXAct(xact);

	if (cc)
	{
		if (txact->xact_bottom == xact)
		{
			txact->xact_bottom = xact->xact_parent;
			cc->xactCurrent = txact->xact_bottom;
		}

		if (txact->xact_child == xact)
		{
			txact->xact_child = 0;
			txact->xact_bottom = 0;
			cc->xactCurrent = txact;
		}

		if (txact == xact)
		{
			cc->xactTop = 0;
			cc->xactCurrent = 0;
		}

		if (xact->xact_parent)
		{
			txact->sp_current = xact->xact_parent->sp_local;
		}

		cxact = xact->xact_child;
		while (cxact)
		{
			vh_hb_close(cxact->hbno);
			cxact = cxact->xact_child;
		}
		
		vh_hb_close(xact->hbno);
		vh_mctx_destroy(xact->mctx);
	}
}

/*
 * Unless this is the top level transaction, just create a SavePoint.  Only the
 * top level transaction can submit a commit message to all impacted back ends.
 *
 * If this isn't the top level transaction, but there are no actions waiting
 * elsewhere, go ahead and let the full commit fire.
 */  
bool
vh_xact_commit(XAct sxact)
{
	XAct txact = TopXAct(sxact);
	SavePoint sp;

	if (IsTopXAct(sxact))
	{
		if (txact->sp_current)
		{
			if (xact_sp_flushthru(txact->sp_current))
				return xact_commit_all(txact);
			else
				return false;
		}
		else
		{
			return xact_commit_all(txact);
		}
	}
	else
	{
		sp = xact_sp_create(sxact);

		if (sp)
			return true;
	}

	return false;
}

/*
 * Runs commit statements on each outstanding connection and then returns them.
 */
static bool
xact_commit_all(XAct txact)
{
	BackEndConnection *nconn, nconn_last_commit;
	ShardAccess sa;
	KeySet ks_committed_nconns;
	KeyValueMapIterator it;

	bool committed, all_committed = true;

	assert(IsTopXAct(txact));

	ks_committed_nconns = vh_kset_create();

	vh_kvmap_it_init(&it, txact->nconns);
	while (vh_kvmap_it_next(&it, &sa, &nconn))
	{
		committed = vh_be_xact_commit(*nconn);

		/*
		 * If we cannot commit a single tranaction in the batch we need to
		 * figure out what to do.  If we're in a two phase commit cycle
		 * (i.e. multiple connections involved and they all support 2pc) 
		 * we can just step backwards and undo the two phase commit.  Call
		 * the rollback action on all affected plans.
		 *
		 * When we do commit, we should update each SavePoint's commit status.
		 * This way if we get halfway thru batch commit and something goes
		 * wrong we've got some idea which records to commit.
		 */

		if (committed)
		{
			nconn_last_commit = *nconn;
			vh_kset_key(ks_committed_nconns, nconn);

			xact_mark_actionfor_commit(txact, ks_committed_nconns, *nconn);
			xact_runtree_commit(txact, *nconn);

			if (nconn_last_commit)
			{
			}
		}
		else
		{
			all_committed = false;
		}
	}
	
	xact_retconns(txact);

	vh_kset_destroy(ks_committed_nconns);

	return all_committed;
}

/*
 * Unless this is the top level transaction, just rollback to the preceeding
 * SavePoint owned by this transaction (xact_sp_preceeding).  This will issue
 * rollback commands on all connections used between the current SavePoint
 * and the preceeding SavePoint.
 *
 * When we're the top level transaction, rollback on every connection.
 */
bool
vh_xact_rollback(XAct sxact)
{
	XAct txact = TopXAct(sxact);
	SavePoint sp;

	if (IsTopXAct(sxact))
	{
		return xact_rollback_all(txact);
	}
	else
	{
		sp = sxact->sp_local;

		if (sp)
		{
			return xact_sp_rollbackto(sp, false);
		}
	}

	return true;
}

static bool
xact_rollback_all(XAct txact)
{
	BackEndConnection nconn, nconn_last_commit, *nconn_it;
	ShardAccess sa;
	KeySet ks_rolledback_nconns;
	KeyValueMapIterator it;
	bool rolled, all_rolled = true;

	assert(IsTopXAct(txact));

	ks_rolledback_nconns = vh_kset_create();

	vh_kvmap_it_init(&it, txact->nconns);
	while (vh_kvmap_it_next(&it, &sa, &nconn_it))
	{
		nconn = *nconn_it;		
		rolled = vh_be_xact_rollback(nconn);

		/*
		 * If we're in two phase commit, we should call that rollback
		 * method instead.
		 */

		if (rolled)
		{
			nconn_last_commit = nconn;
			vh_kset_key(ks_rolledback_nconns, &nconn);

			xact_mark_actionfor_rollback(txact, ks_rolledback_nconns, nconn);
			xact_runtree_rollback(txact, nconn);

			if (nconn_last_commit)
			{
			}
		}
		else
		{
			all_rolled = false;
		}
	}
	
	xact_retconns(txact);

	vh_kset_destroy(ks_rolledback_nconns);

	return all_rolled;
}

/*
 * vh_xact_node
 *
 * Executes a NodeQuery within the given |sxact|.
 *
 * If the query is a write query, we'll need to use the SavePoint
 * functionality.  For SELECT statements, just run it live and forget about
 * using savepoints.
 */

bool
vh_xact_node(XAct sxact, NodeQuery nq, ExecResult *er)
{
	XAct txact = TopXAct(sxact);
	ExecPlan ep;
	ExecState es;
	ExecResult er_local;
	SavePoint sp;
	bool success = true;

	assert(txact);
	assert(nq);

	if (vh_sqlq_isread(nq))
	{
		if (txact->sp_current)
			xact_sp_flushthru(txact->sp_current);

		ep = xact_create_ep_from_node(sxact, nq);

		if (ep)
		{
			es = vh_es_open();
			es->ep = ep;
			es->mctx_result = vh_mctx_current();

			VH_TRY();
			{
				er_local = xact_es_runtree_read(sxact, es);
			}
			VH_CATCH();
			{
				er_local = 0;
				success = false;
			}
			VH_ENDTRY();

			vh_exec_eplan_destroy(ep);
			vh_es_close(es);

			if (er)
				*er = er_local;

			return success;
		}

		return false;
	}
	else
	{
		if (!sxact->sp_local)
			sp = xact_sp_create(sxact);
		else
		{
			if (sxact->sp_local->sp_flushed)
				sp = xact_sp_create(sxact);
			else	
				sp = sxact->sp_local;
		}

		xact_sp_create_ep_from_node(sp, nq);
		
		if (txact->mode == Immediate)
			xact_sp_flushthru(sp);
	}

	return false;
}

HeapTuplePtr
vh_xact_createht(XAct sxact, TableDefVer tdv)
{
	HeapBuffer hb = vh_hb(sxact->hbno);
	HeapTuplePtr htp;

	if (hb)
	{
		htp = vh_hb_allocht(hb, (HeapTupleDef)tdv, 0);

		return htp;
	}

	return 0;	
}

HeapBufferNo
vh_xact_hbno(XAct xact)
{
	if (xact)
		return xact->hbno;

	return 0;
}

static SavePoint
xact_sp_create(XAct sxact)
{
	MemoryContext mctx_old, mctx_sp;
	XAct txact = TopXAct(sxact);
	SavePoint sp;
	char savepoint_buffer[50];
	uint32_t idx;

	assert(txact);
	assert(sxact->mctx);

	mctx_old = vh_mctx_switch(sxact->mctx);

	idx = vh_SListSize(txact->sps);
	snprintf(&savepoint_buffer[0], 50, "VH_IO_XACT_SP_%d", idx);

	sp = vhmalloc(sizeof(struct SavePointData));
	sp->mctx = 0;
	sp->owner = sxact;
	sp->idx = idx;
	sp->name = vh_str.Convert(&savepoint_buffer[0]);

	sp->sp_rolled_back = false;
	sp->sp_flushed = false;
	sp->sp_flushedthru_idx = 0;
	sp->sp_committed = false;
	sp->shards = vh_kset_create();
	sp->plans = vh_SListCreate();

	sxact->sp_local = sp;
	txact->sp_current = sp;
	vh_SListPush(txact->sps, sp);

	mctx_sp = vh_mctx_switch(mctx_old);
	assert(mctx_sp == sxact->mctx);

	return sp;
}


/*
 * xact_sp_create_ep_from_node
 *
 * Creates an execution plan in a SavePoint context.  SavePoints are resricted
 * to write only queries.
 */
static ExecPlan 
xact_sp_create_ep_from_node(SavePoint sp, NodeQuery nq)
{
	PlannerOpts popts = { };
	MemoryContext mctx_old, mctx_sp;
	XAct sxact = sp->owner, txact = TopXAct(sxact);
	ExecPlan ep;

	assert(sxact);
	assert(txact);
	assert(vh_sqlq_iswrite(nq));
	
	mctx_old = vh_mctx_switch(sp->owner->mctx);

	xact_plan_popts_init(sxact, &popts);
	ep = vh_plan_node_opts(&nq->node, popts);

	if (ep)
	{
		vh_SListPush(sp->plans, ep);
	}

	/*
	 * Check to see if we end up with the same memory context that we started
	 * with before dropping into the planner.  This is a good place to check
	 * to make sure we're switching contexts around in the planner correctly.
	 */
	mctx_sp = vh_mctx_switch(mctx_old);
	assert(sp->owner->mctx == mctx_sp);

	return ep;	
}

static ExecPlan 
xact_create_ep_from_node(XAct sxact, NodeQuery nq)
{
	MemoryContext mctx_sxact, mctx_old;
	PlannerOpts popts = { };
	XAct txact = TopXAct(sxact);
	ExecPlan ep;

	assert(txact);

	xact_plan_popts_init(sxact, &popts);

	mctx_old = vh_mctx_switch(sxact->mctx);
	ep = vh_plan_node_opts(&nq->node, popts);
	
	/*
	 * Check to see if we end up with the same memory context that we started
	 * with before dropping into the planner.  This is a good place to check
	 * to make sure we're switching contexts around in the planner correctly.
	 */
	mctx_sxact = vh_mctx_switch(mctx_old);
	assert(sxact->mctx == mctx_sxact);

	return ep;
}

static void 
xact_plan_popts_init(XAct sxact, PlannerOpts *popts)
{
	popts->hbno = sxact->hbno;
	popts->mctx_result = vh_mctx_current();
}

/*
 * xact_es_runtree_plan
 *
 * Runs a tree without returning the connections, should only be for write
 * queries.  The difference between this and xact_es_runtree_read is the read
 * version returns the connections and provides an ExecResult back to the
 * caller.
 */
static bool 
xact_es_runtree_plan(SavePoint sp_target, ExecState es)
{
	struct PutConnsContext pcc = { };
	XAct sxact = sp_target->owner, txact = TopXAct(sxact);
	MemoryContext mctx_old, mctx_sxact;
	bool ret;

	assert(txact);
	assert(es);
	assert(es->ep);

	mctx_old = vh_mctx_switch(sxact->mctx);

	pcc.xact = sxact;
	pcc.sp = sp_target;
	pcc.begin_tx = true;

	VH_TRY();
	{
		ret = vh_es_putconns(es->ep->plan, txact->cat_conn, &txact->nconns,
							 xact_putconns_cb, &pcc, false);

		if (ret)
		{
			assert(!pcc.nconn_read);

			if (vh_exec_eplan_ready(es->ep))
				vh_es_runtree(es, es->ep->plan);

			vh_es_reset(es);
		}
	}
	VH_CATCH();
	{
		vh_mctx_switch(mctx_old);
		vh_rethrow();
	}
	VH_ENDTRY();

	mctx_sxact = vh_mctx_switch(mctx_old);
	assert(mctx_sxact == sxact->mctx);

	return false;
}

static ExecResult 
xact_es_runtree_read(XAct sxact, ExecState es)
{
	struct PutConnsContext pcc = { };
	XAct txact = TopXAct(sxact);
	MemoryContext mctx_old, mctx_sxact;
	bool ret;

	assert(txact);
	assert(es);
	assert(es->ep);

	/*
	 * Make sure we're a read only plan with no commit or rollback actions.
	 * We're going to release any connections acquired specifically for this
	 * plan to the ConnectionCatalog.
	 */
	//assert(vh_sqlq_isread(es->ep->nquery));
	assert(vh_exec_eplan_hasplan(es->ep));
	assert(!vh_exec_eplan_hascommit(es->ep));
	assert(!vh_exec_eplan_hasrollback(es->ep));

	pcc.xact = sxact;
	pcc.begin_tx = false;

	mctx_old = vh_mctx_switch(sxact->mctx);

	VH_TRY();
	{
		ret = vh_es_putconns(es->ep->plan, txact->cat_conn, &txact->nconns,
							 xact_putconns_cb, &pcc, false);

		if (ret)
		{
			ret = vh_exec_eplan_ready(es->ep);

			if (ret)
			{
				vh_es_runtree(es, es->ep->plan);
				vh_es_reset(es);

				if (pcc.nconn_read)
				{
					xact_retconns_read(txact, pcc.nconn_read);
					vh_kvmap_destroy(pcc.nconn_read);
					pcc.nconn_read = 0;
				}

				mctx_sxact = vh_mctx_switch(mctx_old);
				assert(mctx_sxact == sxact->mctx);

				return es->er;
			}
		}
	}
	VH_CATCH();
	{
		mctx_sxact = vh_mctx_switch(mctx_old);
		vh_rethrow();
	}
	VH_ENDTRY();

	mctx_sxact = vh_mctx_switch(mctx_old);
	assert(mctx_sxact == sxact->mctx);

	return 0;
}

/*
 * xact_sp_flushthru
 *
 * Flushes all savepoints from the last flushed SavePoint in TopXAct up thru
 * the SavePoint indicated by |sp_target|.
 *
 * We catch all errors and don' propogate them up.  In the event a SavePoint
 * flush failes for any reason, we return false.
 */
static bool
xact_sp_flushthru(SavePoint sp_target)
{
	XAct sxact = sp_target->owner, txact = TopXAct(sxact);
	SavePoint *sp_head, sp;
	ExecPlan *ep_head, ep;
	uint32_t sp_start_idx, sp_target_idx, i, ep_sz, ep_start, j;
	ExecState es = 0;
	bool flushed = true;

	assert(sxact);
	assert(txact);

	sp_start_idx = txact->sp_lastflushed ?
		txact->sp_lastflushed->idx : 0;
	sp_target_idx = sp_target->idx;

	vh_SListIterator(txact->sps, sp_head);
	for (i = sp_start_idx; i <= sp_target_idx; i++)
	{
		sp = sp_head[i];

		if (sp->sp_flushed)
			ep_start = sp->sp_flushedthru_idx + 1;
		else
			ep_start = 0;
			
		ep_sz = vh_SListIterator(sp->plans, ep_head);

		for (j = ep_start; j < ep_sz; j++)
		{
			ep = ep_head[j];

			if (!es)
				es = vh_es_open();
			else
				vh_es_reset(es);

			es->ep = ep;
			es->mctx_result = sxact->mctx;
	
			VH_TRY();
			{		
				xact_es_runtree_plan(sp, es);
			}
			VH_CATCH();
			{
				flushed = false;
			}
			VH_ENDTRY();

			if (flushed)
			{			
				sp->sp_flushed = true;
				sp->sp_flushedthru_idx = j;	
			}
		}

		if (flushed)
		{
			txact->sp_flushed = true;
			txact->sp_flushedthru_idx = sp->idx;
			txact->sp_lastflushed = sp;

			if (txact->sp_current == sp)
				txact->sp_current = 0;
			if (sxact->sp_local == sp)
				sxact->sp_local = 0;
		}
	}

	if (es)
		vh_es_close(es);

	return flushed;
}

/*
 * 
 * Each connection is then issued ROLLBACK TO commands.
 */
static bool 
xact_sp_rollbackto(SavePoint sp_target, bool xact_local)
{
	MemoryContext mctx_old, mctx_work;
	XAct txact;
	SavePoint *sp_head, sp;
	Shard shd;
	KeyValueMap kvm_nconn_least;
	uintptr_t *nconn_least_idx;
	BackEndConnection *nconn, nconnk;
	vh_beat_xactrollbackto rollbackto;
	KeySetIterator it;
	KeyValueMapIterator kvm_it;
	uint32_t sp_sz, i, sp_current_idx;
	bool ret, all_rolled = true;

	mctx_old = vh_mctx_current();
	mctx_work = vh_MemoryPoolCreate(mctx_old, 8192, "xact_sp_rollback");
	vh_mctx_switch(mctx_work);

	txact = TopXAct(sp_target->owner);
	assert(txact);

	kvm_nconn_least = vh_kvmap_create();

	sp_sz = vh_SListIterator(txact->sps, sp_head);

	if (xact_local)
		sp_current_idx = sp_target->idx;
	else
		sp_current_idx = sp_sz - 1;

	for (i = sp_target->idx; i <= sp_current_idx; i++)
	{
		sp = sp_head[i];

		if ((xact_local && sp->owner == sp_target->owner) ||
			!xact_local)
		{
			if (sp->sp_flushed && !sp->sp_rolled_back)
			{
				vh_kset_it_init(&it, sp->shards);

				while (vh_kset_it_next(&it, &shd))
				{
					vh_kvmap_value(txact->nconns, &shd, nconn);

					if (!vh_kvmap_value(kvm_nconn_least, &nconn, nconn_least_idx))
						*nconn_least_idx = (uintptr_t)sp->idx;
				}
			}
		}	
	}

	/*
	 * Iterate the kvm_nconn_least map and issue the rollback to statements.
	 */
	vh_kvmap_it_init(&kvm_it, kvm_nconn_least);

	while (vh_kvmap_it_next(&kvm_it, &nconnk, nconn_least_idx))
	{
		rollbackto = nconnk->be->at.xactrollbackto;
		sp = sp_head[*nconn_least_idx];

		if (rollbackto)
		{
			VH_TRY();
			{
				ret = rollbackto(nconnk, sp->name);
				sp->sp_rolled_back = ret;
			}
			VH_CATCH();
			{
				all_rolled = false;
				sp->sp_rolled_back = false;
			}
			VH_ENDTRY();
		}
	}

	vh_mctx_switch(mctx_old);
	vh_mctx_destroy(mctx_work);

	return all_rolled;
}

/*
 * Calls the commit plan for all statements affected by the BackEndConnection.
 * Assumes the caller has already invoked vh_beat_xactcommit on the back end
 * succesfully.
 */
static bool 
xact_runtree_commit(XAct txact, BackEndConnection nconn)
{
	struct XActRunTreeContext xrtc = { };
	MemoryContext mctx_txact, mctx_old;

	assert(IsTopXAct(txact));

	mctx_old = vh_mctx_switch(txact->mctx);

	xrtc.tree_commit = true;
	xrtc.success = true;
	
	VH_TRY();
	{
		xact_iterate_ep(txact, xact_runtree_cb, &xrtc);
	}
	VH_CATCH();
	{
		xrtc.success = false;
	}
	VH_ENDTRY();

	mctx_txact = vh_mctx_switch(mctx_old);
	assert(mctx_txact == txact->mctx);

	return xrtc.success;
}

/*
 * Calls the rollback actions for all non-committed SavePoints in the
 * transaction.  This is different that simply issuing a ROLLBACK statement
 * to the BackEnd.  Here, we invoke executor actions generated by the planner
 * in the event a rollback occurs.  If we call this function, we should
 * have already issued ROLLBACK commands to the backend.
 */
static bool 
xact_runtree_rollback(XAct txact, BackEndConnection nconn)
{
	struct XActRunTreeContext xrtc = { };
	MemoryContext mctx_txact, mctx_old;

	assert(IsTopXAct(txact));

	mctx_old = vh_mctx_switch(txact->mctx);

	xrtc.tree_commit = false;
	xrtc.success = true;

	VH_TRY();
	{
		xact_iterate_ep(txact, xact_runtree_cb, &xrtc);
	}
	VH_CATCH();
	{
		xrtc.success = false;
	}
	VH_ENDTRY();

	mctx_txact = vh_mctx_switch(mctx_old);
	assert(mctx_txact == txact->mctx);

	return xrtc.success;
}

/*
 * We assume the caller is going to catch any errors that bubble up.  There's
 * not a good clean up path so any errors will leave the local in memory
 * HeapTuple in a dangerously inconsistent state.
 *
 * Ideally we should be able to determine the actual ExecStep that triggered the
 * error.  Based on the strength of the error and the configuration settings,
 * we can determine what to do here.  This may be a good candidate for
 * ExecState.
 */
static void 
xact_runtree_cb(XAct txact, SavePoint sp, ExecPlan ep,
				uint32_t sp_idx, uint32_t ep_idx,
				void *data)
{
	struct XActRunTreeContext *xrtc = data;
	struct PutConnsContext pcc = { };
	ExecStep root_action = 0;
	bool ret;

	/*
	 * Selectively populate root_action. It's OK to not define one, no
	 * executor actions will be triggered.
	 */
	if (xrtc->tree_commit)
	{
		if (sp->sp_committed)
			root_action = ep->on_commit;
		else
			root_action = 0;
	}
	else
	{
		/*
		 * Even if we haven't flushed the SavePoint, in local memory the
		 * impacted HeapTuple aren't going to be in a consistent state.
		 * 
		 * Thus we call the rollback action to undo any changes the user
		 * may have made without a succesful commit appplied.  The 
		 * executor rolls back the changes on each individual HeapTuple.
		 *
		 * XAct is merely responible for getting the executor going.
		 */

		if (!sp->sp_committed)
			root_action = ep->on_rollback;
	}

	if (root_action)
	{
		pcc.sp = sp;
		pcc.xact = txact;
		pcc.begin_tx = false;

		ret = vh_es_putconns(root_action, txact->cat_conn,
							 &txact->nconns,
							 xact_putconns_cb, &pcc, false);
		if (ret)
		{
			ret = vh_exec_eplan_ready(ep);

			if (ret)
			{
				if (!xrtc->es)
					xrtc->es = vh_es_open();

				vh_es_runtree(xrtc->es, root_action);
				vh_es_reset(xrtc->es);
			}
		}
	}
}

/*
 * We run thru each SavePoint to check to see if it's been fully committed
 * for a given connection.  Use xact_iterate_sp_filter_nconn to iterate the
 * save points.
 *
 * |ks_processed| is a set of BackEndConnection.  The |nconn| passed should
 * already be a member of |ks_processed|.
 */
static void 
xact_mark_actionfor_commit(XAct txact, KeySet ks_processed, 
						   BackEndConnection nconn)
{
	struct XActMarkActionFor xmaf = { };

	assert(ks_processed);
	assert(vh_kset_exists(ks_processed, &nconn));

	xmaf.ks_nconns = ks_processed;
	xmaf.action_commit = true;
	xact_iterate_sp_filter_nconn(txact, xact_mark_actionfor_cb,
								 &xmaf, nconn);
}

static void
xact_mark_actionfor_rollback(XAct txact, KeySet ks_processed,
							 BackEndConnection nconn)
{
	struct XActMarkActionFor xmaf = { };

	assert(ks_processed);
	assert(vh_kset_exists(ks_processed, &nconn));

	xmaf.ks_nconns = ks_processed;
	xmaf.action_commit = false;
	xact_iterate_sp_filter_nconn(txact, xact_mark_actionfor_cb,
								 &xmaf, nconn);
}

/*
 * Marks an action based on the caller's instructions in XActMarkActionFor.
 * We assume this got called thru xact_iterate_sp_filter_nconn and thus if
 * there's only one shard in the SavePoint then we can mark the action.
 *
 * Otherwise we get to spin thru all the SavePoint's shards, looking up the
 * BackEndConnection in the TopXAct |nconns| and checking to make sure they're all
 * there.
 */
static void 
xact_mark_actionfor_cb(XAct txact, SavePoint sp,
						  uint32_t sp_idx, void *cb_data)
{
	struct XActMarkActionFor *xmaf = cb_data;
	bool *action, aborted = false;
	BackEndConnection nconn_lookup;
	ShardAccess sa_sp;
	KeySetIterator it;


	if (xmaf->action_commit)
		action = &sp->sp_committed;
	else
		action = &sp->sp_rolled_back;

	if (vh_kset_count(sp->shards) == 1)
	{
		*action = true;
	}
	else
	{
		vh_kset_it_init(&it, sp->shards);
		while (vh_kset_it_next(&it, sa_sp))
		{
			nconn_lookup = vh_kvmap_find(txact->nconns, &sa_sp);

			/*
			 * See if we can find the BackEndConnection the processed list.  If
			 * it's not there abort.
			 */

			if (!vh_kset_exists(xmaf->ks_nconns, &nconn_lookup))
			{
				aborted = true;
				break;
			}
		}

		if (!aborted)
			*action = true;
	}
}

/*
 * Iterates |nconns| returning each one to the connection catalog.  We do
 * a few cleanup sanity checks to make sure there isn't an outstanding
 * transaction open.  The sanity checks are really for debugging purposes.
 */

static void
xact_retconns(XAct txact)
{
	xact_retconns_read(txact, txact->nconns);
}

static void 
xact_retconns_read(XAct txact, KeyValueMap map)
{
	ShardAccess sa, *sa_head;
	BackEndConnection *nconn;
	SList removes = 0;
	KeyValueMapIterator it;
	uint32_t removes_sz = 0, i;

	assert(IsTopXAct(txact));

	vh_kvmap_it_init(&it, map);

	while (vh_kvmap_it_next(&it, &sa, &nconn))
	{
		vh_ConnectionReturn(txact->cat_conn, *nconn);

		if (!removes)
			removes = vh_SListCreate();

		vh_SListPush(removes, sa);
	}

	if (removes)
		removes_sz = vh_SListIterator(removes, sa_head);

	for (i = 0; i < removes_sz; i++)
	{
		vh_kvmap_remove(map, &sa_head[i]);
	}

	if (removes)
		vh_SListDestroy(removes);
}

/*
 * When we call vh_es_putconns we want to track each connection in a SavePoint
 * and the top XAct if vh_es_putconns had to go to the ConnectionCatalog.
 *
 * If we bring a ShardAccess into a SavePoint's scope, we'll want to
 * submit a savepoint to the back end.  Read only connection go into a separate
 * list so that when the scope of the read immediate goes away the connection
 * can be released back to the catalog.  In the event a read flushes 
 */
static void 
xact_putconns_cb(void *putconnsctx, 
				 PlannedStmt pstmt, PlannedStmtShard pstmtshd, 
				 BackEndConnection nconn,
				 bool from_connection_catalog)
{
	struct PutConnsContext *pcc = putconnsctx;
	ShardAccess sa = pstmtshd->sharda;
	BackEnd be = sa->be;
	XAct txact = TopXAct(pcc->xact);
	BackEndConnection *kvm_nconn;
	bool created;
	vh_beat_savepoint beat_sp;

	assert(txact);
	assert(pcc);

	if (!sa || !be)
	{
		pcc->general_error++;		
		return;
	}

	if (vh_pstmt_iswrite(pstmt))
	{
		beat_sp = nconn->be->at.savepoint;

		if (!vh_kset_key(pcc->sp->shards, &sa))
		{
			/*
			 * We need to bring the SavePoint up to date on the local connection
			 * since it doesn't exist yet.
			 */
		
			if (beat_sp)
			{
				created = beat_sp(nconn, pcc->sp->name);

				if (created)
					pcc->sp_conn_attached++;
				else
					pcc->sp_conn_attach_fail++;
			}
			else
			{
				pcc->sp_conn_no_support++;
			}

			/*
			 * Check to see if the connection we were handed has two phase commit
			 * support.  If it doesn't increment the counter.
			 */
		}

		if (from_connection_catalog)
		{
			/*
			 * Quick sanity check, vh_es_putconns should never indicate the
			 * connection came from the catalog if it was already available in
			 * the top level XAct |nconns| kvmap.  We should be passing the
			 * |nconns| kvmap directly into vh_es_putconns.
			 */

			assert(!vh_kvmap_value(txact->nconns, &sa, kvm_nconn));
			(*kvm_nconn) = nconn;


			/*
			 * Check to see if we need to begin a transaction on the the
			 * connection, but first make sure no transactions are active.
			 */
		
			created = vh_be_xact_begin(nconn);

			if (!created)
				pcc->sp_conn_tx_begin_fail++;
		}
		else
		{
			/*
			 * If the BackEndConnection is in the read-only list, we should remove
			 * it.
			 */

			if (pcc->nconn_read)
				vh_kvmap_remove(pcc->nconn_read, sa);
		
			/*
			 * Check to see if we need to start a transaction.
			 */

			created = vh_be_xact_begin(nconn);

			if (!created)
				pcc->sp_conn_tx_begin_fail++;
		}
	}
	else
	{
		/*
		 * We're a non-transactional query where SavePoints aren't applicable.
		 * We still want to track any BackEndConnection obtained, but since it's
		 * likely a read, we'll want to release those connections back as once
		 * the query returns.  Push it into the |nconn_read| KeySet for now.
		 *
		 * If it comes off the top level XAct stack we know a write query
		 * picked it up.
		 */

		if (from_connection_catalog)
		{
			if (!pcc->nconn_read)
				pcc->nconn_read = vh_kvmap_create();

			vh_kvmap_value(pcc->nconn_read, &sa, kvm_nconn);
			(*kvm_nconn) = nconn;

			/*
			 * Drop it off in the top level node connection KeyValueMap so that
			 * we don't try to obtain a connection to the same shard twice if
			 * we're flushing to accomodate a read immediate query.  After the
			 * read is complete, we should traverse the list of read only
			 * connections and release them back to the pool.
			 */
			assert(!vh_kvmap_value(txact->nconns, &sa, kvm_nconn));

			/*
			 * Quick sanity check, vh_es_putconns should never indicate the
			 * connection came from the catalog if it was already available in
			 * the top level XAct |nconns| kvmap.  We should be passing the
			 * |nconns| kvmap directly into vh_es_putconns.
			 */
			(*kvm_nconn) = nconn;
		}
	}
}

/*
 * Since we iterate each SavePoint's list of ExecPlans so often, we just created
 * a function to do it for us.
 *
 * Provide a |cb| in the correct signature.
 */
static void 
xact_iterate_ep(XAct txact, xact_iterate_ep_cb cb, void *cb_data)
{
	SavePoint *sp_head, sp;
	ExecPlan *ep_head;
	uint32_t  sp_sz, i, ep_sz, j;

	assert(txact);
	assert(TopXAct(txact));

	sp_sz = vh_SListIterator(txact->sps, sp_head);

	for (i = 0; i < sp_sz; i++)
	{
		sp = sp_head[i];
		ep_sz = vh_SListIterator(sp->plans, ep_head);

		for (j = 0; j < ep_sz; j++)
			cb(txact, sp, ep_head[j], i, j, cb_data);
	}
}

/*
 * Since we iterate each SavePoint often, we just created a function to do it
 * for us.
 */

static void 
xact_iterate_sp(XAct txact, xact_iterate_sp_cb cb, void *cb_data)
{
	SavePoint *sp_head;
	uint32_t sp_sz, i;

	assert(txact);
	assert(TopXAct(txact));

	sp_sz = vh_SListIterator(txact->sps, sp_head);

	for (i = 0; i < sp_sz; i++)
		cb(txact, sp_head[i], i, cb_data);
}

static void 
xact_iterate_sp_filter_nconn(XAct txact, xact_iterate_sp_cb cb,
							 void *cb_data,
							 BackEndConnection filter_nconn)
{
	SavePoint *sp_head, sp;
	uint32_t sp_sz, i;
	ShardAccess sp_shard;
	BackEndConnection found_nconn;
	KeySetIterator it;

	assert(txact);
	assert(TopXAct(txact));

	sp_sz = vh_SListIterator(txact->sps, sp_head);

	for (i = 0; i < sp_sz; i++)
	{
		sp = sp_head[i];

		vh_kset_it_init(&it, sp->shards);

		while (vh_kset_it_next(&it, &sp_shard))
		{
			found_nconn = vh_kvmap_find(txact->nconns, &filter_nconn);

			if (found_nconn && found_nconn == filter_nconn)
			{
				cb(txact, sp, i, cb_data);
				break;
			}
		}
	}
}

