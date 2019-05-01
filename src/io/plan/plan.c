/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/executor/eplan.h"
#include "io/executor/estepfwd.h"
#include "io/executor/estep.h"
#include "io/nodes/Node.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/nodes/NodeQueryUpdate.h"
#include "io/nodes/NodeUpdateField.h"
#include "io/plan/esg.h"
#include "io/plan/flatten.h"
#include "io/plan/plan.h"
#include "io/plan/pstmt_funcs.h"
#include "io/shard/Shard.h"
#include "io/utils/kvmap.h"
#include "io/utils/SList.h"

typedef struct PlannerStateData *PlannerState;

struct PlannerStateData
{
	ExecPlan ep;
	PlannerOpts *popts;
};

/*
 * There are a few ways we may form up a plan.  The single entry point for all
 * planning is vh_plan_node_opts.  We teach this function to handle all of the
 * scenarios a user may present.
 */

static void planner_state_destroy(PlannerState pstate);
static void planner_state_init(PlannerState pstate, PlannerOpts *opts);
static void plan_opts_set_defaults(PlannerOpts *opts);
static void plan_find_nodes_recurse(Node node, void *data);


/*
 * We provide two planning modes, simple and shard based.  The simple mode
 * does not recognize any beacon or shard techniques.  It will form an ESG
 * group based on the query.  The shard mode is the more advanced planner
 * and will detect beacons, shards, and generally speaking slice and dice
 * the input query into multiple paths to obtain the desired result set.
 */
static void plan_esg_simple(PlannerState pstate, NodeQuery nq);


void (*plan_esg)(PlannerState pstate, NodeQuery nq) = plan_esg_simple;

static bool plan_insert(ExecPlan ep, Shard shard, NodeQueryInsert nq,
						HeapBufferNo hbno);
static bool plan_select(ExecPlan ep, Shard shard, NodeQuerySelect nq,
						HeapBufferNo hbno);
static bool plan_update(ExecPlan ep, Shard shard, NodeQueryUpdate nq,
						HeapBufferNo hbno);

static bool plan_command(ExecPlan ep, Shard shad, NodeQuery nq,
						 PlannerOpts *popts);



ExecPlan
vh_plan_node_opts(Node root, PlannerOpts opts)
{
	ExecPlan ep = 0;
	struct PlannerStateData pstate;
	SList node_queries = vh_SListCreate();
	NodeQuery nq, *nq_head;
	uint32_t nq_sz;

	plan_opts_set_defaults(&opts);
	planner_state_init(&pstate, &opts);

	vh_nsql_visit_tree(root, plan_find_nodes_recurse, 
					   node_queries);
	nq_sz = vh_SListIterator(node_queries, nq_head);

	if (nq_sz == 1)
	{
		nq = nq_head[0];
		plan_esg(&pstate, nq);

		ep = pstate.ep;
	}
	else if (nq_sz == 0)
	{
		elog(ERROR2,
			 emsg("Unable to locate a well formed NodeQuery in the tree!"));
	}
	else
	{
		elog(ERROR2,
			 emsg("Unable to plan with multiple NodeQuery tree!"));
	}

	planner_state_destroy(&pstate);
	vh_SListDestroy(node_queries);

	return ep;
}

static void
plan_opts_set_defaults(PlannerOpts *opts)
{
	CatalogContext cc = vh_ctx();

	if (cc && !opts->hbno)
		opts->hbno = cc->hbno_general;

	if (!opts->mctx_result)
		opts->mctx_result = vh_mctx_current();
}



/*
 * Node filter for vh_nsql_visit_tree to only push NodeQuery objects into an
 * SList.
 */
static void
plan_find_nodes_recurse(Node node, void *data)
{
	SList nodes = data;
	
	switch (node->tag)
	{
	case Query:
		vh_SListPush(nodes, node);
		break;

	default:
		break;
	}
}

static void 
planner_state_destroy(PlannerState pstate)
{
}

static void 
planner_state_init(PlannerState pstate, PlannerOpts *opts)
{
	pstate->popts = opts;
	pstate->ep = 0;
}




static void
plan_esg_simple(PlannerState pstate, NodeQuery nq)
{
	CatalogContext cc = vh_ctx();	
	PlannerOpts* popts = pstate->popts;
	Shard shard;
	ExecPlan ep;
	bool plan_res = false, ep_ready = false;

	/*
	 * Transfer planner options to ExecStepGroupOpts
	 */
	if (popts->shard)
	{
		shard = popts->shard;
	}
	else if (popts->shards)
	{
		elog(ERROR2,
			 emsg("The VH.IO basic planner is unable to accomodate queries against "
				  "more than one shard.  Please revise the calling convention to "
				  "target one shard"));
		return;
	}
	else
	{
		shard = cc->shard_general;
	}

	if (shard)
	{
		ep = vh_exec_eplan_create();
		ep->mctx_result = pstate->popts->mctx_result;

		switch (nq->action)
		{
		case Delete:
			break;

		case BulkInsert:
		case Insert:
			plan_res = plan_insert(ep, shard,
								   (NodeQueryInsert)nq, popts->hbno);
			break;

		case Select:
			plan_res = plan_select(ep, shard, 
								   (NodeQuerySelect)nq, popts->hbno);
			break;

		case Update:
			plan_res = plan_update(ep, shard,
								   (NodeQueryUpdate)nq, popts->hbno);
			break;

		case DDLCreateTable:
			plan_res = plan_command(ep, shard, nq, popts);
			break;

		default:
			break;
		}

		if (plan_res)
		{
			ep_ready = vh_exec_eplan_ready(ep);

			if (ep_ready)
				pstate->ep = ep;
			else
				pstate->ep = 0;
		}
	}
}

/*
 * plan_insert
 *
 * We've got a good bit of work to do here.  We should absolutely get the
 * null bitmaps for each tuple expected to be inserted.  From there we can
 * define a target list, which may require us to spin up additional 
 * NodeQueryInsert structures and plan nodes.
 */
static bool
plan_insert(ExecPlan ep, Shard shard, NodeQueryInsert nq,
			HeapBufferNo hbno)
{
	NodeQueryInsert *nq_kvm, nq_new;
	ExecStepFetch esf, esf_root = 0, esf_prior = 0;
	BackEnd be;
	size_t nbm_sz;
	char *nbm;
	NodeFrom nfrom = nq->into, nfrom_new;
	HeapField *hf_head, hf;
	HeapTuplePtr *htp_head, htp;
	HeapTuple ht;
	KeyValueMap nbm_kvm = 0;
	KeyValueMapIterator nbm_it;
	int32_t hf_sz, htp_sz, i, j;

	if (nfrom)
	{
		nbm_sz = vh_htd_nbm_size(vh_tdv_htd(nfrom->tdv));
		nbm = vhmalloc(nbm_sz);

		hf_sz = vh_SListIterator(vh_tdv_htd(nfrom->tdv)->fields, hf_head);
		htp_sz = vh_SListIterator(nfrom->htps, htp_head);

		for (i = 0; i < htp_sz; i++)
		{
			htp = htp_head[i];
			ht = vh_htp(htp);

			/*
			 * If there's only one record scheduled for insert, then we can just
			 * hijack our existing query and schedule the non-null fields for
			 * INSERT.
			 *
			 * More than one record means we get to stand up a hash map.
			 */
			if (htp_sz == 1)
			{
				for (j = 0; j < hf_sz; j++)
				{
					hf = hf_head[j];

					if (vh_htf_isnull(ht, hf))
					{
						/*
						 * Null field so add it to the return list
						 */

						vh_sqlq_ins_rfield_add(nq, nfrom, hf);
					}
					else
					{
						/*
						 * Non-null field so add it to the target list
						 */

						vh_sqlq_ins_field_add(nq, nfrom, hf);
					}
				}
			}
			else
			{
				if (!nbm_kvm)
				{
					nbm_kvm = vh_kvmap_create_impl(nbm_sz, 				/* Key Size */
												   sizeof(NodeQueryInsert),	/* Value Size */
												   vh_htbl_hash_bin,	/* Hash Function */
												   vh_htbl_comp_bin,	/* Comparison Function */
												   vh_mctx_current());
				}

				if (!vh_ht_nullbitmap(ht, nbm, nbm_sz))
				{
					/*
					 * We've got a serious problem in our code.  We allocated
					 * an exact null bitmap size the HeapTuple we're passing in
					 * requires a null bitmap size larger than what we passed.
					 *
					 * Possible cause: more than one type of TableDefVer in the
					 * SList of HeapTuplePtr.
					 */
				}
				
				/*
				 * Lookup our null bitmap in the KeyValueMap.  If it's not there
				 * we'll get to build a new query.
				 */

				if (vh_kvmap_value(nbm_kvm, nbm, nq_kvm))
				{
					/*
					 * We've got a matching null bitmap already in the HashMap.
					 *
					 * Just pop this HeapTuplePtr into the SList.
					 */

					nq_new = *nq_kvm;
					vh_sqlq_ins_htp(nq_new, htp);
				}
				else
				{
					/*
					 * Null bitmap isn't in the map, so we'll have to spin up a
					 * new query.  Since this is the first time thru and we only
					 * want to INSERT NON-NULL fields, we'll need to tell the
					 * query explicity what fields to add based on their null
					 * property.  We can use the null bitmap that has already been
					 * built for this.
					 */
					nq_new = vh_sqlq_ins_create();
					*nq_kvm = nq_new;

					nfrom_new = vh_sqlq_ins_table(nq_new, nfrom->tdv->td);

					for (j = 0; j < hf_sz; j++)
					{
						hf = hf_head[j];

						if (vh_ht_nbm_isnull(nbm, j))
						{
							/*
							 * Null field so add it to the returning list
							 */

							vh_sqlq_ins_rfield_add(nq_new, nfrom_new, hf);
						}
						else
						{
							/*
							 * Non-null field so add it to the target list.
							 */

							vh_sqlq_ins_field_add(nq_new, nfrom_new, hf);
						}
					}

					vh_sqlq_ins_htp(nq_new, htp);
				}
			}
		}

		vhfree(nbm);
	}
	
	be = shard->access[0]->be;

	if (nbm_kvm)
	{
		vh_kvmap_it_init(&nbm_it, nbm_kvm);

		while (vh_kvmap_it_next(&nbm_it, nbm, &nq_kvm))
		{
			if (vh_SListSize((*nq_kvm)->into->htps) > 4)
				(*nq_kvm)->query.action = BulkInsert;

			esf = vh_es_create(EST_Fetch, 0);
			esf->hbno = hbno;
			esf->pstmt = vh_pstmt_generate_from_query((NodeQuery) (*nq_kvm), be);
			esf->pstmtshd = vh_pstmtshd_generate(esf->pstmt, shard, shard->access[0]);
			esf->indexed = false;
			esf->returning = ((*nq_kvm)->rfields) ? true : false;

			if (vh_pstmt_qrp(esf->pstmt))
				return false;

			if (!esf_root)
				esf_root = esf;

			if (esf_prior)
				esf_prior->es.sibling = (ExecStep)esf;

			esf_prior = esf;
		}

		vh_kvmap_destroy(nbm_kvm);

		ep->plan = (ExecStep)esf;
	}
	else
	{
		if (vh_SListSize(nq->into->htps) > 4)
			nq->query.action = BulkInsert;

		esf = vh_es_create(EST_Fetch, 0);

		ep->plan = (ExecStep)esf;

		esf->hbno = hbno;
		esf->pstmt = vh_pstmt_generate_from_query((NodeQuery)nq, be);
		esf->pstmtshd = vh_pstmtshd_generate(esf->pstmt, shard, shard->access[0]);
		esf->indexed = false;
		esf->returning = (nq->rfields ? true : false);

		assert((NodeQuery)esf->pstmt->nquery == (NodeQuery)nq);

		if (vh_pstmt_qrp(esf->pstmt))
			return false;
	}
	
	vh_SListPush(ep->shards, shard->access[0]);

	return true;
}

static bool
plan_update(ExecPlan ep, Shard shard, NodeQueryUpdate nq,
			HeapBufferNo hbno)
{
	ExecStepFetch esf;
	BackEnd be;
	ShardAccess sa;
	TableDefVer tdv;
	NodeUpdateField nuf;
	NodeFrom nfrom;
	HeapField *hf_head, hf;
	HeapTuple ht, hti;
	int32_t i, hf_sz, comp;

	sa = shard->access[0];
	be = sa->be;

	esf = vh_es_create(EST_Fetch, 0);
	ep->plan = (ExecStep)esf;
	esf->hbno = hbno;
	esf->indexed = false;
	esf->returning = false;

	nfrom = nq->nfrom;
	tdv = nfrom->tdv;

	if (!nq->nfields && nq->htp)
	{
		hf_sz = vh_SListIterator(tdv->heap.fields, hf_head);
		ht = vh_htp(nq->htp);
		hti = vh_htp_flags(nq->htp, 0);

		comp = vh_ht_compare(hti, ht, true);

		if (comp)
		{
			for (i = 0; i < hf_sz; i++)
			{
				hf = hf_head[i];

				if (vh_htf_flags(ht, hf) & VH_HTF_FLAG_CHANGED)
				{
					nuf = vh_nsql_updfield_create((TableField)hf, nq->htp);
					vh_sqlq_upd_field_add(nq, nuf);
				}
			}
		}
	}

	esf->pstmt = vh_pstmt_generate_from_query((NodeQuery)nq, be);
	esf->pstmtshd = vh_pstmtshd_generate(esf->pstmt, shard, sa);
		
	if (vh_pstmt_qrp(esf->pstmt))
		return false;

	return true;
}

static bool
plan_select(ExecPlan ep, Shard shard, NodeQuerySelect nq,
			HeapBufferNo hbno)
{
	ExecStepFetch esf;
	BackEnd be;
	
	be = shard->access[0]->be;
	esf = vh_es_create(EST_Fetch, 0);

	ep->plan = (ExecStep)esf;
	
	esf->hbno = hbno;
	esf->pstmt = vh_pstmt_generate_from_query((NodeQuery)nq, be);
	esf->pstmtshd = vh_pstmtshd_generate(esf->pstmt, shard, shard->access[0]);
	esf->indexed = false;
	esf->returning = false;

	if (vh_pstmt_qrp(esf->pstmt))
		return false;

	vh_SListPush(ep->shards, shard->access[0]);

	return true;
}

static bool
plan_command(ExecPlan ep, Shard shard, NodeQuery nq, PlannerOpts *popts)
{
	ExecStepDiscard esd;
	BackEnd be;

	if (popts->bec)
	{
		be = popts->bec->be;
	}
	else
	{
		be = shard->access[0]->be;
	}

	esd = vh_es_create(EST_Discard, 0);

	ep->plan = (ExecStep)esd;

	esd->pstmt = vh_pstmt_generate_from_query(nq, be);
	esd->pstmtshd = vh_pstmtshd_generate(esd->pstmt, shard, shard->access[0]);
	esd->pstmtshd->nconn = popts->bec;

	esd->pstmt->qrp_table = 0;
	esd->pstmt->qrp_field = 0;
	esd->pstmt->qrp_backend = 0;

	vh_SListPush(ep->shards, shard->access[0]);

	return true;
}

