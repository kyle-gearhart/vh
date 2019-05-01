/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/shard/Beacon.h"
#include "io/shard/Shard.h"
#include "io/shard/shtm.h"
#include "io/executor/estep.h"
#include "io/executor/estepfwd.h"
#include "io/plan/esg.h"
#include "io/plan/esg_from.h"
#include "io/plan/esg_join.h"
#include "io/plan/esg_quals.h"
#include "io/plan/tree.h"
#include "io/plan/pstmt.h"
#include "io/plan/pstmt_funcs.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQual.h"
#include "io/utils/kvlist.h"
#include "io/utils/kvmap.h"
#include "io/utils/SList.h"


static ExecStepGroup esg_sel_generate_beac(ExecStepGroupOpts opts,
			   							   NodeQuerySelect nqsel,
			   							   enum ExecStepTag est_hint);

static ExecStepGroup esg_sel_generate_xbeac(ExecStepGroupOpts opts,
				   							NodeQuerySelect nqsel,
			   								enum ExecStepTag est_hint,
			   								KeyValueList uniq_nfrom_beac,
		   									KeyValueList uniq_njoin_beac);

static ExecStepGroup esg_sel_generate_singletd(ExecStepGroupOpts opts,
					 						   NodeQuerySelect nqsel,
				 							   enum ExecStepTag est_hint,
				 							   TableDef td);
static ExecStep esg_sel_generate_singletd_shard(ExecStepGroupOpts opts,
												NodeQuerySelect nqsel,
												enum ExecStepTag est_hint,
												Shard shard,				
												PlannedStmt *pstmt);
static void esg_sel_generate_singletd_mshard(ExecStepGroupOpts opts,
											 ExecStepGroup esg,
											 NodeQuerySelect nqsel,
											 TableDef td);

static SList esg_sel_tds(NodeQuerySelect nqsel);


/*
 * Planner will transform multiple FROM tables into JOINs were possible.  If
 * we've still got FROM statements left, we'll want to check to make sure
 * they're from the same beacon.  If so, this is pretty straight forward,
 * just detect the shards involved based on the quals and then spin up
 * the queries.  Otherwise, we're going to attempt a cartesian product
 * across two or more beacons.
 */

ExecStepGroup
vh_esg_sel_generate(ExecStepGroupOpts opts, NodeQuery nquery,
					enum ExecStepTag est_hint)
{
	NodeQuerySelect nqsel = (NodeQuerySelect)nquery;
	KeyValueList uniq_nfrom_beac;
	KeyValueList uniq_njoin_beac;
	bool has_mult_from_beac, has_mult_join_beac;
	
	if (nqsel->from)
	{
		uniq_nfrom_beac = vh_esg_from_beac(nqsel->from);
		has_mult_from_beac = (uniq_nfrom_beac &&
							  vh_kvlist_count(uniq_nfrom_beac) > 0 ? 
							  true : false);

		if (has_mult_from_beac)
		{
			return esg_sel_generate_xbeac(opts, nqsel, est_hint,
										  uniq_nfrom_beac,
										  0);
		}

		if (nqsel->joins)
		{
			uniq_njoin_beac = vh_esg_join_beac(nqsel->joins);
			has_mult_join_beac = (uniq_njoin_beac &&
								  vh_kvlist_count(uniq_njoin_beac) > 0 ? 
								  true : false);

			if (has_mult_join_beac)
			{
				return esg_sel_generate_xbeac(opts, nqsel, est_hint,
											  uniq_nfrom_beac,
											  uniq_njoin_beac);
			}
		}
		else
		{
			uniq_njoin_beac = 0;
		}

		/*
		 * Drop into a single beacon mode
		 */
		
		if (uniq_nfrom_beac)
			vh_kvlist_destroy(uniq_nfrom_beac);

		if (uniq_njoin_beac)
			vh_kvlist_destroy(uniq_njoin_beac);

		return esg_sel_generate_beac(opts, nqsel, est_hint);
	}

	return 0;
}


/*
 * There's only one beacon involved so we only need to detect how many shards
 * are involved.  If we need to a do a cross shard query, we'll drop out of 
 * here and do it somewhere else.
 */
static ExecStepGroup 
esg_sel_generate_beac(ExecStepGroupOpts opts,
					  NodeQuerySelect nqsel,
					  enum ExecStepTag est_hint)
{
	SList tds;
	TableDef *td_head;
	uint32_t td_sz;

	tds = esg_sel_tds(nqsel);

	if (tds)
		td_sz = vh_SListIterator(tds, td_head);
	else
		td_sz = 0;

	if (td_sz == 1)
	{
		return esg_sel_generate_singletd(opts,
										 nqsel,
										 est_hint,
										 td_head[0]);
	}
	else
	{
	}

	return 0;
}

static ExecStepGroup 
esg_sel_generate_xbeac(ExecStepGroupOpts opts,
					   NodeQuerySelect nqsel,
					   enum ExecStepTag est_hint,
					   KeyValueList uniq_nfrom_beac,
					   KeyValueList uniq_njoin_beac)
{
	return 0;
}


static ExecStepGroup
esg_sel_generate_singletd(ExecStepGroupOpts opts,
						  NodeQuerySelect nqsel,
						  enum ExecStepTag est_hint,
						  TableDef td)
{
	ExecStepGroup esg;
	ExecStepFunnel esf;
	ExecStep es;
	struct ESG_PullShardOpts ps_opts;
	struct ESG_PullShardRet ps_ret;
	Shard shard;
	Beacon beacon;

	beacon = td->beacon;

	if (!beacon)
	{
	}
	
	if (opts->detect_shards)
	{
		/*
		 * Let's detect the shards and if we find some, populate the |opts| with
		 * them.  Then we'll just call esg_sel_generate_singletd and then we'll
		 * fall out of the top level branch here to plan with
		 * esg_sel_generate_singletd_shard or esg_sel_generate_singletd_mshard.
		 */
		ps_opts.td = td;

		if (nqsel->quals)
		{
			vh_esg_quals_pullshard(&ps_opts, nqsel->quals,
								   &ps_ret);
		}
		else
		{
			/*
			 * Since there are no quals, we're going against all shards available
			 * in the beacon.
			 */
		}
		
		opts->detect_shards = false;
		
		return esg_sel_generate_singletd(opts, nqsel, est_hint, td);
	}
	else
	{
		if (opts->shard || opts->shard_sz == 1)
		{
			/*
			 * Just request whatever was given to us.
			 */
			 
			shard = opts->shard ? opts->shard : opts->shard_head[0];
			esg = vh_esg_create();
			
			es = esg_sel_generate_singletd_shard(opts, nqsel, est_hint,
												 shard, 0);
			
			if (es)
			{
				vh_esg_addstep(esg, es);
				
				return esg;
			}
			
			vh_esg_destroy(esg);
			
			return 0;
		}
		else if (opts->shard_head && opts->shard_sz > 1)
		{
			/*
			 * Setup a funnel
			 */
			esg = vh_esg_create();
			esf = vh_es_create(EST_Funnel, 0);
			
			esf->hbno = opts->hbno;
			
			vh_esg_addstep(esg, (ExecStep)esf);
			
			esg_sel_generate_singletd_mshard(opts, esg, nqsel, td);
			
			return esg;
		}
	}
	
	return 0;
}

static ExecStep 
esg_sel_generate_singletd_shard(ExecStepGroupOpts opts,
								NodeQuerySelect nqsel,
								enum ExecStepTag est_hint,
								Shard shard,
								PlannedStmt *pstmt)
{
	ExecStepFetch esfetch;
	BackEnd be;
	PlannedStmt pstmt_lcl;
	PlannedStmtShard pstmtshd = 0;
	ShardAccess sharda;
	bool form_pstmt = true;
	
	if (pstmt && *pstmt)
	{
		form_pstmt = false;
		pstmt_lcl = *pstmt;
	}

	
	if (shard)
	{
		sharda = shard->access[0];
		be = sharda->be;
		
		if (form_pstmt && be)
		{
			pstmt_lcl = vh_pstmt_generate_from_query((NodeQuery)nqsel, be);
			pstmt_lcl->nquery = (NodeQuery)nqsel; 
			pstmt_lcl->be = be;


			vh_pstmt_qrp(pstmt_lcl);
			
			if (pstmt && pstmt_lcl)
				*pstmt = pstmt_lcl;
		}
		
		//pstmtshd = vh_pstmtshd_generate(pstmt_lcl, sharda);
		pstmtshd->shard = shard;
		
		vh_SListPush(pstmt_lcl->shards, shard);
		
		esfetch = vh_es_create(EST_Fetch, 0);
		esfetch->pstmt = pstmt_lcl;
		esfetch->pstmtshd = pstmtshd;
		esfetch->hbno = opts->hbno;
		
		return (ExecStep)esfetch;		
	}
	
	return 0;
}

static void 
esg_sel_generate_singletd_mshard(ExecStepGroupOpts opts,
								 ExecStepGroup esg,
								 NodeQuerySelect nqsel,
								 TableDef td)
 {
	 KeyValueList shard_by_be;
	 BackEnd be;
	 SList shards = 0;
	 Shard *shard_head, shard;
	 PlannedStmt pstmt;
	 ExecStep es, es_prev = 0;
	 KeyValueListIterator it;
	 uint32_t shard_sz = 0, i;
	 
	 shard_by_be = vh_shard_group_be(opts->shard_head, shard_sz, 0);
	 
	 if (shard_by_be)
	 {
		 vh_kvlist_it_init(&it, shard_by_be);
		 
		 while (vh_kvlist_it_next(&it, &be, shards))
		 {
			 /*
			  * Be cautious about resetting pstmt to null.  A null pstmt
			  * pointer passed to singletd_shard will spin up a new
			  * PlannedStmt automatically and attach it to the ExecStep.
			  */
			  
			shard_sz = vh_SListIterator(shards, shard_head);
			  
			for (i = 0; i < shard_sz; i++)
			{
				shard = shard_head[i];
				
				if (i == 0)
					pstmt = 0;
					
				es = esg_sel_generate_singletd_shard(opts, nqsel,
													 EST_Fetch, shard,
													 &pstmt);
				
				if (es)
				{
					if (es_prev)
						vh_esg_addsibling(es_prev, es);
					else
						vh_esg_addstep(esg, es);
					
					es_prev = es;
				}
			}
		 }
		 
		 vh_kvlist_destroy(shard_by_be);
	 }
 }

static SList
esg_sel_tds(NodeQuerySelect nqsel)
{
	SList nodes = 0, tds = 0, temp;

	if (nqsel->from)
	{
		nodes = vh_esg_from_pullup(nqsel->from);
		tds = vh_esg_from_list_tds(nodes, true);
		vh_SListDestroy(nodes);
	}

	if (nqsel->joins)
	{
		nodes = vh_esg_join_pullup(nqsel->joins);

		if (tds)
		{
			temp = vh_esg_join_list_tds(nodes, true);
			vh_SListMerge(tds, temp);
			vh_SListDestroy(temp);
		}
		else
		{
			tds = vh_esg_join_list_tds(nodes, true);
		}

		vh_SListDestroy(nodes);
	}

	return tds;
}

