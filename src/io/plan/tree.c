/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeJoin.h"
#include "io/nodes/NodeOrderBy.h"
#include "io/nodes/NodeQual.h"
#include "io/nodes/NodeQuery.h"
#include "io/plan/tree.h"
#include "io/shard/Beacon.h"
#include "io/utils/kvlist.h"
#include "io/utils/kvmap.h"
#include "io/utils/kset.h"
#include "io/utils/SList.h"


/* Pull Up TDs */
static void pt_pullup_tds_init(PlanTree pt, PlanTreeOpts ptopts);
static void pt_pullup_tds(Node node, void *data);


/* Scan Quals */
static void pt_scan_quals_init(PlanTree pt, PlanTreeOpts ptopts);
static void pt_scan_quals_add_jt(PlanTree pt, Node lhs, Node rhs, NodeQual nqual);
static void pt_scan_quals(Node node, void *data);

static void pt_scan_htp_slist(TableDef parent_td, SList htps, 
							  Node node, PlanTree pt);
static void pt_scan_htp(TableDef parent_td, HeapTuplePtr htp,
						Node node, PlanTree pt);


/* Pull Up and Form HeapTuple from QualList */
struct PullUpTupleQualList
{
};

static void pt_pullup_ht_init(struct PullUpTupleQualList*);
static void pt_pullup_ht(Node node, void *data);
static void pt_pullup_htend(struct PullUpTupleQualList*);

static void pt_br_detect(PlanTree pt);
static PlanBeaconRoot pt_br_get_bynode(PlanTree pt, Node node);

static SList pt_jt_maketablerel_from_qual(PlanTree pt, Node outter, Node inner);
static void pt_jt_directlink(PlanTree pt);
static void pt_jt_indirectlink(PlanTree pt, SList hint);


/*
 * We do two complete scans on the tree.  The first scan simply populates 
 * |node_td| mapping TableDefs to nodes.  The second scan examines the entire
 * tree again and builds up the actual join tree.
 *
 * Things get complicated in the second traversal of the tree.
 */

bool
vh_plan_tree(NodeQuery nquery, PlanTree *input, PlanTreeOpts ptopts)
{
	if (nquery && input)
	{
		pt_pullup_tds_init(*input, ptopts);
		vh_nsql_visit_tree((Node)nquery, pt_pullup_tds, input);

		pt_scan_quals_init(*input, ptopts);
		vh_nsql_visit_tree((Node)nquery, pt_scan_quals, input);

		pt_br_detect(*input);

		return true;
	}

	return false;
}



/* Pull Up TDs */

static void
pt_pullup_tds_init(PlanTree pt, PlanTreeOpts ptopts)
{
	if (pt->node_td)
		vh_kvlist_destroy(pt->node_td);

	pt->node_td = vh_kvlist_create();
}

static void
pt_pullup_tds(Node node, void *data)
{
	PlanTree pt = data;
	NodeFrom nfrom;
	NodeJoin njoin;
	SList values;

	switch (node->tag)
	{
	case From:
		nfrom = (NodeFrom) node;

		vh_kvlist_value(pt->node_td, &nfrom->tdv, values);
		vh_SListPush(values, nfrom);	

		break;

	case Join:
		njoin = (NodeJoin) node;
		nfrom = &njoin->join_table;

		vh_kvlist_value(pt->node_td, &nfrom->tdv, values);
		vh_SListPush(values, njoin);

		break;

	default:
		/* Do nothing */
		break;
	}
}



/* Scan Quals */

static void 
pt_scan_quals_init(PlanTree pt, PlanTreeOpts ptopts)
{
	pt->htp_quals = vh_kvlist_create();
	pt->node_qual = vh_kvlist_create();
	pt->node_jt = vh_kvmap_create();
}

static void 
pt_scan_quals(Node node, void *data)
{
	PlanTree pt = data;
	Node njoins[2];
	NodeFrom nfrom;
	NodeJoin njoin;
	NodeQual nqual;
	NodeField nfield;
	TableField tf;
	TableDef td, tds[2];
	NodeQualS nquals[2];
	SList values;
	bool is_constant[2];
	uint8_t i;

	switch (node->tag)
	{
	case From:
		nfrom = (NodeFrom) node;

		if (nfrom->htps)
			pt_scan_htp_slist(nfrom->tdv->td, nfrom->htps, node, pt);

		break;

	case Join:
		njoin = (NodeJoin) node;
		nfrom = &njoin->join_table;

		if (nfrom->htps)
			pt_scan_htp_slist(nfrom->tdv->td, nfrom->htps, node, pt);



		break;

	case Qual:
		nqual = (NodeQual) node;
		nquals[0] = &nqual->lhs;
		nquals[1] = &nqual->rhs;

		is_constant[0] = false;
		is_constant[1] = false;

		for (i = 0; i < 2; i++)
		{
			switch (nquals[i]->format)
			{
			case TableFieldRef:
				tf = nquals[i]->table_field;
				tds[i] = tf->tdv->td;

				/* Search for a NodeFrom or NodeJoin this belongs to */
				if (vh_kvlist_value(pt->node_td, &tds[i], values))
				{
					if (vh_SListSize(values) == 1)
					{
						njoins[i] = vh_SListFirst(values);

						switch (njoins[i]->tag)
						{
						case From:
							nfrom = (NodeFrom)njoins[i];
							td = nfrom->tdv->td;

							break;

						case Join:
							njoin = (NodeJoin)njoins[i];
							nfrom = &njoin->join_table;
							td = nfrom->tdv->td;

							break;

						default:
							break;
						}

						vh_kvlist_value(pt->node_qual, &node, values);
						vh_SListPush(values, njoins[i]);

						vh_kvlist_value(pt->node_fj, &njoins[i], values);
						vh_SListPush(values, node);
					}
					else
					{
						elog(ERROR2,
							 emsg("Unable to determine the NodeFrom/NodeJoin "
								  "the NodeQual belongs to because there are "
								  "multiple JOINs to the same TableDef.  Try "
								  "converting the NodeQual to a QueryFieldRef "
								  "instead of a TableFieldRef or only join the "
								  "referenced TableDef once in the query."));
					}
				}
				else
				{
					elog(ERROR2,
						 emsg("Corrupt NodeQuery tree, expected to find a "
							  "TableDef for %s in the query parse |node_td| "
							  "but it did not exist",
							  vh_str_buffer(td->tname)));
				}
				
				break;

			case QueryFieldRef:

				nfield = nquals[i]->field;
				njoins[i] = (Node)nfield->nfrom;
				tf = nfield->tf;
				tds[i]= tf->tdv->td;

				switch (njoins[i]->tag)
				{
				case From:
					nfrom = nfield->nfrom;

					break;

				case Join:
					nfrom = &nfield->njoin->join_table;
					break;

				default:
					/* Fix complier warning */
					nfrom = 0;
				}

				if (vh_kvlist_value(pt->node_qual, &node, values))
				{
					vh_SListPush(values, nfrom);
				}
				else
				{
					elog(ERROR2,
						 emsg("Corrupt NodeQuery tree, expected to find a "
							  "corresponding NodeFrom or NodeJoin in the query"));
				}

				if (vh_kvlist_value(pt->node_fj, &njoins[i], values))
					vh_SListPush(values, node);

				break;

			case HeapTupleRef:
				td = 0;
				tds[i] = 0;
				break;

			case HeapTupleFieldRef:
				td = 0;
				tds[i] = 0;
				break;

			default:

				break;
			}

			if (tds[i])
			{
				vh_kvlist_value(pt->node_td, &tds[i], values);
				vh_SListPush(values, node);
			}
		}

		/*
		 * If both sides of the qual are not constants, when we need to build up
		 * the join tree with node_jt.  To do this properly, we should only hang
		 * nodes that are a parent.  Thus iterating node_jt alone only provides
		 * Nodes that have children.  We detect this thru the cardinality of the
		 * two tables.  We can then iterate PlanBeaconRoot via |beacon_roots| and
		 * find all of the joins hanging from there.  Since we only rehang parents
		 * in the tree, we can recursively step thru join tree to check to see if
		 * everything is hung off the beacon root by atleast its natural key.
		 */
		if (is_constant[0] == is_constant[1])
		{
			if (!is_constant[0] && njoins[0] && njoins[0] != njoins[1])
			{
				TableRel tr = vh_tdr_get(tds[0], tds[1]);

				if (tr && tr->op)
				{
					switch (tr->card)
					{
					case Rel_OneToOne:
					case Rel_OneToMany:
					case Rel_ManyToMany:
						pt_scan_quals_add_jt(pt, njoins[0], njoins[1], nqual);
						break;
					
					case Rel_ManyToOne:
						pt_scan_quals_add_jt(pt, njoins[1], njoins[0], nqual);
						break;
					}
				}
				else
				{
					/*
					 * We couldn't find a natural relationship between the tables
					 * so just do it in the order we found the quals.
					 */
					pt_scan_quals_add_jt(pt, njoins[0], njoins[1], nqual);
				}
			}
		}

		break;

	case QualList:
		/*
		 * QualLists have special properties for PlanTree.  In the event the 
		 * user has already pushed down Quals for a HeapTuple key, we're
		 * going to want to detect that and form up a HeapTuple for shard
		 * resolution.  We use this opporunity to setup another tree 
		 * recursion to do this.  We expect to do constant folding.
		 */

		//vh_nsql_visit_tree(node, pt_pullup_ht, &htql);

	default:

		break;
	}
}


/*
 * Adds a join pairing to the join tree.  The caller is responsible for
 * reversing the LHS and RHS join nodes to add the inverse to the tree.
 */
static void
pt_scan_quals_add_jt(PlanTree pt, Node lhs, Node rhs,
					 NodeQual nqual)
{
	KeyValueList kvl, *kvl_search;
	SList nquals;

	if (!vh_kvmap_value(pt->node_jt, lhs, kvl_search))
	{
		kvl = vh_kvlist_create();
		*kvl_search = kvl;
	}
	else
	{
		kvl = *kvl_search;
	}

	if (kvl)
	{
		vh_kvlist_value(kvl, &rhs, nquals);
		vh_SListPush(nquals, nqual);
	}
}

static void 
pt_scan_htp_slist(TableDef parent_td, SList htps, 
				  Node node, PlanTree pt)
{
	HeapTuplePtr *htp_head;
	uint32_t htp_sz, i;

	htp_sz = vh_SListIterator(htps, htp_head);

	for (i = 0; i < htp_sz; i++)
		pt_scan_htp(parent_td, htp_head[i], node, pt);
}

/*
 * We're going to update 6 PlanTree members from the HeapTuplePtr |htp|.
 * 	1)	htp_beacons
 * 	2)	htp_tds
 * 	3)	htp
 * 	4)	td_beacons
 * 	5)	td_htp
 * 	6)	node_htp
 */
static void 
pt_scan_htp(TableDef parent_td, HeapTuplePtr htp,
			Node node, PlanTree pt)
{
	HeapTuple ht;
	TableDef td;
	Beacon beacon;
	SList values;
	void **map_value;

	ht = vh_htp(htp);
	
	if (ht)
	{
		td = (TableDef) ht->htd;
		beacon = td->beacon;

		/* 1	htp_beacons */
		vh_kvlist_value(pt->htp_beacons, &beacon, values);
		vh_htp_SListPush(values, htp);

		if (!parent_td || td == parent_td)
		{
			/* 2	htp_tds */
			vh_kvlist_value(pt->htp_tds, &td, values);
			vh_htp_SListPush(values, htp);
		
			/* 3	htp */
			if (!vh_kset_key(pt->htp, &htp))
			{
				/* 4	td_beacons */
				vh_kvlist_value(pt->td_beacons, &beacon, values);
				vh_SListPush(values, td);
			}

		}

		/* 5	td_htp */
		vh_kvmap_value(pt->td_htp, &htp, map_value);
		*map_value = td;

		/* 6 	node_htp */
		vh_kvlist_value(pt->node_htp, &htp, values);
		vh_SListPush(values, node);
	}
}

/*
 * We should probably cache BeaconRoot information in the Catalog somewhere.
 * Realizing some gains here by just doing lookups for each query processed
 * is likely worth the memory overhead of just indexing these relations in
 * the catalog as they're requested.
 */
static void 
pt_br_detect(PlanTree pt)
{
	Beacon beacon;
	TableDef beacon_root = 0, *td_head;
	SList tds = 0;
	uint32_t i, j, td_sz, node_count;
	TableRel pbr_tr;
	PlanBeaconRoot pbr;
	KeyValueListIterator it;
	Node td_node;
	KeySet ks_ignore_node;

	ks_ignore_node = vh_kset_create();

	vh_kvlist_it_init(&it, pt->td_beacons);
	while (vh_kvlist_it_next(&it, &beacon, tds))
	{
		//beacon_root = vh_beac_td(beacon);

		/*
		 * Iterate all of the TableDef's in the query associated with the
		 * beacon.
		 */
		td_sz = vh_SListIterator(tds, td_head);

		for (i = 0; i < td_sz; i++)
		{
			node_count = 1;

			for (j = 0; j < node_count; j++)
			{
				//td_node = vh_plan_tree_getnode_td(pt, td_head[i], j, &node_count);
				pbr = 0;

				if (td_node && 
					!vh_kset_exists(ks_ignore_node, &td_node))
				{
					if (td_head[i] == beacon_root)
					{
						pbr = vhmalloc(sizeof(struct PlanBeaconRootData));
						pbr->node = td_node;
						pbr->td = td_head[i];
						pbr->beacon = beacon;
						pbr->is_root = true;
						pbr->is_related = false;
						pbr->is_proxy = false;
						pbr->tr = 0;
					}
					else
					{
						/*
						 * Let's check the relations to see if this table can be
						 * directly joined to the beacon root.
						 */

						pbr_tr = vh_tdr_get(beacon_root, td_head[i]);
						
						if (pbr_tr)
						{
							if (pbr_tr->card == Rel_OneToOne ||
								pbr_tr->card == Rel_OneToMany)
							{
								pbr = vhmalloc(sizeof(struct PlanBeaconRootData));
								pbr->node = td_node;
								pbr->td = td_head[i];
								pbr->beacon = beacon;
								pbr->is_root = false;
								pbr->is_related = true;
								pbr->is_proxy = false;
								pbr->tr = pbr_tr;
							}
						}

						/*
						 * Let's check to see if this table has corresponding,
						 * mandatory (i.e. non-null) fields that may be directly
						 * joined with beacon root's identification columns.
						 */
					}

					if (pbr)
					{
						vh_SListPush(pt->beacon_roots, pbr);
						vh_kset_key(ks_ignore_node, &td_node);
					}
				}
			}
		}
	}

	vh_kset_destroy(ks_ignore_node);
}

static PlanBeaconRoot 
pt_br_get_bynode(PlanTree pt, Node node)
{
	PlanBeaconRoot *pbr_head;
	uint32_t i, pbr_sz;

	if (pt->beacon_roots)
	{
		pbr_sz = vh_SListIterator(pt->beacon_roots, pbr_head);

		for (i = 0; i < pbr_sz; i++)
		{
			if (pbr_head[i]->node == node)
				return pbr_head[i];
		}
	}

	return 0;
}

static SList 
pt_jt_maketablerel_from_qual(PlanTree pt, Node outter, Node inner)
{
	TableRelQual trq;
	NodeQual *nqual_head, nqual;
	NodeQualS nquals[2];
	SList quals, trqs;
	KeyValueList *kvl_inner;
	TableField tfs[2];
	uint32_t nqual_sz, i, j;

	trqs = 0;

	vh_kvmap_value(pt->node_jt, &outter, kvl_inner);
	vh_kvlist_value(*kvl_inner, &inner, quals);

	if (quals)
	{
		nqual_sz = vh_SListIterator(quals, nqual_head);

		for (i = 0; i < nqual_sz; i++)
		{
			nqual = nqual_head[i];
			nquals[0] = &nqual->lhs;
			nquals[1] = &nqual->rhs;

			if (nqual->oper != Eq)
				continue;
			
			for (j = 0; j < 2; j++)
			{
				switch (nquals[j]->format)
				{
				case QueryFieldRef:
					tfs[j] = nquals[j]->field->tf;
					break;

				case TableFieldRef:
					tfs[j] = nquals[j]->table_field;
					break;

				default:
					tfs[j] = 0;
					break;
				}
			}

			if (tfs[0] && tfs[1])
			{
				trq = vhmalloc(sizeof(struct TableRelQualData));
				trq->tf_outter = tfs[0];
				trq->tf_inner = tfs[1];

				if (!trqs)
					trqs = vh_SListCreate();

				vh_SListPush(trqs, trq);
			}
		}
	}	
	
	return trqs;
}

/*
 * Iterate |node_jt| and put every NodeFrom/Node join we cannot immediately trace
 * back to a BeaconRoot with it's natural TableDef key into a SList.  We'll then
 * take the SList and recursively step thru the join tree to see if this is just
 * a table who's relation to the beacon root is thru another query in the table.
 *
 * Only tables without any sensible link to a beacon root are placed in
 * |indirect_jt| which will typically invoke a cross shard scan.
 */
static void 
pt_jt_directlink(PlanTree pt)
{
	PlanBeaconRoot pbr, *pbr_head;
	KeyValueList *kvl_join;
	Node noutter;
	uint32_t i, pbr_sz;

	pbr_sz = vh_SListIterator(pt->beacon_roots, pbr_head);

	for (i = 0; i < pbr_sz; i++)
	{
		pbr = pbr_head[i];

		if (!pbr)
		{
		}

		vh_kvmap_value(pt->node_jt, &noutter, kvl_join);

		if (*kvl_join)
		{
			/* The current PlanBeaconRoot also has nested children in the
			 * join tree.  We should iterate all joins recursively, stepping
			 * down the tree until kvl_join is null.  Each step should check
			 * to make sure the child table is joined by atleast its natural
			 * key.  If it's not joined by its natural key, check to see what
			 * quals are available for the relation.  If any are present,
			 * we'll need to do a cross shard query to fetch.
			 */
		}
	}
}


static void 
pt_jt_indirectlink(PlanTree pt, SList hint)
{
}

