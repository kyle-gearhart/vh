/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_plan_tree_H
#define vh_datacatalog_plan_tree_H


/*
 * vh_plan_tree
 *
 * The function traverses a specific node query tree to:
 * 	1)	Fold qual constants associated with a TableField into new HeapTuple
 * 		a)	If several fields are chained together with AND operator within the 
 * 			node parent, we'll put all those on one HeapTuple.
 * 	2)	Gathers all HeapTuplePtr referenced by the query (including those created
 * 		as a result of the folding in step 2) into three data structures:
 * 		a)	|tds| Lists all HeapTuplePtr for a given TableDef
 * 		b)	|quals| Lists all HeapTuplePtr for a given Node
 * 		c)	|tups| Lists all HeapTuplePtr in the above two structures
 *
 * We can then submit the tuples to the beacon for shard discovery.  Shard
 * discovery will simply set the Shard on the HeapTuple data structure itself.
 *
 * After we get the shards, we can iterate the HeapTuple list.  With each
 * HeapTuplePtr, we lookup the HeapTuplePtr in |node_htp| to find the 
 * Nodes associated with the HeapTuplePtr.  If the node isn't a NodeFrom or
 * NodeJoin, then we need to look one up using |node_qual|.  We have now have
 * the opporunity to associate unique shards with From/Join nodes.
 *
 * It's recommended the association look something like this:
 * 	KeyValueMap		key: NodeFrom/NodeJoin
 * 					value: KeyValueList		key: Shard
 * 											value: HeapTuplePtr 
 *
 * The Join Tree |node_jt| could then be iterated.  Only OUTTER nodes will 
 * be added as keys to the Join Tree. Intersections and differences can be 
 * computed using the standard KeyValueList functions from the
 * Node/Shard/HeapTuplePtr association above.  The ESG functions can then
 * decide how to perform the join.  If a perfect itersection exists then
 * all we have to do is splice all the quals up to the correct shard and
 * layer everything under single funnel node.
 *
 * The goal is to give the esg_ functions enough data in PlanFlattenData to
 * make decisions about forming an execution plan without having to recurse
 * the NodeQuery tree again.  
 */

typedef struct PlanTreeData *PlanTree;
typedef struct PlanTreeOptsData PlanTreeOptsData, *PlanTreeOpts;
typedef struct PlanBeaconRootData *PlanBeaconRoot;

struct PlanTreeData
{
	/* HeapTuplePtr indexes */
	KeyValueList htp_beacons;	/* key: Beacon; value: HeapTuplePtr */
	KeyValueList htp_tds;		/* key: TableDef; value: HeapTuplePtr */
	KeyValueList htp_quals;		/* key: Node; value: HeapTuplePtr */
	KeySet htp;					/* HeapTuplePtr */


	/* TableDef indexes */
	KeyValueList td_beacons;	/* key: Beacon; value: TableDef */
	KeyValueMap td_htp;			/* key: HeapTuplePtr; value: TableDef */
	
	/* Node indexes */
	KeyValueList node_fj;		/* key: NodeFrom/NodeJoin; value: NodeQual */
	KeyValueList node_htp;		/* key: HeapTuplePtr; value: Node */
	KeyValueList node_td; 		/* key: TableDef; value: Node */
	KeyValueList node_qual;		/* key: NodeQual; value: NodeFrom/NodeJoin */
	KeyValueMap node_jt;		/* key: NodeFrom/NodeJoin; value: KeyValueList
								   								key: NodeFrom/NodeJoin;
																value: NodeQual */
	
	SList beacon_roots;			/* PlanBeaconRoot */
};


struct PlanTreeOptsData
{
};

struct PlanBeaconRootData
{
	Beacon beacon;
	TableDef td;
	Node node; 		// NodeFrom or NodeJoin
	TableRel tr;	// Matching TableRel

	bool is_root;	// |td| is the beacon's TableDef
	bool is_related;	// |td| is directly related to the beacon's TableDef
	bool is_proxy;		// |td| is a proxy to the beacon because it has the
						// same mandatory (i.e. non-null) fields as
	bool is_uk;			// there's no relation back to the beacon root, but we
						// need to fetch results
	SList rquals;		// beacon restrictive quals
};

bool vh_plan_tree(NodeQuery nquery, PlanTree* input, PlanTreeOpts opts);
void vh_plan_tree_destroy(PlanTree tree);

Node vh_plan_tree_getnode_td(PlanTree tree, TableDef td, uint32_t idx, uint32_t *n);

#endif

