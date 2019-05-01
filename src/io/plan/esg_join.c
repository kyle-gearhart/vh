/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/plan/esg_join.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeJoin.h"
#include "io/utils/SList.h"
#include "io/utils/kvlist.h"

static void esg_join_pullup_recurse(Node node, void* njoins);

/*
 * Recursively iterates a Node looking for NodeJoin structures and adds them to a
 * list.  Could be the top of a query tree, it's smart enough to check into a
 * Node that contains a NodeFrom but is not a NodeFrom itself (e.g. NodeJoin).
 */
SList
vh_esg_join_pullup(Node root)
{
	SList njoins;

	if (root)
	{
		njoins = vh_SListCreate();
		vh_nsql_visit_tree(root, esg_join_pullup_recurse, njoins);

		return njoins;
	}

	return 0;
}

/*
 * This is really easy, since we just call the generic vh_nsql_visit_tree
 * function and let this serve as a filter for everything.  If it's a from
 * node, pop it in the list, otherwise forget about it and move on.
 */
static void 
esg_join_pullup_recurse(Node node, void* slist)
{
	SList nfroms = slist;
	NodeJoin njoin;

	switch (node->tag)
	{
	case Join:
		njoin = (NodeJoin)node;

		vh_SListPush(nfroms, njoin);

		break;

	default:
		break;
	}
}



/*
 * Takes a list of NodeFrom and lists the TableDef's involved.  If the unique flag
 * is set, it will only list a TableDef once.
 */
SList
vh_esg_join_list_tds(SList njoins, bool unique)
{
	SList tds;
	NodeJoin *njoin_head;
	uint32_t sz, i;

	if (njoins)
	{
		sz = vh_SListIterator(njoins, njoin_head);
		tds = vh_SListCreate();

		for (i = 0; i < sz; i++)
			vh_SListPush(tds, njoin_head[i]->join_table.tdv->td);

		return tds;
	}

	return 0;
}

/*
 * Gets all of the unique beacons contained in a Node tree from From tags.
 */
KeyValueList
vh_esg_join_beac(Node root)
{
	KeyValueList kvl;
	SList njoins, beacon_list;
	NodeJoin *njoin_head, njoin_i;
	uint32_t njoin_sz, i;
	TableDef td;

	njoins = vh_esg_join_pullup(root);

	if (njoins)
	{
		njoin_sz = vh_SListIterator(njoins, njoin_head);

		if (njoin_sz > 1)
		{
			kvl = vh_kvlist_create();
			
			for (i = 0; i < njoin_sz; i++)
			{
				njoin_i = njoin_head[i];
				
				if (njoin_i && njoin_i->join_table.tdv)
				{
					td = njoin_i->join_table.tdv->td;
					
					vh_kvlist_value(kvl, &td->beacon, beacon_list);
					vh_SListPush(beacon_list, njoin_i);
				}				
			}
			
			vh_SListDestroy(njoins);
		
			return kvl;
		}

		vh_SListDestroy(njoins);
	}
	
	return 0;
}

