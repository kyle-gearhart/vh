/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/plan/esg_from.h"
#include "io/nodes/NodeFrom.h"
#include "io/utils/kvlist.h"
#include "io/utils/SList.h"

static void esg_from_pullup_recurse(Node node, void* nfroms);


/*
 * Recursively iterates a Node looking for NodeFrom structures and adds them to a
 * list.  Could be the top of a query tree, it's smart enough to check into a
 * Node that contains a NodeFrom but is not a NodeFrom itself (e.g. NodeJoin).
 */
SList
vh_esg_from_pullup(Node nfrom)
{
	SList nfroms;

	if (nfrom)
	{
		nfroms = vh_SListCreate();
		vh_nsql_visit_tree(nfrom, esg_from_pullup_recurse, nfroms);

		return nfroms;
	}

	return 0;
}

/*
 * This is really easy, since we just call the generic vh_nsql_visit_tree
 * function and let this serve as a filter for everything.  If it's a from
 * node, pop it in the list, otherwise forget about it and move on.
 */
static void 
esg_from_pullup_recurse(Node node, void* slist)
{
	SList nfroms = slist;
	NodeFrom nfrom;

	switch (node->tag)
	{
	case From:
		nfrom = (NodeFrom)node;

		vh_SListPush(nfroms, nfrom);

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
vh_esg_from_list_tds(SList nfroms, bool unique)
{
	SList tds;
	NodeFrom *nfrom_head;
	uint32_t sz, i;

	if (nfroms)
	{
		sz = vh_SListIterator(nfroms, nfrom_head);
		tds = vh_SListCreate();

		for (i = 0; i < sz; i++)
			vh_SListPush(tds, nfrom_head[i]->tdv->td);

		return tds;
	}

	return 0;
}

/*
 * Gets all of the unique beacons contained in a Node tree from From tags.
 */
KeyValueList
vh_esg_from_beac(Node root)
{
	KeyValueList kvl;
	SList nfroms, beacon_list;
	NodeFrom *nfrom_head, nfrom_i;
	uint32_t nfrom_sz, i;

	nfroms = vh_esg_from_pullup(root);

	if (nfroms)
	{
		nfrom_sz = vh_SListIterator(nfroms, nfrom_head);

		if (nfrom_sz > 1)
		{
			kvl = vh_kvlist_create();

			for (i = 0; i < nfrom_sz; i++)
			{
				nfrom_i = nfrom_head[i];
				
				if (nfrom_i && nfrom_i->tdv->td)
				{
					vh_kvlist_value(kvl, &nfrom_i->tdv->td->beacon, beacon_list);
					vh_SListPush(beacon_list, nfrom_i);
				}
			}
			
			vh_SListDestroy(nfroms);

			return kvl;
		}

		vh_SListDestroy(nfroms);
	}

	return 0;
}

