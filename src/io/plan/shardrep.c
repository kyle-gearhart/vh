/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/nodes/NodeQuery.h"
#include "io/plan/paramt.h"
#include "io/plan/shardrep.h"
#include "io/shard/Beacon.h"
#include "io/shard/Shard.h"
#include "io/shard/shtm.h"
#include "io/utils/SList.h"


typedef struct ShardRepNodeData
{
	Node original;
	Node current;

	Node original_left_sibling;
	Node original_right_sibling;
	SList original_children;
	bool original_se[2];
} *ShardRepNode;

struct ShardRepData
{
	NodeQuery nq;
	Shard shard;
	ShardHeapTupleMap shtm;

	SList rep_nodes;
	MemoryContext mctx;

	bool restored;
};

static void plan_sr_copy_node(ShardRep sr, Node replace);
static void plan_sr_restore_node(ShardRepNode srn);

ShardRep
vh_plan_sr_create(NodeQuery nq, Shard shard,
				  void *htp_shard_map,
				  ParamTransferMap ptm)
{
	ShardRep sr;
	MemoryContext mctx_old, mctx_sr;

	mctx_old = vh_mctx_current();
	mctx_sr = vh_MemoryPoolCreate(mctx_old, 8192,
								  "ShardRep context");
	vh_mctx_switch(mctx_sr);

	sr = vhmalloc(sizeof(struct ShardRepData));
	sr->rep_nodes = vh_SListCreate();
	sr->nq = nq;
	sr->shard = shard;
	sr->shtm = htp_shard_map;
	sr->mctx = mctx_sr;
	sr->restored = false;

	vh_mctx_switch(mctx_old);

	return sr;
}

void
vh_plan_sr_destroy(ShardRep sr)
{
	if (!sr->restored)
		vh_plan_sr_restore(sr);

	vh_mctx_destroy(sr->mctx);
}

/*
 * We restore to the original NodeQuery structure by iterating the |rep_nodes|
 * backwards.
 */
void
vh_plan_sr_restore(ShardRep sr)
{
	ShardRepNode *srn_head;
	uint32_t srn_sz, i;

	srn_sz = vh_SListIterator(sr->rep_nodes, srn_head);

	if (sr->restored)
	{
		/*
		 * If it's been restored, just issue an error message and abort the function.
		 */
	}
	else
	{
		for (i = srn_sz; i > 0; --i)
		{
			plan_sr_restore_node(srn_head[i]);

			/*
			 * Kill the replacement node.
			 */	
		}

		sr->restored = true;
	}
}

/*
 * Copies the node |replace| by  calling it's copy function.  We start by maintaining
 * the child links and next sibling links.  Then we step up to the parent and maintain
 * the downlink to the new node.  
 */
static void
plan_sr_copy_node(ShardRep sr, Node replace)
{
	struct NodeCopyState node_cs = { };
	Node copy, it, last;
	ShardRepNode srn;

	if (replace->funcs && replace->funcs->copy)
	{
		srn = vhmalloc(sizeof(struct ShardRepNodeData));
		copy = replace->funcs->copy(&node_cs, replace, 0);

		srn->original_se[0] = false;
		srn->original_se[1] = false;

		copy->firstChild = replace->firstChild;
		copy->lastChild = replace->lastChild;
		copy->nextSibling = replace->nextSibling;

		it = copy->firstChild;

		if (it)
		{
			srn->original_children = vh_SListCreate();

			while (it)
			{
				vh_SListPush(srn->original_children, it);
				it->parent = copy;
				it = it->nextSibling;
			}
		}

		copy->parent = replace->parent;

		if (copy->parent)
		{
			if (copy->parent->firstChild == replace)
			{
				copy->parent->firstChild = copy;
				srn->original_se[0] = true;
			}
			
			if (copy->parent->lastChild == replace)
			{
				copy->parent->lastChild = copy;
				srn->original_se[1] = true;
			}
			
			it = copy->parent->firstChild;
			last = 0;

			while (it)
			{
				if (it == replace)
				{
					if (last)
					{
						last->nextSibling = copy;
						srn->original_left_sibling = last;
					}

					srn->original_right_sibling = it->nextSibling;

					break;
				}

				last = it;
				it = it->nextSibling;
			}
		}

		srn->current = copy;
		srn->original = replace;

		vh_SListPush(sr->rep_nodes, srn);
	}
}

/*
 * Maintain the parent uplink back to the original node for all children.  Then
 * maintain the left and right sibling sublinks.  Go up to the original parent
 * and make sure the start and end sibling links are maintained.
 */

static void
plan_sr_restore_node(ShardRepNode srn)
{
	Node it, *it_head;
	uint32_t it_sz, i;

	if (srn->original_children)
	{
		it_sz = vh_SListIterator(srn->original_children, it_head);

		for (i = 0; i < it_sz; i++)
		{
			it = it_head[i];
			it->parent = srn->original;
		}
	}

	if (srn->original_left_sibling)
		srn->original_left_sibling->nextSibling = srn->original;

	if (srn->original_right_sibling)
		srn->original->nextSibling = srn->original_right_sibling;

	if (srn->original->parent)
	{
		if (srn->original_se[0])
			srn->original->parent->firstChild = srn->original;

		if (srn->original_se[1])
			srn->original->parent->lastChild = srn->original;
	}
}

