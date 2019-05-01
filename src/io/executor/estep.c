/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/executor/eplan.h"
#include "io/executor/estep.h"
#include "io/executor/estepfwd.h"
#include "io/plan/pstmt.h"
#include "io/utils/kset.h"
#include "io/utils/SList.h"



static void es_visit_tree_recurse(ExecStep root,
		 						  vh_es_visit_tree_func funcs[2],
		 						  void *data[2]);

/*
 * ScanUniqueShard Infrastructure
 *
 * Used to find all unique Shards in a ExecStep tree.  Relies on
 * vh_es_visit_tree to function.
 */
struct ScanUniqueShard
{
	KeySet ks;
};

static void es_sus_recurse(ExecStep es, void *state);


void*
vh_es_create(enum ExecStepTag tag, ExecStep parent)
{
	ExecStep es;

	switch (tag)
	{
	case EST_CommitHeapTups:
		es = 0;
		break;

	case EST_Discard:
		es = vhmalloc(sizeof(struct ExecStepDiscardData));
		break;

	case EST_Fetch:
		es = vhmalloc(sizeof(struct ExecStepFetchData));
		break;

	case EST_Funnel:	
		es = vhmalloc(sizeof(struct ExecStepFunnelData));
		break;

	default:
		es = 0;
		break;
	}

	if (es)
	{
		es->tag = tag;
		es->sibling = 0;
		es->fwd = 0;
		es->child = 0;
		es->parent = 0;
	}

	return es;
}

/*
 * vh_es_shard
 *
 * Returns the shard associated with the ExecStep.  If no shard has been
 * identified or if the ExecStep doesn't require a shard, null will be
 * returned.
 */

Shard
vh_es_shard(ExecStep estep)
{
	Shard shard = 0;

	switch (estep->tag)
	{
	case EST_Fetch:
		{
			ExecStepFetch esf = (ExecStepFetch) estep;

			if (esf->pstmtshd)
				shard = esf->pstmtshd->shard;
		}

		break;

	case EST_Discard:
		{
			ExecStepDiscard esd = (ExecStepDiscard) estep;

			if (esd->pstmtshd)
				shard = esd->pstmtshd->shard;
		}

		break;

	default:
		shard = 0;
		break;		
	}

	return shard;
}

/*
 * vh_es_shard_unique
 *
 * Puts all unique shards contained in an ExecutionStep tree into an SList.
 */
	
SList
vh_es_shard_unique(ExecStep root)
{
	struct ScanUniqueShard sus;
	SList shards;
	vh_es_visit_tree_func sus_func[2];
	void *sus_data[2];

	if (root)
	{
		sus_func[0] = es_sus_recurse;
		sus_func[1] = 0;
		sus_data[0] = &sus;
		sus_data[1] = 0;
		
		sus.ks = vh_kset_create();

		vh_es_visit_tree(root, sus_func, sus_data);
		
		shards = vh_kset_to_slist(sus.ks);		
		
		vh_kset_destroy(sus.ks);

		return shards;
	}

	return 0;
}


static void 
es_sus_recurse(ExecStep es, void *state)
{
	Shard shard;
	struct ScanUniqueShard *sus = state;
	bool found;

	shard = vh_es_shard(es);

	if (shard)
	{
		found = vh_kset_exists(sus->ks, &shard);

		/*
		 * Insert the key into the KeySet if it cannot be found.
		 */
		if (!found)
			vh_kset_key(sus->ks, &shard);
	}
}

/*
 * Visits the ExecStep tree by dropping all the way to the lowest left most
 * node and processing back up.  The first function/data pairing is executed
 * prior to traversing the children.  The second executes after all the children
 * have been traversed.
 */
void
vh_es_visit_tree(ExecStep root,
				 vh_es_visit_tree_func funcs[2],
				 void *data[2])
{
	if (root)
	{
		/*
		 * Don't bother traversing if the user doesn't provide any callbacks
		 */

		if (funcs[0] || funcs[1])
		{
			es_visit_tree_recurse(root, funcs, data);
		}
	}
}

static void
es_visit_tree_recurse(ExecStep root,
		 			  vh_es_visit_tree_func funcs[2],
	 				  void *data[2])
{
	ExecStep child;

	child = root->child;

	if (funcs[0])
		funcs[0](root, data[0]);

	while (child)
	{
		es_visit_tree_recurse(child, funcs, data);
		child = child->sibling;
	}

	if (funcs[1])
		funcs[1](root, data[1]);
}


/*
 * vh_es_pstmt
 *
 * Attempts to pull a PlannedStmt and PlannedStmtShard from the ExecStep.
 * Will return true when both the PlannedStmt and PlannedStmtShard could be
 * set if the caller passes valid pointers to both.  If PlannedStmt or
 * PlannedStmtShard is a valid pointer and the corresponding entity on the
 * ExecStep could be found, it will return true.
 */
bool
vh_es_pstmt(ExecStep estep,
			PlannedStmt *pstmt, PlannedStmtShard *pstmtshd)
{
	PlannedStmt pstmt_lcl;
	PlannedStmtShard pstmtshd_lcl;
	
	if (estep)
	{
		switch (estep->tag)
		{
		case EST_Fetch:
			{
				ExecStepFetch esf = (ExecStepFetch)estep;
				
				pstmt_lcl = esf->pstmt;
				pstmtshd_lcl = esf->pstmtshd;
			}
			
			break;
			
		case EST_Discard:
			{
				ExecStepDiscard esd = (ExecStepDiscard)estep;
				
				pstmt_lcl = esd->pstmt;
				pstmtshd_lcl = esd->pstmtshd;
			}
			
			break;
			
		default:
			pstmt_lcl = 0;
			pstmtshd_lcl = 0;
		}
		
		if (pstmt && pstmtshd)
		{
			*pstmt = pstmt_lcl;
			*pstmtshd = pstmtshd_lcl;
			
			return (pstmt_lcl && pstmtshd_lcl) != 0;
		}
		else if (pstmt)
		{
			*pstmt = pstmt_lcl;
			
			return pstmt_lcl != 0;
		}
		else
		{
			*pstmtshd = pstmtshd_lcl;
			
			return pstmtshd_lcl != 0;
		}
	}
	
	return false;
}

void
vh_est_fetch_idx_init(struct ExecStepFetchData *esf, uint32_t rtups)
{
	assert(esf && esf->pstmt);
	assert(esf->pstmt->qrp_ntables == rtups);

	esf->nhfs = vhmalloc((sizeof(int32_t) + sizeof(HeapField*)) * rtups);
	esf->hfs = (HeapField**)(esf->nhfs + rtups);
	esf->indexed = true;
}

void
vh_est_fetch_idx_add(struct ExecStepFetchData *esf, uint32_t rtup, 
					 uint32_t nhfs)
{
	assert(esf && esf->indexed);
	assert(rtup < esf->pstmt->qrp_ntables);

	esf->nhfs[rtup] = 0;
	esf->hfs[rtup] = vhmalloc(sizeof(HeapField) * nhfs);
}

void
vh_est_fetch_idx_push(struct ExecStepFetchData *esf, uint32_t rtup,
					  HeapField hf)
{
	assert(esf && esf->indexed);	
	esf->hfs[rtup][esf->nhfs[rtup]++] = hf;
}

