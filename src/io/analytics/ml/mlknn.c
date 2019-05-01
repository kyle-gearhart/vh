/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/analytics/ml/mlknn.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/utils/SList.h"


typedef struct mlKNNData mlKNNData;

struct mlKNNData
{
	struct MachLearnData mld;

	SearchPath class;	
	SList training_set;
	
	vh_mlf_distance_func distance;
	void *mlf_distance_cache;

	int32_t k;

	ML_KNN_Mode mode;
};

struct mlKNNVoteData
{
	TypeVarSlot class;
	int32_t votes;
};

struct mlKNNDistanceData
{
	TypeVarSlot distance;
	HeapTuplePtr htp;
};

static int32_t ml_knn_train(void *ml, HeapTuplePtr, HeapTuple);

static int32_t ml_knn_vote_compare(const void *lhs, const void *rhs);
static int32_t ml_knn_class_compare(const void *lhs, const void *rhs);
static int32_t ml_knn_distance_compare(const void *lhs, const void *rhs);

static int32_t ml_knn_populate_slot(void *pc, TypeVarSlot *target,
									TypeVarSlot **datas, int32_t n_datas);

static int32_t ml_knn_finalize(void *ml);

static const struct MachLearnFuncTableData knn_func = {
	.pcf = {
		.populate_slot = ml_knn_populate_slot,
		.finalize = ml_knn_finalize
	},

	.train_htp = ml_knn_train
};



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

void*
vh_ml_knn_create(SearchPath *paths, int32_t n_paths,
				 SearchPath class,
				 vh_mlf_distance_func func,
				 ML_KNN_Mode mode)
{
	struct mlKNNData *knn;

	knn = vh_ml_create(&knn_func, paths, n_paths, sizeof(struct mlKNNData));
	knn->class = class;
	knn->training_set = 0;
	knn->distance = func;
	knn->mlf_distance_cache = 0;
	knn->k = 3;
	knn->mode = mode;

	return knn;
}

static int32_t
ml_knn_finalize(void *ml)
{
	struct mlKNNData *knn = ml;

	if (knn->mlf_distance_cache)
	{
		vh_mlf_distance_destroy(knn->distance, knn->mlf_distance_cache);
	}

	if (knn->training_set)
	{
		vh_SListDestroy(knn->training_set);
	}

	return 0;
}


/*
 * ============================================================================
 * ML Interface
 * ============================================================================
 */
static int32_t
ml_knn_train(void *ml, HeapTuplePtr htp, HeapTuple ht)
{
	mlKNNData *knn = ml;

	if (!knn->training_set)
		vh_htp_SListCreate(knn->training_set);

	vh_htp_SListPush(knn->training_set, htp);

	return 0;
}


/*
 * ============================================================================
 * PrepCol Interface
 * ============================================================================
 */

/*
 * ml_knn_populate_slot
 *
 * Iterate the training set, calculating the distance between each and force the
 * distance into a BTree.  We should ideally only keep n neighbors in the tree.
 */
static int32_t
ml_knn_populate_slot(void *pc, TypeVarSlot *target,
					 TypeVarSlot **datas, int32_t n_datas)
{
	mlKNNData *knn = pc;
	HeapTuplePtr *htp_head, htp;
	struct mlKNNVoteData vote_probe = { }, *votes, *vote;
	struct mlKNNDistanceData *distances;
	size_t working_set;
	HeapTuple ht;
	HeapField *hfs, hf_class = 0;
	int32_t htp_sz, i, j, ret, n_class = 0, distance_count = 0, sp_res;
	TypeVarSlot *slots;

	/*
	 * This is a hot path, so we want to minimize the number of malloc calls
	 * we make.  We know exactly how much space we need so just do one
	 * allocation and split everything out.
	 *
	 * @slots will be our anchor and hold the original pointer so we can free
	 * it.
	 *
 	 *	1) TypeVarSlot to support knn->n_paths + 1
 	 * 	2) HeapField* to suport knn->n_paths
	 * 	3) struct mlKNNVoteData to support knn->k
	 * 	4) struct mlKNNDistanceData to support knn->k + 1
	 */
	working_set = (sizeof(TypeVarSlot) * (knn->mld.n_paths)) +
				  (sizeof(HeapField) * knn->mld.n_paths) +
				  (sizeof(struct mlKNNVoteData) * knn->k) +
				  (sizeof(struct mlKNNDistanceData) * (knn->k + 1));

	htp_sz = vh_SListIterator(knn->training_set, htp_head);
	slots = vhmalloc(working_set);
	hfs = (HeapField*)(slots + knn->mld.n_paths);
	votes = (struct mlKNNVoteData*)(hfs + knn->mld.n_paths);
	distances = (struct mlKNNDistanceData*)(votes + knn->k);

	memset(slots, 0, working_set);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];
		ht = vh_htp(htp);

		/*
		 * Fill our TypeVarSlots from the training data before handing things
		 * over to the distance function.
		 */
		for (j = 0; j < knn->mld.n_paths; j++)
		{
			if (!hfs[j])
			{
				hfs[j] = vh_sp_search(knn->mld.paths[j], &ret, 1, VH_SP_CTX_HT, ht);

				if (j == 0)
				{
					/*
					 * Store our distance result in the first type.  This could
					 * be a little more advanced, but we'll make the assumption
					 * (requirement?) that all distance calculations be performed
					 * on the same type.
					 */
					//vh_tvs_init(distance_result);
					//vh_tvs_store_var(distance_result,
					//				 vh_typevar_make_tys(hfs[j]->types, hfs[j]->type_depth),
					//				 VH_TVS_RA_DVAR);
				}
			}

			vh_tvs_store_ht_hf(&slots[j], ht, hfs[j]);
		}

		/*
		 * Call the distance function, which is going to populate a TypeVarSlot
		 * for us.
		 */
		ret = knn->distance(&knn->mlf_distance_cache,	/* Cache */
							&distances[distance_count].distance,	/* TypeVarSlot: Result */
							slots,				/* TypeVarSlot: Training */
							datas,				/* TypeVarSlot: Instance */
							n_datas);			/* Number of Slots */

		if (ret)
		{
			/*
			 * Distance function threw an error up.
			 */
		}

		distances[distance_count].htp = htp;

		if (distance_count < knn->k)
		{
			/*
			 * Since there are less than k distances in the array at the moment,
			 * they're all eligible.  There's no sense in wasting resources doing
			 * a qsort.  There's nothing to "fall off the edge" in the knn->k + 1
			 * slot.
			 *
			 * Increment the distance_count and call it a day.
			 */
			distance_count++;
		}
		else if (distance_count == knn->k)
		{
			/*
			 * We need to sort the distance array.  Since we store knn->k +1 
			 * distances in the array, by sorting we know we've always got the
			 * closest neighbors.  It's far simplier than standing up another
			 * data tree.  If k is small, it's a quick operation.
			 */

			qsort(distances, distance_count, sizeof(struct mlKNNDistanceData),
				  ml_knn_distance_compare);
		}
	}

	/*
	 * Run thru our sorted distances to get the closest neighbors and do the
	 * voting. 
	 */
	i = 0;
	while (i < knn->k && i < distance_count)
	{
		/*
		 * Pull the classification and vote for it.  (We've got a gap
		 * at the moment, where we should extract the classification
		 * field from the HeapTuplePtr.
		 */

		if (hf_class == 0)
		{
			hf_class = vh_sp_search(knn->class, &sp_res, 
									1, VH_SP_CTX_HTP, distances[i].htp);
		}

		vh_tvs_store_htp_hf(&vote_probe.class, distances[i].htp, hf_class);

		vote = bsearch(&vote_probe, votes, n_class,
					   sizeof(struct mlKNNVoteData),
					   ml_knn_class_compare);

		if (vote)
		{
			vote->votes++;
		}
		else
		{
			/*
			 * Classification doesn't exist in our list yet, so
			 * put a new one at the end of the array, set the 
			 * classification, vote to 1.  Resort the whole array
			 * so our binary search stays in place.
			 */
			votes[n_class].votes = 1;
			vh_tvs_store_htp_hf(&votes[n_class].class, distances[i].htp, hf_class);

			n_class++;

			if (i + 1 < knn->k)
			{
				/*
				 * Don't sort it if we're at the last nearest neighbor,
				 * we have to sort on the votes next.
				 */
				qsort(votes, n_class, sizeof(struct mlKNNVoteData),
					  ml_knn_class_compare);
			}
		}

		i++;
	}

	/*
	 * Sort the votes and populate the @slot with the classification.
	 */

	qsort(votes, n_class, sizeof(struct mlKNNVoteData), ml_knn_vote_compare);
	vote = &votes[n_class - 1];
	vh_tvs_copy(target, &vote->class);

	/*
	 * Clean everything up.  We should probably call finalize on all our
	 * TypeVarSlot sitting in this block of memory.
	 */

	vhfree(slots);

	return 0;
}

/*
 * ml_knn_vote_compare
 *
 * Compare the votes.
 */
static int32_t 
ml_knn_vote_compare(const void *lhs, const void *rhs)
{
	const struct mlKNNVoteData *vlhs = lhs,
		  					   *vrhs = rhs;

	return (vlhs->votes < vrhs->votes ? -1 : vlhs->votes > vrhs->votes);
}

/*
 * ml_knn_class_compare
 *
 * Compare the class
 */
static int32_t
ml_knn_class_compare(const void *lhs, const void *rhs)
{
	const struct mlKNNVoteData *vlhs = lhs,
		  					   *vrhs = rhs;


	return vh_tvs_compare(&vlhs->class, &vrhs->class);
}

/*
 * ml_knn_distance_compare
 *
 * Sorts the distance, from least to greatest.
 */
static int32_t
ml_knn_distance_compare(const void *lhs, const void *rhs)
{
	const struct mlKNNDistanceData *vlhs = lhs, *vrhs = rhs;

	return (vh_tvs_compare(&vlhs->distance, &vrhs->distance));
}

