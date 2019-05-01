/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/analytics/ml/mlnaive_bayes.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarSlot.h"

/* Bring in the nest engine to do the summarization */
#include "io/analytics/nestidx.h"
#include "io/analytics/nestlevel.h"


/*
 * ============================================================================
 * Local Structures
 * ============================================================================
 */

typedef struct mlNaiveBayesData mlNaiveBayesData;
typedef struct mlNaiveBayesScanData mlNaiveBayesScanData;

struct mlNaiveBayesData
{
	struct MachLearnData mld;

	MemoryContext mctx;

	SearchPath class;
	Nest nest;
	NestLevel nl;
	vh_mlf_probability_func prob;
	int64_t total_rows;
};

/*
 * struct mlNaivesBayesScanData
 *
 * Used as the user data pointer during a NestIdx scan callback.
 */
struct mlNaiveBayesScanData
{
	mlNaiveBayesData *nb;
	TypeVarSlot **inputs;
	void *prob_cache;
	TypeVarSlot *best_probabilty;
	bool first_probability;
};

static int32_t ml_nb_finalize(void *ml);
static int32_t ml_nb_train(void *ml, HeapTuplePtr htp, HeapTuple ht);
static int32_t ml_nb_populate_slot(void *pc, TypeVarSlot *target,
	   							   TypeVarSlot **datas, int32_t n_datas);	

static int32_t ml_nb_summary_create(mlNaiveBayesData *nb, HeapTuplePtr htp);
static bool ml_nb_scanner(NestIdxAccess nia, void *user);

static const struct MachLearnFuncTableData nb_func = {
	.pcf = {
		.populate_slot = ml_nb_populate_slot,
		.finalize = ml_nb_finalize
	},
	
	.train_htp = ml_nb_train
};



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

void*
vh_ml_naive_bayes_create(SearchPath *paths, int32_t n_paths,
						 SearchPath class,
						 vh_mlf_probability_func prob)
{
	mlNaiveBayesData *nb;

	nb = vh_ml_create(&nb_func, paths, n_paths, sizeof(mlNaiveBayesData));
	nb->class = class;
	nb->nest = 0;
	nb->nl = 0;
	nb->prob = prob;
	nb->total_rows = 0;
	nb->mctx = vh_MemoryPoolCreate(vh_mctx_current(), 1024, "Naive Bayes");

	return nb;
}

static int32_t
ml_nb_finalize(void *ml)
{
	//struct mlNaiveBayesData *nb = ml;

	/*
	 * Destroy the nest and we're done.
	 */

	return 0;
}



/*
 * ============================================================================
 * ML Interface
 * ============================================================================
 */
static int32_t
ml_nb_train(void *ml, HeapTuplePtr htp, HeapTuple ht)
{
	MemoryContext mctx_old;
	mlNaiveBayesData *nb = ml;

	mctx_old = vh_mctx_switch(nb->mctx);

	if (!nb->nest)
	{
		/*
		 * We need to create the summary table before we can submit a HeapTuplePtr
		 * to it.  This tracks the mean, standard deviation and over all count
		 * by each class.
		 */
		ml_nb_summary_create(nb, htp);
	}

	vh_nest_input_htp(nb->nest, htp);
	nb->total_rows++;

	vh_mctx_switch(mctx_old);

	return 0;
}



/*
 * ============================================================================
 * PrepCol Interface
 * ============================================================================
 */
static int32_t 
ml_nb_populate_slot(void *pc, TypeVarSlot *target,
					TypeVarSlot **datas, int32_t n_datas)
{
	mlNaiveBayesData *nb = pc;
	MemoryContext mctx_old;
	TypeVarSlot best_probabilty[2];	/* 0: Class, 1: Probability */
	mlNaiveBayesScanData sd = { };
	int32_t scan_res;


	/*
	 * Let's make sure that we've done some training.  Otherwise there's no
	 * summary level data for us to calculate against.
	 */

	if (!nb->nest)
	{
		elog(ERROR1,
				emsg("No training data has been supplied to the Naive Bayes "
					 "implementation at [%p].  Unable to predict a value",
					 nb));

		return -1;
	}

	mctx_old = vh_mctx_switch(nb->mctx);
	/*
	 * Scan the BTree to calculate the probability for each class, but we don't
	 * care about storing each individual class' probability, just the best
	 * one.
	 */ 

	sd.nb = nb;
	sd.inputs = datas;
	sd.best_probabilty = &best_probabilty[0];
	sd.first_probability = true;

	/*
	 * Invoke the scan on nest level.
	 */
	scan_res = vh_nl_scan_all(nb->nl, ml_nb_scanner, &sd, true);

	if (scan_res)
	{
		/*
		 * Error during the scan
		 */
		vh_tvs_finalize(&best_probabilty[0]);
		vh_tvs_finalize(&best_probabilty[1]);

		vh_mctx_switch(mctx_old);

		elog(ERROR1,
				emsg("Unable to scan summary structure for Naive Bayes implementation "
					 "at [%p].",
					 nb));

		return -2;
	}

	/*
	 * After the scan is complete, we can take the best probabilty and store it
	 * in the result TypeVarSlot.
	 */

	vh_tvs_copy(target, &best_probabilty[0]);
	vh_tvs_finalize(&best_probabilty[1]);

	vh_mctx_switch(mctx_old);

	return 0;
}



/*
 * ============================================================================
 * Static Functions
 * ============================================================================
 */
static int32_t
ml_nb_summary_create(mlNaiveBayesData *nb, HeapTuplePtr htp)
{
	Nest nest;
	NestLevel nl;
	int32_t i;

	nest = vh_nest_create();
	nl = vh_nl_create();

	/*
	 * Group by the class search path
	 */
	vh_nl_groupby_create(nl, "classifier", nb->class);

	/*
	 * We only need one count, but for the remaining input fields we'll need to
	 * calculate their average and standard deviation.
	 */
	vh_nl_agg_create(nl, 0, nb->mld.paths[0], vh_acm_count_tys);
	
	for (i = 0; i < nb->mld.n_paths; i++)
	{
		vh_nl_agg_create(nl, 0, nb->mld.paths[i], vh_acm_avg_tys);
		vh_nl_agg_create(nl, 0, nb->mld.paths[i], vh_acm_devp_tys);
	}

	vh_nest_level_add(nest, nl);

	nb->nest = nest;
	nb->nl = nl;

	return 0;
}

/*
 * ml_nb_scanner
 *
 * @vals
 * 		0:	Count
 *		1:	Mean
 *		2:	Standard deviation
 *		3: 	Probability calculation
 *		4:	Probability accumulation
 */

#define VH_NBSVAL_COUNT		0
#define VH_NBSVAL_MEAN		1
#define VH_NBSVAL_STDEV		2
#define VH_NBSVAL_PROBC		3
#define VH_NBSVAL_PROBA		4

static bool 
ml_nb_scanner(NestIdxAccess nia, void *user)
{
	mlNaiveBayesScanData *sd = user;
	TypeVarSlot vals[5];
	TypeVarSlot *ptrs[2];
	TypeVarAcm tvacm;
	TypeVarAcmState tvacms;
	int32_t i, j, acms_ret, prob_ret;
	int64_t count;
	double port;

	ptrs[0] = &vals[VH_NBSVAL_MEAN];
	ptrs[1] = &vals[VH_NBSVAL_STDEV];

	/*
	 * To calculate the probability:
	 * 	1)	Current class' count divided by the total count
	 * 	2)	For each input column, mulitply the probability by the calculation 
	 * 		for the probability of that column
	 * 	3)	If the new probability is better than the existing probability in
	 * 		SD, store it in SD.
	 *
	 * When we're done, SD will contain the best fit class.
	 */

	for (i = 0; i < 5; i++)
		vh_tvs_init(&vals[i]);

	
	tvacms = vh_nestidxv_value(nia->data, 0, 0); 
	acms_ret = vh_acms_result(sd->nb->nl->agg_cols[0].acm, tvacms, &vals[0]);
	
	for (i = 0; i < sd->nb->mld.n_paths; i++)
	{
		if (i == 0)
		{
			tvacm = sd->nb->nl->agg_cols[i].acm;
			tvacms = vh_nestidxv_value(nia->data, 0, 0);
			vh_acms_result(tvacm, tvacms, &vals[VH_NBSVAL_COUNT]);

			if (!vh_tvs_i64(&vals[VH_NBSVAL_COUNT], &count))
			{
			}

			port = count / ((double)sd->nb->total_rows);
			vh_tvs_store_double(&vals[VH_NBSVAL_PROBA], port);
		}

		/*
		 * We use @j to offset where we are in the array.  Since the first entry
		 * contains a single count and then for each field we store a mean and
		 * standard deviation, we use @j to do that offset.
		 */
		j = (i * 2) + 1;

		/*
		 * Store the mean
		 */
		tvacm = sd->nb->nl->agg_cols[j].acm;
		tvacms = vh_nestidxv_value(nia->data, 0, j);
		acms_ret = vh_acms_result(tvacm, tvacms, &vals[VH_NBSVAL_MEAN]);

		if (acms_ret)
		{
		}

		/*
		 * Store the standard deviation
		 */
		tvacm = sd->nb->nl->agg_cols[j + 1].acm;
		tvacms = vh_nestidxv_value(nia->data, 0, j + 1);
		acms_ret = vh_acms_result(tvacm, tvacms, &vals[VH_NBSVAL_STDEV]);

		if (acms_ret)
		{
		}

		/*
		 * Calculate the probability
		 */
		prob_ret = sd->nb->prob(sd->prob_cache,
								&vals[VH_NBSVAL_PROBC],
								sd->inputs[i],
								ptrs,
								2);

		if (prob_ret)
		{
			elog(WARNING,
					emsg("Unable to calculate probability for Naive Bayes.  Exiting "
						 "scan."));

			return false;
		}

		/*
		 * Accumulate the probability that was just calculated
		 */
		vh_typevar_op("*=",
					  VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
					  				  VH_OP_DT_TVS,
									  VH_OP_ID_INVALID,
									  VH_OP_DT_TVS,
									  VH_OP_ID_INVALID),
					  &vals[VH_NBSVAL_PROBA],
					  &vals[VH_NBSVAL_PROBC]);
	}

	if (sd->first_probability ||
		vh_typevar_comp(">",
						VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
										VH_OP_DT_TVS,
										VH_OP_ID_INVALID,
										VH_OP_DT_TVS,
										VH_OP_ID_INVALID),
						&vals[VH_NBSVAL_PROBA],
						&sd->best_probabilty[1]))
	{
		/*
		 * The probability we just calculated is now the best probability,
		 * so let's store it and the class in @sd.  We'll also hit this
		 * block if we're the first instance in a scan, so we can setup
		 * the inital best probability.  Clear the first probability flag.
		 */

		vh_tvs_copy(&sd->best_probabilty[0], &nia->keys[0]);
		vh_tvs_copy(&sd->best_probabilty[1], &vals[VH_NBSVAL_PROBA]);

		sd->first_probability = false;
	}

	/*
	 * Clean up our TypeVarSlot array up thru the instance slot number.
	 */

	vh_tvs_finalize(&vals[VH_NBSVAL_MEAN]);
	vh_tvs_finalize(&vals[VH_NBSVAL_STDEV]);
	vh_tvs_finalize(&vals[VH_NBSVAL_PROBC]);

	return true;
}

