/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/analytics/ml/ml.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/utils/SList.h"



void*
vh_ml_create(MachLearnFuncTable funcs,
			 SearchPath *paths, int32_t n_paths,
			 size_t sz)
{
	MachLearn ml;
	size_t alloc_sz;
	int32_t i;

	assert(sz >= sizeof(struct MachLearnData));

	alloc_sz = sz + (sizeof(SearchPath) * n_paths);
	ml = vhmalloc(alloc_sz);
	memset(ml, 0, sz);

	ml->mlft = funcs;
	ml->paths = (SearchPath*)(((char*)ml) + sz);
	ml->n_paths = n_paths;

	for (i = 0; i < n_paths; i++)
		ml->paths[i] = paths[i];

	return ml;
}

/*
 * vh_ml_setdatas_ht
 *
 * Extracts the variables into a TypeVarSlot ** array for processing by the PrepCol
 * engine.  We expect either a null @slots or @n_slots equal to zero to spin up a
 * new allocation of sufficient size to accomodate an array of pointers to the
 * TypeVarSlot.
 */

bool
vh_ml_setdatas_ht(MachLearn ml, 
				  TypeVarSlot ***slots, int32_t n_slots, 
				  HeapTuple ht)
{
	TypeVarSlot **slotptrs;
	TypeVarSlot *slotsa;
	HeapField hf;
	size_t alloc_sz;
	int32_t i, sp_res;

	if (!n_slots ||
		(*slots) == 0)
	{
		alloc_sz = (sizeof(TypeVarSlot**) + sizeof(TypeVarSlot)) *
					ml->n_paths;
		slotptrs = vhmalloc(alloc_sz);
		slotsa = (TypeVarSlot*)(slotptrs + ml->n_paths);

		for (i = 0; i < ml->n_paths; i++)
			slotptrs[i] = &slotsa[i];

		*slots = slotptrs;
	}
	else if (n_slots < ml->n_paths)
	{
		elog(ERROR2,
				emsg("n_slots at vh_ml_setdatas_ht only has capacity for %d "
					 "expected at least %d",
					 n_slots,
					 ml->n_paths));
		return false;
	}
	else
	{
		slotptrs = *slots;
	}

	for (i = 0; i < ml->n_paths; i++)
	{
		hf = vh_sp_search(ml->paths[i], &sp_res, 1, VH_SP_CTX_HT, ht);

		if (sp_res < 0)
		{
		}

		vh_tvs_store_ht_hf(slotptrs[i], ht, hf);
	}

	return true;
}


/*
 * vh_ml_test_classification
 *
 * Trains a machine learning algorithm and then iterates the test set to compare
 * the predicated value with the actual value.
 */
float
vh_ml_test_classification(MachLearn ml, SList training, SList test, 
						  SearchPath class)
{
	HeapTuplePtr *htp_head, htp;
	HeapTuple ht;
	HeapField hf;
	PrepCol pc;
	TypeVarSlot actual_class = { }, predicted_class = { }, **test_datas = 0;
	int32_t htp_sz, i, train_res, sp_res, ps_res, hits = 0;

	pc = (PrepCol)ml;
	htp_sz = vh_SListIterator(training, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		train_res = vh_ml_train_htp(ml, htp_head[i], 0);

		if (train_res)
		{
			/*
			 * Error training, we should propagate this up and keep on moving
			 * forward.
			 */
			elog(WARNING, emsg("Error [%d]: training algorithm at index %d",
							   train_res, i));
			continue;
		}
	}

	htp_sz = vh_SListIterator(test, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];
		ht = vh_htp(htp);

		/*
		 * Get the classification on the test set.
		 */
		hf = vh_sp_search(class, &sp_res, 1, VH_SP_CTX_HT, ht);
		vh_tvs_store_ht_hf(&actual_class, ht, hf);

		/*
		 * Get the columns used to detect the classification and store them
		 * in the array represented by test_datas.  We use vh_ml_set_datas
		 * to create a new array and underlying TypeVarSlot.
		 */
	  	vh_ml_setdatas_ht(ml, &test_datas, (test_datas == 0 ? 0 : ml->n_paths), ht);

		/*
		 * Call the prediction algorithm!
		 */

		ps_res = vh_pc_populate_slot(pc, &predicted_class, test_datas, ml->n_paths);

		if (ps_res)
		{
			/*
			 * We had a problem predicting a value.
			 */
			elog(WARNING,
					emsg("Error [%d]: Unable to predict a value for the test "
						 "set instance at index %d", ps_res, i));
			continue;
		}

		if (vh_tvs_compare(&predicted_class, &actual_class))
		{
			/*
			 * We do not have a match between the predicted class and the actual
			 * class.  We should probably log some more details here for
			 * diagnostic purposes, but for now don't do anything.
			 */
		}
		else
		{
			/*
			 * Predicted = Actual
			 */
			hits++;
		}
	}

	if (test_datas)
		vhfree(test_datas);

	return hits / ((float)htp_sz);
}

