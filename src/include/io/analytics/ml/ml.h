/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_ANALYTICS_ML_ML_H
#define VH_ANALYTICS_ML_ML_H

#include "io/catalog/searchpath.h"
#include "io/catalog/prepcol/prepcol.h"

/*
 * Machine Learning
 *
 * We use the PrepCol interface to execute a machine learning algorithm.  Prior
 * to calling populate_slot on the PrepCol, the model should be trainined.
 *
 * Generally, we're going to pass a HeapTuplePtr in for training.  Some models 
 * can train on the fly while others are going to simply store the HTP somewhere 
 * and access the actual data later.
 */

typedef const struct MachLearnFuncTableData *MachLearnFuncTable;
typedef struct MachLearnData *MachLearn;

struct MachLearnFuncTableData
{
	struct PrepColFuncTableData pcf;
	int32_t (*train_htp)(void *ml, HeapTuplePtr htp, HeapTuple ht);
};

struct MachLearnData
{
	MachLearnFuncTable mlft;
	SearchPath *paths;
	int32_t n_paths;
};

void* vh_ml_create(MachLearnFuncTable funcs, 
				   SearchPath *paths, int32_t n_paths,
				   size_t sz);

#define vh_ml_train_htp(ml, htp, ht)	((*((MachLearnFuncTable*)ml))->train_htp((ml), 	\
																				 (htp), \
																				 (ht)))

bool vh_ml_setdatas_ht(MachLearn ml, 
					   TypeVarSlot ***slots, int32_t n_slots, 
					   HeapTuple ht);


float vh_ml_test_classification(MachLearn ml, SList training, SList test,
								SearchPath class);


#endif

