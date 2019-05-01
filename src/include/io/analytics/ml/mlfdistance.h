/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_ANALYTICS_ML_MLFDISTANCE_H
#define VH_ANALYTICS_ML_MLFDISTANCE_H

#include "vh.h"

/*
 * Machine Learning Distance Functions
 */

typedef int32_t (*vh_mlf_distance_func)(void **cache,
										TypeVarSlot *result,
										TypeVarSlot *training_set,
										TypeVarSlot **instance,
										int32_t n_datas);

/*
 * We have a series of distance functions that are available to the various
 * algorithms.  The only quirk to the them is the first parameter is an opaque
 * pointer to a cache structure.  When the caller passes in a null value, no
 * cache will be utilized.  If the pointer is not null, but the pointed to
 * value is null, we'll create the cache structure on the fly and store it.
 *
 * This way we don't have to call a creator function if we don't need to.
 *
 * To destroy the cache, pass the function pointer that was used to create
 * it followed by the pointer to the cache.  Null cache pointers may be passed. 
 */

void vh_mlf_distance_destroy(vh_mlf_distance_func distance, void *cache);


/*
 * There are three distane functions available:
 * 	1)	Euclidean
 * 	2)	Manhattan
 * 	3)	Vector dot
 */

int32_t vh_mlf_distance_euclidean(void **cache,
								  TypeVarSlot *result,
								  TypeVarSlot *training_set,
								  TypeVarSlot **instance,
								  int32_t n_datas);
int32_t vh_mlf_distance_manhattan(void **cache,
								  TypeVarSlot *result,
								  TypeVarSlot *training_set,
								  TypeVarSlot **instance, 
								  int32_t n_datas);
int32_t vh_mlf_distance_vectordot(void **cache,
								  TypeVarSlot *result,
								  TypeVarSlot *training_set,
								  TypeVarSlot **instance, 
								  int32_t n_datas);

#endif

