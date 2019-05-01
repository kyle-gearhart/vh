/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_ANALYTICS_MLFPROBABILITY_H
#define VH_ANALYTICS_MLFPROBABILITY_H

typedef int32_t (*vh_mlf_probability_func)(void **cache,
										  TypeVarSlot *result,
										  TypeVarSlot *instance,
										  TypeVarSlot **datas,
										  int32_t n_datas);

int32_t vh_mlf_probability_gaussian(void **cache,
									TypeVarSlot *result,
									TypeVarSlot *instance,
									TypeVarSlot **datas,
									int32_t n_datas);

#endif

