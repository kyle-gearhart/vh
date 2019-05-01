/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_ANALYTICS_ML_NAIVE_BAYES_H
#define VH_ANALYTICS_ML_NAIVE_BAYES_H

#include "io/analytics/ml/ml.h"
#include "io/analytics/ml/mlfprobability.h"

void* vh_ml_naive_bayes_create(SearchPath *paths, int32_t n_paths,
							   SearchPath class,
							   vh_mlf_probability_func prob);

#endif

