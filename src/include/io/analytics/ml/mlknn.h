/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_ANALYTICS_ML_KNN_H
#define VH_ANALYTICS_ML_KNN_H

#include "io/analytics/ml/ml.h"
#include "io/analytics/ml/mlfdistance.h"

/*
 * Implements K-Nearest Neighbors Machine Learning Algorithm
 *
 * 	@paths
 * 	Input data fields from the training data
 *
 * 	@n_paths
 * 	Number of paths
 *
 * 	@class
 * 	The classifier field from the training data
 *
 * 	@distance
 * 	The distance function to use (mlfdistance.h)
 *
 * 	@mode
 * 	The mode (classification or regression)
 */

typedef enum
{
	KNN_Classification,
	KNN_Regression
} ML_KNN_Mode;

void* vh_ml_knn_create(SearchPath *paths, int32_t n_paths,
					   SearchPath class,
					   vh_mlf_distance_func distance,
					   ML_KNN_Mode mode);

#endif

