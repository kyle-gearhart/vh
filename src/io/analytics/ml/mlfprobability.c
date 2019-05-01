/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <math.h>

#include "vh.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarPage.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/analytics/ml/mlfprobability.h"

/*
 * ============================================================================
 * Gaussian
 * ============================================================================
 *
 * We do all Gaussian Probability on the fly by just using doubles.
 */


/*
 * ============================================================================
 * Gaussian Probability Implementation
 * ============================================================================
 */

/*
 * vh_mlf_probability_gaussian
 *
 * To make this a lot easier, we're just going to convert the niput over to
 * a double, along with the mean and standard deviation.
 */

int32_t
vh_mlf_probability_gaussian(void **cache,
							TypeVarSlot *result,
							TypeVarSlot *instance,
							TypeVarSlot **datas,
							int32_t n_datas)
{
	double exponent, mean, deviation, x, prob;

	if (!vh_tvs_double(instance, &x))
		return -1;

	if (!vh_tvs_double(datas[0], &mean))
		return -2;

	if (!vh_tvs_double(datas[1], &deviation))
		return -3;

	exponent = exp(-(pow((x-mean), 2) / (2 * pow(deviation, 2))));
	prob = (1 / (sqrt(2 * M_PI) * deviation)) * exponent;
	vh_tvs_store_double(result, prob);

	return 0;
}

