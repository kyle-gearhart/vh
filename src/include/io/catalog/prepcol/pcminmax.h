/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_prepcol_pcminmax_h
#define vh_catalog_prepcol_pcminmax_h

#include "io/catalog/prepcol/prepcol.h"

/*
 * PrepCol MinMax
 *
 * Used for normalizing values, particularly useful for machine learning
 * algorithms so that certain attributes don't over power the distance
 * functoins being used.  We expected to be provided with a minimum and maximum
 * value to calculate against.
 */

PrepCol vh_pcminmax_create(TypeVarSlot* min, TypeVarSlot* max);

#endif

