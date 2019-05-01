/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_prepcol_defaultv_H
#define vh_catalog_prepcol_defaultv_H

#include "io/catalog/prepcol/prepcol.h"

/*
 * PrepCol Default Value
 *
 * Defaults a static value into a column.  A single condition check, is_null
 * may be used to prevent defaulting the value.
 */

PrepCol vh_pc_defaultv_create(void *typevar, bool is_null);

#endif

