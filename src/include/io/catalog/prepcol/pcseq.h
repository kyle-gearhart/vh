/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */

#ifndef vh_catalog_prepcol_pcseq_h
#define vh_catalog_prepcol_pcseq_h

#include "io/catalog/prepcol/prepcol.h"

/*
 * Sequence Number PrepCol
 *
 * The base is expected to the same type as the target field.  No type conversions
 * will be performed when populate_slot is called.
 *
 * If no base is specified, then sequence will start at 1.  When a base is specified,
 * the @base value will be the first value assigned when populate_slot is called.
 *
 * Always increment by one (typically thru the ++ operator).
 */

PrepCol vh_pcseq_create(TypeVar base);


#endif

