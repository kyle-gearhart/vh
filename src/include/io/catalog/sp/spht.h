/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_sp_spht_h
#define vh_catalog_sp_spht_h

#include "io/catalog/searchpath.h"

/*
 * SearchPath for HeapTuple
 *
 * Extracts a TableField, TypeVarSlot, or raw data pointer from a HeapTuple,
 * HeapTuplePtr.
 *
 * Supported return types:
 * 		SPRET_DataAt
 * 		SPRET_TableField
 */

/*
 * vh_spht_tf_create
 *
 * Returns a TableField from a HeapTuple, TableDef, or TableDefVer input.  The 
 * field name may be supplied at runtime or when the SearchPath is created.
 */
SearchPath vh_spht_tf_create(const char *fname);

/*
 * vh_spht_dat_create
 *
 * Returns a raw pointer to the data element from a HeapTuple represented by
 * the field name passed at runtime or creation.
 */
SearchPath vh_spht_dat_create(const char *fname);

#endif

