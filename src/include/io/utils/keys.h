/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef vh_utils_key_H
#define vh_utils_key_H

#include "io/utils/htbl.h"

/*
 * Generic Key Implementation to wrap a HashMap.  We have the following
 * patterns implemented:
 *		<> KeySet		Unique keys
 *		<> KeyList		Unique keys with an SList value_comp
 *		<> KeyValueMap	Unique keys with a unique value
 *
 * We use a few macros to set everything up.  The main driver is to accomodate
 * the HeapTuplePtr on a 32-bit system.
 */


typedef struct KeyIteratorData KeyIterator;

struct KeyIteratorData
{
	HashTable htbl;
	int32_t idx;
};


/*
 * ============================================================================
 * Shared Iterators
 * ============================================================================
 */

#define vh_key_it_init(it, hash)	((it)->htbl = (hash), (it)->idx = -1)
#define vh_key_it_first(it, k, v)	(((it)->idx = vh_htbl_iter((it)->htbl, 0, (k), (v))) >= 0)
#define vh_key_it_next(it, k, v)	(((it)->idx = vh_htbl_iter((it)->htbl, ++(it)->idx, (k), (v))) >= 0)
#define vh_key_it_last(it, k, v)	(((it)->idx = vh_htbl_iter_last((it)->htbl, 0, (k), (v))) >= 0)

#define vh_key_slist(hash)			vh_htbl_to_slist((hash))

#endif

