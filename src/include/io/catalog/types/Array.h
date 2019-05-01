/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_types_Array_H
#define vh_datacatalog_types_Array_H

#include "io/catalog/types/varlen.h"

/*
 * For array types, we explicitly call out the inner type of the elements the
 * array contains.  It's possible for users to nest arrays within themselves.
 *
 * It's best to use emplace, as we push arrays as themselves rather than 
 * pointers when we're nested more than one deep.  This smooths out our 
 * constructor to be compliant with the TOM rules.
 *
 * The MemSet family of TAMs have been taught how to copy values and do copy
 * construct when the inner type warrants it.
 */

typedef struct ArrayData
{
	struct vhvarlenm vl;
	Type ty_inner;

	void *buffer;
	uint8_t ndimensions;
	uint8_t nelemavail;
} *Array;

size_t vh_ty_array_nelems(const struct ArrayData* array);
size_t vh_ty_array_elems_full_width(const struct ArrayData* array);
void* vh_ty_array_elemat(const struct ArrayData *array, uint32_t idx);

void* vh_ty_array_emplace(Array array);

#define vh_GetArrayNm(htp, fname)												\
	((Array)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname))

#endif

