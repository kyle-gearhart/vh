/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"


void* 
vh_ty_int8_tam_bin_get(struct TamBinGetStack *tamstack, 
						const BinaryAMOptions bopts,
						const void *src, void *tgt,
			  			size_t *length, size_t *cursor)
{
	const int8_t *source = src;
	int8_t *target = tgt;
	int8_t **pptarget = (int8_t**)target, *buffer, swapped;
	
	if (!bopts->malloc && length && *length == 0)
	{
		*length = sizeof(int8_t);
		return 0;
	}

	swapped = *source;

	if (bopts->malloc)
	{
		buffer = vhmalloc(sizeof(int8_t));
		*buffer = swapped;

		if (cursor)
			*cursor = sizeof(int8_t);

		if (target)
			*pptarget = buffer;

		if (length)
			*length = sizeof(int8_t);

		return buffer;
	}
	else
	{
		if (target)
		{
			*target = swapped;

			if (cursor)
				*cursor = sizeof(int8_t);

			if (length)
				*length = sizeof(int8_t);
		}
		else
		{
			elog(ERROR2,
				 emsg("vh_tam_bin_get for int8 has been called without a "
					  "non-null target pointer parameter!  Please see "
					  "the rules contained in catalog/Type.h and adjust the "
					  "calling convention!"));
		}
	}

	return 0;
}

void* 
vh_ty_int8_tam_bin_set(struct TamBinSetStack *tamstack, 
						const BinaryAMOptions bopts,
						const void *src, void *tgt,
			  			size_t length, size_t cursor)
{
	const int8_t *source = src;
	int8_t *target = tgt;
	int8_t **pptarget = (int8_t**)target, *buffer, swapped;

	swapped = *source;

	if (bopts->malloc)
	{
		buffer = vhmalloc(sizeof(int8_t));
		*buffer = swapped;

		*pptarget = buffer;

		return buffer;
	}
	else
	{
		*target = swapped;
	}

	return 0;
}

size_t 
vh_ty_int8_tam_bin_len(Type type, const void *source)
{
	return sizeof(int8_t);
}

void 
vh_ty_int8_tam_memset_get(struct TamGenStack *tamstack,
						   void *src, void *tgt)
{
	int8_t *target = tgt, *source = src;

	if (source && target)
		*target = *source;
}

void 
vh_ty_int8_tam_memset_set(struct TamGenStack *tamstack,
						   void *src, void *tgt)
{
	int8_t *target = tgt, *source = src;

	if (source && target)
		*target = *source;
}


int32_t 
vh_ty_int8_tom_comparison(struct TomCompStack *tamstack,
						   const void *lhs, const void *rhs)
{
	const int8_t *l = lhs, *r = rhs;

	vh_tom_assert_bottom(tamstack);
	return (*l < *r) ? -1 : (*l > *r);
}

struct TypeData const vh_type_int8 =
{
	.id = 3,
	.name = "int8",
	.varlen = false,
	.size = sizeof(int8_t),
	.alignment = sizeof(int8_t),
	.construct_forhtd = false,

	.tam = {
		.bin_get = vh_ty_int8_tam_bin_get,
		.bin_set = vh_ty_int8_tam_bin_set,
		.bin_length = vh_ty_int8_tam_bin_len,

		.memset_get = vh_ty_int8_tam_memset_get,
		.memset_set = vh_ty_int8_tam_memset_set
	},
	.tom = {
		.comp = vh_ty_int8_tom_comparison
	}
};

