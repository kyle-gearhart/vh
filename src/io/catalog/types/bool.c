/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/types/int.h"

/*
 * We split out boolean into a specific TypeData structure so that back ends
 * may distinguish between a char and boolean if required.
 */

static char*
type_bool_cstr_get(struct TamCStrGetStack *tamstack, CStrAMOptions copts,
				   const void *source, char *target,
				   size_t *length, size_t *cursor,
				   void *formatter);

struct TypeData const vh_type_bool =
{
	.id = 35,
	.name = "bool",
	.varlen = false,
	.size = sizeof(int8_t),
	.alignment = sizeof(int8_t),
	.construct_forhtd = false,

	.tam = {
		.bin_get = vh_ty_int8_tam_bin_get,
		.bin_set = vh_ty_int8_tam_bin_set,
		.bin_length = vh_ty_int8_tam_bin_len,

		.cstr_get = type_bool_cstr_get,

		.memset_get = vh_ty_int8_tam_memset_get,
		.memset_set = vh_ty_int8_tam_memset_set
	},
	.tom = {
		.comp = vh_ty_int8_tom_comparison
	}
};


static char*
type_bool_cstr_get(struct TamCStrGetStack *tamstack, CStrAMOptions copts,
				   const void *source, char *target,
				   size_t *length, size_t *cursor,
				   void *formatter)
{
	static const char* values[] = { "false", "true" };
	char *t;
	const int8_t *s = source;
	int8_t slot, slotlen;

	slot = !(*s == 0);
	slotlen = strlen(values[slot]);

	if (copts->malloc)
	{
		t = vhmalloc(strlen(values[slot]) + 1);
		strncpy(t, values[slot], slotlen);

		if (*length)
			*length = slotlen;

		if (*cursor)
			*cursor = slotlen;

		return t;
	}
	else if (length)
	{
		if (*length > slotlen)
		{
			memcpy(target, values[slot], slotlen + 1);

			if (cursor)
				*cursor = slotlen;
		}
		else
		{
			if (cursor)
				*cursor = 0;
		}

		*length = slotlen;
	}

	return 0;
}

