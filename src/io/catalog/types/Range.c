/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/types/Range.h"

static void type_range_tam_memset_set(struct TamGenStack *tamstack,
									  void *source, void *target);

static int32_t type_range_tom_comp(struct TomCompStack *tomstack,
								   const void *lhs, const void *rhs);

const struct TypeData vh_type_Range = {
	.id = 111,
	.name = "Range",
	.size = sizeof(uint8_t),
	.alignment = sizeof(uint8_t),
	.varlen = false,
	.construct_forhtd = false,
	.allow_inner = true,
	.include_inner_for_size = true,
	.inner_for_size_multiplier = 2,
	.tam = {
		.memset_set = type_range_tam_memset_set
	},	
	.tom = {
		.comp = type_range_tom_comp
	}
};

/*
 * Both |range| and |to| are the Ranges with |ty_inner| as elements.
 */
bool
vh_ty_range_contains(Type ty_inner, const void *range, const void *to)
{
	return false;
}

size_t
vh_ty_range_boundary_member_sz(Type *types)
{
	Type ty;
	size_t sz = 0;
	uint32_t i = 0;

	ty = types[0];

	while (ty)
	{
		sz += ty->size;
		i++;

		ty = types[i];
	}

	return sz;
}

static void 
type_range_tam_memset_set(struct TamGenStack *tamstack,
						  void *source, void *target)
{
	char *s_lower = source, *t_lower = target;
	char *s_upper, *t_upper;
	size_t membersz;

	assert(tamstack->types[0]);
	assert(tamstack->funcs[0]);

	membersz = vh_ty_range_boundary_member_sz(&tamstack->types[0]);

	s_upper = s_lower + membersz;
	t_upper = t_lower + membersz;

	vh_tam_firen_memset_set(tamstack, s_lower, t_lower);
	vh_tam_firen_memset_set(tamstack, s_upper, t_upper);
}

/*
 * This needs to be a lot smarter about setting the offests.
 */
static int32_t
type_range_tom_comp(struct TomCompStack *tomstack,
					const void *lhs, const void *rhs)
{
	const char *l_lower = lhs, *r_lower = rhs;
	const char *l_upper, *r_upper;
	int32_t comp;
	size_t membersz;

	assert(tomstack->types[0]);
	assert(tomstack->funcs[0]);

	membersz = vh_ty_range_boundary_member_sz(&tomstack->types[0]);

	l_upper = l_lower + membersz;
	r_upper = r_lower + membersz;

	comp = vh_tom_firen_comp(tomstack, l_lower, r_lower);
	
	if (!comp)
	{
		comp = vh_tom_firen_comp(tomstack, l_upper, r_upper);
	}

	return comp;
}

