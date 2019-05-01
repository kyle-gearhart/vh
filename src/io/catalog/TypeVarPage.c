/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarPage.h"

/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */
size_t
vh_tvp_space(Type *tys)
{
	size_t var_offset = 0, space;

	space = vh_typevar_tys_size(tys, &var_offset);
	space += vh_tvp_slot_size;

	return space;
}

void
vh_tvp_initialize(TypeVarPage page, size_t sz)
{
	page->d_upper = vh_tvp_header_size;
	page->d_lower = vh_tvp_header_size + sz;
}

int32_t
vh_tvp_add(TypeVarPage page, Type *tys)
{
	size_t var_offset = 0, space, maxa, diff;
	int32_t idx;

	space = vh_typevar_tys_size(tys, &var_offset);
	maxa = vh_type_stack_data_maxalign(tys);

	if (vh_tvp_freespace(page) >= space + vh_tvp_slot_size)
	{
		idx = vh_tvp_array_size(page);
		page->d_lower -= space;

		/*
		 * If we're not the right boundary, try to fix it.
		 */
		if ((diff = page->d_lower % maxa))
		{
			diff = maxa - diff;

			if (vh_tvp_freespace(page) >= diff + vh_tvp_slot_size)
			{
				page->d_lower -= diff;
			}
			else
			{
				/*
				 * We're out of space on the page and cannot accomodate the
				 * request.
				 */
				elog(WARNING,
						emsg("Insufficient space left on page %p to acommodate %ld bytes "
							 "plus an alignment variance of %ld bytes for the TypeVar.  "
							 "Ensure the overall page size is max-aligned.",
							 page,
							 space,
							 diff));
				return -2;
			}
		}

		page->slots[idx].varhead = page->d_lower;
		page->slots[idx].data = page->slots[idx].varhead + var_offset;

		page->d_upper += vh_tvp_slot_size;

		vh_typevar_init(vh_tvp_varhead(page, idx + 1), space, tys);

		return idx + 1;
	}
	else
	{
		elog(WARNING,
				emsg("Insufficient space left on page %p to accomodate %ld bytes "
					 "for the TypeVar",
					 page,
					 space));
	}

	return -1;
}

void
vh_tvp_finalize(TypeVarPage page)
{
	int32_t count, i;

	count = vh_tvp_array_size(page);

	for (i = 1; i <= count; i++)
	{
		vh_typevar_finalize(vh_tvp_var(page, i));
	}
}

