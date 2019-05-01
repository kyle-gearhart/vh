/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/prepcol/prepcol.h"


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

/*
 * vh_pc_create
 *
 * Make sure we've got a valid PrepColFuncTable and the minimum functions have
 * been implemented.
 */
void*
vh_pc_create(PrepColFuncTable funcs, size_t sz)
{
	PrepCol pc;

	assert(sz >= sizeof(struct PrepColData));

	if (!funcs)
	{
		elog(WARNING,
				emsg("Invalid PrepColFuncTable passed to vh_pc_create.  A "
					 "PrepCol cannot be created with an invalid function table."));

		return 0;
	}

	if (!funcs->populate_slot)
	{
		elog(WARNING,
				emsg("Incomplete PrepCol function table implementation [%p], "
					 "populate_slot is not present.  Unable to create a PrepCol "
					 "without a valid populate_slot function.",
					 funcs));

		return 0;
	}

	pc = vhmalloc(sz);
	memset(pc, 0, sz);
	pc->funcs = funcs;

	return pc;
}

