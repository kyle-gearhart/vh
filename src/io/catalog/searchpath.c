/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>

#include "vh.h"
#include "io/catalog/searchpath.h"


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

void*
vh_sp_create(SearchPathFuncTable funcs, SPRET spret, size_t sz)
{
	SearchPath sp;

	if (!funcs)
	{
		elog(ERROR2,
				emsg("Required SearchPathFuncTable has an invalid pointer [%p].  Unable "
					 "to generate a new SearchPath.",
					 funcs));

		return 0;
	}

	if (!funcs->search ||
		!funcs->next)
	{
		elog(ERROR2,
				emsg("The SearchPath function table at [%p] does not have a @search and/or "
					 "@next function implemented.  Unable to generate a new SearchPath.",
					 funcs));

		return 0;
	}

	if (sz < sizeof(struct SearchPathData))
	{
		elog(ERROR2,
				emsg("Requested SearchPath size [%llu] is too small.  The minimum "
					 "size for a new SearchPath is [%llu] bytes.",
					 sz,
					 sizeof(struct SearchPathData)));

		return 0;
	}

	sp = vhmalloc(sz);
	memset(sp, 0, sz);

	sp->funcs = funcs;
	sp->spret = spret;
	sp->verbosity = 0;

	return sp;
}

void
vh_sp_finalize(SearchPath sp)
{
	if (sp && sp->funcs->finalize)
	{
		sp->funcs->finalize(sp);
	}
}

void
vh_sp_destroy(SearchPath sp)
{
	if (sp)
	{
		if (sp->funcs->finalize)
		{
			sp->funcs->finalize(sp);
		}

		vhfree(sp);
	}
}


/*
 * vh_sp_pull_unk_arg(va_list args)
 *
 * This one exists so that we can keep moving along the arugment list at the
 * right offset.  We don't expect every search path to require each of the
 * available runtime arguments.  To keep things running smoothly, we implement
 * this stub to pull an "unknown" arugment off the list.  Unknown simply means
 * the SearchPath implementation doesn't care about it if it's there.  This
 * allows for our caller to shove everything it knows about it's context down
 * each SearchPath's throat.
 */

void
vh_sp_pull_unk_arg(int32_t argt, va_list args)
{
	switch (argt)
	{
	}
}

