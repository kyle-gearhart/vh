/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/analytics/nestidx.h"
#include "io/analytics/nestlevel.h"

/*
 * ============================================================================
 * Static Helper Functions
 * ============================================================================
 */

static int32_t nestidx_fill_access_keys(NestIdx idx, 
										GroupByCol *cols, TypeVarSlot **keys, int32_t n_vals,
										TypeVarSlot **akeys, bool *nulls);

typedef const struct NestIdxFuncTable *IdxFuncs;
static IdxFuncs nestidx_get_funcs(NestIdx idx);


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */



/*
 * vh_nestidx_access_create
 *
 * The NestIdxAccess structure is used to "return" data from a vh_nestidx_access
 * call.  We also use it for scans.  It stores the keys, a pointer to the leaf
 * along with an opaque pointer to additional information depending on the
 * underlying index type.
 */
NestIdxAccess
vh_nestidx_access_create(NestIdx idx)
{
	NestIdxAccess nia;
	size_t sz;
	int32_t n_cols;

	n_cols = idx->n_cols;

	sz = sizeof(struct NestIdxAccessData);
	sz += sizeof(struct TypeVarSlot) * n_cols;

	nia = vhmalloc(sz);
	memset(nia, 0, sz);

	return nia;	
}

/*
 * vh_nestidx_access_reset
 *
 * Releases the pinned page of a given index.  We use the opaque pointer to do
 * this.  It depends on the underlying index type.
 */
int32_t
vh_nestidx_access_reset(NestIdx idx, NestIdxAccess nia)
{
	int32_t i;

	for (i = 0; i < idx->n_cols; i++)
	{
		vh_tvs_reset(&nia->keys[i]);
	}

	nia->exists = false;

	return 0;
}

/*
 * vh_nestidx_access_destroys
 *
 * Resets a NestIdxAccess and free's it.
 */
int32_t
vh_nestidx_access_destroy(NestIdx idx, NestIdxAccess nia)
{
	int32_t ret;

	ret = vh_nestidx_access_reset(idx, nia);
	vhfree(nia);

	return ret;
}

/*
 * vh_nestidx_access
 *
 * We only support FETCH, INSERT, and DELETE access methods.
 *
 * 	@idx	The index being accessed
 * 	@cols	The columns representing the @keys
 * 	@keys	The keys to be accessed, inserted or deleted
 * 	@n_vals	Size of @cols and @keys arrays, each
 * 	@am		The access method (i.e. FETCH, INSERT, DELETE)
 * 	@nia	Store the result
 *
 * By using the columns functionality, the caller can build out the
 * keys the way the want and let the index routines get the keys exactly
 * right for the underlying index type.
 *
 * It's possible for the caller to use the TypeVarSlot of the @nia to
 * pass the keys in.  It should be noted that if the access method is
 * succesful, the underlying routine will overwrite the keys.
 */

int32_t
vh_nestidx_access(NestIdx idx,
				  GroupByCol *cols, TypeVarSlot **keys, int32_t n_vals,
				  int32_t am,
				  NestIdxAccess nia)
{
	IdxFuncs funcs;
	TypeVarSlot **amkeys;
	bool *null_amkeys;
	int32_t i, fill_amkeys_res, access_res;

	if (am & VH_NESTIDX_AM_SCAN)
	{
		elog(ERROR1,
				emsg("vh_nestidx_access does not support an index scan method.  "
					 "vh_nestidx_scan should be used instead."));

		return -1;
	}

	if (!nia)
	{
		elog(ERROR1,
				emsg("Invalid NestIdxAccess pointer [%p] passed to vh_nestidx_access!  "
					 "Unable to proceed with the access.",
					 nia));

		return -2;
	}

	/*
	 * Get our function table.
	 */

	funcs = nestidx_get_funcs(idx);

	if (!funcs)
	{
		elog(ERROR1,
				emsg("Unable to resolve the function table to access the index type %d.  "
					 "Unable to proceed with the access.",
					 idx->method));

		return -3;
	}

	if (!funcs->access)
	{
		elog(ERROR1,
				emsg("No index access function has been defined for the index type %d.  "
					 "Unable to proceed with the access.",
					 idx->method));
		return -4;
	}

	/*
	 * Setup the @nia with a few variables we know will not be changing.
	 */

	nia->am = am;

	/*
	 * Organize the keys.  We want this in column order.  When the column is not
	 * specified, then we've got to make a null TypeVar in it's place to access
	 * the index with.
	 */

	amkeys = vhmalloc((sizeof(TypeVarSlot*) * idx->n_cols) +
					  (sizeof(bool) * idx->n_cols));
	null_amkeys = (bool*)(amkeys + idx->n_cols);
	
	fill_amkeys_res = nestidx_fill_access_keys(idx, 
											   cols, 
											   keys, 
											   n_vals,
											   amkeys,
											   null_amkeys);

	if (fill_amkeys_res < 0)
	{
		/*
		 * Abort, we've got an error.
		 */
	}

	/*
	 * Call the index access method with our amkeys.
	 */
	access_res = funcs->access(idx, amkeys, am, nia);

	if (access_res)
	{
	}

	/*
	 * Cleanup our mess from the keys.  If we got hit with some nulls we'll want
	 * to release those TypeVarSlots we allocated.
	 */

	for (i = 0; i < fill_amkeys_res ? idx->n_cols : 0; i++)
	{
		if (null_amkeys[i])
		{
			vh_tvs_reset(amkeys[i]);
			vhfree(amkeys[i]);

			amkeys[i] = 0;
		}
	}

	vhfree(amkeys);

	return 0;								
}

static IdxFuncs 
nestidx_get_funcs(NestIdx idx)
{
	switch (idx->method)
	{
		case VH_NESTIDX_METH_BTREE:
			return &vh_nestidx_func_btree;
	}

	return 0;
}

/*
 * ============================================================================
 * Static Access Helper Functions
 * ============================================================================
 */

/*
 * nestidx_fill_access_keys
 *
 * The caller may not always provide vh_nestidx_access with all of the keys
 * necessary to access the index.  This is due to multiple NestLevels with
 * similar group by criteria sharing the same index.
 *
 * We return 0 if we have a perfect match, 1 if we had to make nulls to fill
 * the index and a negative number if there was an error.
 */
static int32_t 
nestidx_fill_access_keys(NestIdx idx, 
						 GroupByCol *cols, TypeVarSlot **keys, int32_t n_vals,
					 	 TypeVarSlot **akeys,
						 bool *nulls)
{
	GroupByCol gbc_outter, gbc_inner;
	int32_t i, j, ret = 0;
	bool match;

	for (i = 0; i < idx->n_cols; i++)
	{
		gbc_outter = idx->cols[i];

		match = false;

		for (j = 0; j < n_vals; j++)
		{
			gbc_inner = cols[j];

			if (gbc_inner->idx_slot == gbc_outter->idx_slot)
			{
				match = true;
				break;
			}
		}

		if (match)
		{
			akeys[i] = keys[j];
			nulls[i] = false;
		}
		else
		{
			akeys[i] = vhmalloc(sizeof(TypeVarSlot));
			nulls[i] = true;

			vh_tvs_init(akeys[i]);
			vh_tvs_store_null(akeys[i]);

			ret = 1;
		}
	}

	return ret;
}



/*
 * ============================================================================
 * Public Interface: Scans
 * ============================================================================
 */

/*
 * vh_nestidx_scan_all
 *
 * Scans all entries in a NestIdx from start to finish.
 *
 * This is our most basic scan and should be rarely called.
 */
int32_t
vh_nestidx_scan_all(NestIdx idx,
					vh_nestidx_scan_cb cb, void *user,
					bool forward)
{
	IdxFuncs funcs;
	NestIdxAccess nia;
	bool run_more;

	if (!idx)
	{
		elog(WARNING, 
				emsg("Invalid NestIdx pointer [%p] passed to vh_nestidx_scan_all.  "
					 "Unable to execute the scan.",
					 idx));

		return -1;
	}

	if (!cb)
	{
		elog(WARNING,
				emsg("Invalid vh_nestidx_scan_cb callback function passed to "
					 "vh_nestidx_scan_all against the index [%p].  Scan will "
					 "not be executed.",
					 idx));

		return -2;
	}

	nia = vh_nestidx_access_create(idx);
	funcs = nestidx_get_funcs(idx);

	/*
	 * Begin the scan with no keys
	 */
	if (funcs->scan_begin(idx, nia, 0))
	{
		elog(WARNING,
				emsg("Index access error when attempting to scan the entire index "
					 "at [%p].",
					 idx));

		return -2;
	}

	if ((funcs->scan_first(idx, nia, 0, 0, forward)))
	{
		run_more = cb(nia, user);
	}

	while (run_more && funcs->scan_next(idx, nia, forward))
	{
		run_more = cb(nia, user);
	}

	return 0;
}

int32_t
vh_nl_scan_all(NestLevel nl, vh_nestidx_scan_cb cb, void *user, bool fwd)
{
	Nest nest;
	NestIdx nidx;
	int32_t res;

	if (nl && nl->nest)
	{
		/*
		 * We really need to set the appropriate quals to access the index
		 * with.
		 */
		nest = nl->nest;
		nidx = &nest->idxs[nl->idx];

		if (nl->idx >= 0 && nl->idx < nl->nest->n_idxs)
		{
			res = vh_nestidx_scan_all(nidx, cb, user, fwd);
		}
		else
		{
			elog(ERROR1,
					emsg("Invalid Nest index indicated by the NestLevel [%p] "
						 "unable to proceed with the full scan of the NestLevel.",
						 nl));
			res = -2;
		}

		return res;
	}

		elog(ERROR1,
				emsg("NestLevel at [%p] has not been added to a Nest.",
					 nl));

		return -1;
}

/*
 * ============================================================================
 * Public Interface: Values
 * ============================================================================
 */

/*
 * vh_nestidxv_initialize
 *
 * When a leaf is first added, we want to call the initialize function to setup
 * the base values for our mini-page we put in place.
 */
NestIdxValue
vh_nestidxv_initialize(void *data, size_t sz)
{
	NestIdxValue niv = data;

	if (sz < sizeof(struct NestIdxValueData))
	{
		elog(WARNING,
				emsg("Insufficient space for the NestIdxValueData structure at "
					 "[%p] with a size of %llu.",
					 data,
					 sz));

		return 0;
	}

	/*
	 * Make sure data is on the right boundary
	 */
	assert(((uintptr_t)data) % sizeof(uintptr_t) == 0);

	niv->d_upper = offsetof(struct NestIdxValueData, slots[0]);
	niv->d_lower = sz;

	assert(vh_nestidxv_array_size(niv) == 0);

	return niv;
}

/*
 * vh_nestidxv_value
 *
 * Return null if the nl_idx and item_idx cannot be found in our array.
 */
void*
vh_nestidxv_value(NestIdxValue niv, int8_t nl_idx, int8_t item_idx)
{
	char *value = (char*)niv;
	int16_t i, max;

	max = vh_nestidxv_array_size(niv);

	for (i = 0; i < max; i++)
	{
		if (niv->slots[i].nl_idx == nl_idx &&
			niv->slots[i].item_idx == item_idx)
		{
			value += niv->slots[i].offset;

			return value;
		}
	}

	return 0;
}

/*
 * vh_nestidxv_add
 *
 * Make sure these things end on a uintptr_t boundary, to simplify our access
 * altogether.
 */
void*
vh_nestidxv_add(NestIdxValue niv, int8_t nl_idx, int8_t item_idx, size_t sz)
{
	char *value = (char*)niv;
	size_t align_diff, alloc_sz;
	int16_t max;

	/*
	 * Make sure we've got somewhat sane values here.  Someone could have passed
	 * in a pointer at the wrong address.
	 */
	assert(niv);
	assert(niv->d_upper <= niv->d_lower);
	assert(sz);

	if ((align_diff = (sz % sizeof(uintptr_t))))
		alloc_sz = sz + (sizeof(uintptr_t) - align_diff);
	else
		alloc_sz = sz;

	if (alloc_sz <= vh_nestidxv_freespace_slot(niv))
	{
		max = vh_nestidxv_array_size(niv);
		niv->d_upper += vh_nestidxv_slot_size;
		niv->d_lower -= alloc_sz;
		
		assert(niv->d_upper <= niv->d_lower);

		niv->slots[max].nl_idx = nl_idx;
		niv->slots[max].item_idx = item_idx;
		niv->slots[max].offset = niv->d_lower;

		value += niv->d_lower;

		return value;
	}
	else
	{
		/*
		 * Not enough space on the page for another item plus the requested
		 * size.
		 */

		elog(WARNING,
				emsg("Insufficent space on NestIdxValue [%p] for %llu bytes of "
					 "storage.  Only %llu bytes available.",
					 niv,
					 alloc_sz,
					 vh_nestidxv_freespace_slot(niv)));

		return 0;
	}

	return 0;
}

/*
 * vh_nestidxv_remove
 *
 * We should really concentrate on building a new value area to scratch
 * everything on and then just call memcpy.  Moving things around on the page
 * gets super messy and is easy to screw up.
 */
void
vh_nestidxv_remove(NestIdxValue niv, int8_t nl_idx, int8_t item_idx)
{
}


