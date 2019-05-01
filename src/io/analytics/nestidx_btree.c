/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/analytics/nestidx.h"
#include "io/utils/btree.h"


/*
 * ============================================================================
 * NestIdx Index Implementation Functions
 * ============================================================================
 */

static int32_t nestidx_btree_access(NestIdx idx, TypeVarSlot **keys, int32_t am,
									NestIdxAccess nia);


static int32_t nestidx_btree_scan_begin(NestIdx idx, NestIdxAccess nia,
										int32_t n_skeys);
static bool nestidx_btree_scan_first(NestIdx idx, NestIdxAccess nia,
									 NestScanKey *skeys, int32_t n_skeys,
									 bool forward);
static bool nestidx_btree_scan_next(NestIdx idx, NestIdxAccess nia, bool forward);
static void nestidx_btree_scan_end(NestIdx idx, NestIdxAccess nia);


const struct NestIdxFuncTable vh_nestidx_func_btree = {
	.access = nestidx_btree_access,

	/* Scans */
	.scan_begin = nestidx_btree_scan_begin,
	.scan_first = nestidx_btree_scan_first,
	.scan_next = nestidx_btree_scan_next,
	.scan_end = nestidx_btree_scan_end
};



/*
 * ============================================================================
 * BTree Helper Functions
 * ============================================================================
 */

static int32_t nestidx_btree_create(NestIdx idx, TypeVarSlot **keys);


/*
 * nestidx_btree_access
 *
 * Accesses the BTree per the NestIdx specification.  The first the we do is
 * see if we need to create the BTree to begin with.
 */
static int32_t
nestidx_btree_access(NestIdx idx, TypeVarSlot **keys, int32_t am,
					 NestIdxAccess nia)
{
	btRoot bt;
	void *value;
	int32_t res_create_idx;
	bool upsert, found;

	bt = idx->idx;

	if (!bt)
	{
		/*
		 * We need to create the index now.
		 */
		res_create_idx = nestidx_btree_create(idx, keys);

		if (res_create_idx)
		{
		}

		bt = idx->idx;
	}

	/*
	 * Run the delete first.  Our likely calling order is going to be 
	 * INSERT, FETCH, DELETE.  The fetch will be required prior to the
	 * delete to to clean up the data on the leaf.
	 */

	if (am & VH_NESTIDX_AM_DELETE)
	{
	}

	/*
	 * Check to see if we're an UPSERT access method.
	 */
	upsert = (am & VH_NESTIDX_AM_FETCH) && (am & VH_NESTIDX_AM_INSERT) ? true : false;
	found = vh_bt_find_tvs(bt, keys, idx->n_cols, &value);

	if (found && value)
	{
		/*
		 * We were able to find the key.
		 */

		nia->exists = true;
		nia->inserted = false;
		nia->data = value;

		return 0;
	}
	else
	{
		/*
		 * We could not find the key but may need to do an insert.
		 */
		if (upsert)
		{
			if (vh_bt_insert_tvs(bt, keys, idx->n_cols, &value))
			{
				nia->exists = false;
				nia->inserted = true;

				nia->data = value;

				return 0;
			}
			else
			{
				/*
				 * Unable to find or insert the key.  Don't to any logging here,
				 * let our caller determine what to do.
				 */

				return -10;
			}
		}
		else
		{
			/*
			 * We were not an upsert and could not find the requested key in the
			 * index so set the NIA that way.
			 */

			nia->exists = false;
			nia->inserted = false;
			
			nia->data = 0;

			return 0;
		}
	}

	/*
	 * Should not get here.
	 */
	return -1;
}

/*
 * nestidx_btree_create
 *
 * Creates the BTree index based on the keys we were provided.  We make sure that
 * the keys all have an accessible TypeStack.
 */
static int32_t
nestidx_btree_create(NestIdx idx, TypeVarSlot **keys)
{
	btRoot bt;
	Type tys[VH_TAMS_MAX_DEPTH];
	int32_t i;
	int8_t ty_depth;

	bt = vh_bt_create(vh_mctx_current(), true);

	if (!bt)
		return -1;

	for (i = 0; i < idx->n_cols; i++)
	{
		ty_depth = vh_tvs_fill_tys(keys[i], tys);

		if (ty_depth <= 0)
		{
			/*
			 * We've got a problem, let's unroll our btree and close this thing
			 * out.  We cannot create the BTree because we don't have a Type
			 * Stack for all of the columns.
			 */
			vh_bt_destroy(bt);

			return -2;
		}

		if (!vh_bt_add_column_tys(bt, tys, true))
		{
			/*
			 * There was a problem adding the column to the BTree.  Bail out.
			 */

			vh_bt_destroy(bt);

			return -3;
		}
	}

	idx->idx = bt;

	return 0;
}

static int32_t 
nestidx_btree_scan_begin(NestIdx idx, NestIdxAccess nia,
						 int32_t n_skeys)
{
	nia->opaque = vh_bt_scan_begin(idx->idx, n_skeys);

	return nia->opaque ? 0 : -1;
}


static bool 
nestidx_btree_scan_first(NestIdx idx, NestIdxAccess nia,
						 NestScanKey *skeys, int32_t n_skeys,
		 				 bool forward)
{
	TypeVarSlot *slot_array;
	int32_t i;
	bool impl = false;
   
	if (!n_skeys)
	{
		impl = vh_bt_scan_first(nia->opaque, 0, 0, forward);

		if (impl)
		{
			/*
			 * Get our leaf value pointer and copy over the TypeVarSlot
			 * for the keys into nia.
			 */
			
			vh_bt_scan_get(nia->opaque, &slot_array, &nia->data);

			for (i = 0; i < idx->n_cols; i++)
			{
				vh_tvs_copy(&nia->keys[i], &slot_array[i]);
			}
		}
	}
	else
	{
	}
	
	return impl;
}

static bool 
nestidx_btree_scan_next(NestIdx idx, NestIdxAccess nia, bool forward)
{
	TypeVarSlot *slot_array;
	int32_t i;
	bool impl = false;

	impl = vh_bt_scan_next(nia->opaque, forward);

	if (impl)
	{
		/*
		 * Get our leaf value pointer and copy over the TypeVarSlot
		 * for the keys into nia.
		 */
		
		vh_bt_scan_get(nia->opaque, &slot_array, &nia->data);

		for (i = 0; i < idx->n_cols; i++)
		{
			vh_tvs_copy(&nia->keys[i], &slot_array[i]);
		}
	}

	return impl;
}


static void
nestidx_btree_scan_end(NestIdx idx, NestIdxAccess nia)
{
	vh_bt_scan_end(nia->opaque);

	vhfree(nia->extra);
}

