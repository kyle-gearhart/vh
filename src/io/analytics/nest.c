/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>

#include "vh.h"
#include "io/analytics/nestidx.h"
#include "io/analytics/nestlevel.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableField.h"
#include "io/catalog/TypeVar.h"



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */
Nest
vh_nest_create(void)
{
	Nest n;

	n = vhmalloc(sizeof(struct NestData));
	memset(n, 0, sizeof(struct NestData));

	return n;
}

int32_t
vh_nest_level_add(Nest nest, NestLevel nl)
{
	NestIdx ni;
	int32_t i;

	if (nest->n_idxs)
	{
	}
	else
	{
		/*
		 * This is our first one, we can spin this thing up exactly how the nest
		 * level wants us to.  One thing that's extremely imporant here is that
		 * we don't actually create an index yet.  Instead, we wait for the first
		 * HeapTuple to arrive.  nestidx.c is smart enough to build our index
		 * on the fly.  This way we don't have to know the data types until the
		 * very last moment.
		 */

		ni = nest->idxs = vhmalloc(sizeof(struct NestIdxData));
		memset(ni, 0, sizeof(struct NestIdxData));

		nest->n_idxs = 1;

		ni->method = VH_NESTIDX_METH_BTREE;
		ni->cols = vhmalloc(sizeof(GroupByCol*) * nl->groupby_n_cols);
		ni->n_cols = nl->groupby_n_cols;
		
		for (i = 0; i < nl->groupby_n_cols; i++)
		{
			ni->cols[i] = &nl->groupby_cols[i];
			ni->cols[i]->idx_slot = i;
		}
	}

	if (ni->n_ls)
	{
		ni->ls = vhrealloc(ni->ls, sizeof(struct NestLevelData*) * (ni->n_ls + 1));
		ni->ls[ni->n_ls++] = nl;
	}
	else
	{
		ni->ls = vhmalloc(sizeof(struct NestLevelData*));
		ni->ls[ni->n_ls++] = nl;
	}

	if (nest->n_ls)
	{
		nest->ls = vhrealloc(nest->ls, sizeof(struct NestLevelData*) * (nest->n_ls + 1));
		nest->ls[nest->n_ls++] = nl;
	}
	else
	{
		nest->ls = vhmalloc(sizeof(struct NestLevelData*));
		nest->ls[nest->n_ls++] = nl;
	}

	nl->nest = nest;

	return 0;
}

/*
 * vh_nest_input_htp
 *
 * How do we determine what indexes to send this thing to?  By default, we'll
 * iterate thru the indexes.  In the future, we should use this function to do
 * the routing and then have another build out the TypeVarSlot to access the
 * index with.
 */

int32_t
vh_nest_input_htp(Nest nest, HeapTuplePtr htp)
{
	NestIdx ni;
	NestIdxAccess nia;
	NestIdxValue niv;
	NestLevel nl;
	GroupByCol gbc, *gbc_acols;
	TypeVarSlot *slots, slot_temp, *slot_datas[1], **slot_akeys;
	TableField tf;
	HeapTuple ht;
	int32_t i, j, k, sp_ret, pc_ret, acc_ret, nl_input_ret;

	ht = vh_htp(htp);

	for (i = 0; i < nest->n_idxs; i++)
	{
		ni = &nest->idxs[i];

		slots = vhmalloc((sizeof(TypeVarSlot) + 
						  sizeof(GroupByCol) + 
						  sizeof(TypeVarSlot*)) * 
						 ni->n_cols);
	
		gbc_acols = (GroupByCol*)(slots + ni->n_cols);
		slot_akeys = (TypeVarSlot**)(gbc_acols + ni->n_cols);
		
		nia = vh_nestidx_access_create(ni); 

		
		for (j = 0; j < ni->n_cols; j++)
		{
			gbc = ni->cols[j];
			vh_tvs_init(&slots[j]);

			tf = vh_sp_search(gbc->sp_field, &sp_ret, 1,
				   			  VH_SP_CTX_HT, ht);

			if (!sp_ret)
			{
				/*
				 * We could not resolve TableField using the search path.
				 */
			}
			

			switch (gbc->type)
			{
				case GBT_COL:

					vh_tvs_store_ht_hf(&slots[j], ht, &tf->heap);
					pc_ret = 1;
			 		break;

				case GBT_PREPCOL:
					/*
					 * Store our HeapTuple value in the in the slot_datas argument.
					 *
					 * We pass in the slot[j] to be filled by the PrepCol.
					 */
					vh_tvs_init(&slot_temp);
					vh_tvs_store_ht_hf(&slot_temp, ht, &tf->heap);
					slot_datas[0] = &slot_temp;
			
					vh_tvs_store_var(&slots[j], 
							 vh_typevar_make_tys(tf->heap.types),
							 VH_TVS_RA_DVAR);

					pc_ret = vh_pc_populate_slot(gbc->pc,
												 &slots[j],
					 							 slot_datas,
				 								 1);
					break;
			}

			if (pc_ret < 0)
			{
				/*
				 * We had a problem populating the slot.
				 */
			}
		}

		/*
		 * Now, based on each NestLevel that's attached to this index, we need
		 * access it.  We prefer to go in least specific (smallest number of 
		 * Group Bys) to most specific.  But this isn't always possible, 
		 * especially if this is our first insert into the index altogether.
		 *
		 * After each index is accessed, we need to trigger the nest level to
		 * do it's thing with the HeapTuple.  We pass a few things over to
		 * the NestLevel to accomplish this:
		 * 	1)	NestLevel
		 * 	2)	The portion of the leaf that belongs to it
		 * 	3)	The size available to it
		 * 	4)	HeapTuple
		 * 	5)	HeapTuplePtr
		 * 	6)	TypeVarSlot for the keys we used to get here (++ gotta fix this one)
		 * 	7)	TypeVarSlot for the keys applicable to the NestLevel (slot_akeys)
		 * 	8)	An intialize flag (we had to insert into the index)
		 *
		 * We'll add this HeapTuplePtr to the array prior to triggering the
		 * NestLevel to do its calculations.  Adding to the array can be a 
		 * bit tricky, because the only array this goes in is the most 
		 * specific key possible.   This way when we scan just the first
		 * key, we get everything with no duplicates (unless the same HTP
		 * was inserted twice).
		 *
		 * Should the NestLevel need to iterate over all the HeapTuple, it
		 * can call NestIdx to move across the array that span one or more
		 * key spaces.
		 */


		/*
		 * Iterate the NestLevels attached to this index and spin thru their
		 * GroupBy columns.
		 */

		for (j = 0; j < ni->n_ls; j++)
		{
			nl = ni->ls[j];

			for (k = 0; k < nl->groupby_n_cols; k++)
			{
				gbc_acols[k] = &nl->groupby_cols[k];
				slot_akeys[k] = &slots[nl->groupby_cols[k].idx_slot];
			}

			/*
			 * Now we've got enough information to access the index.
			 */
			acc_ret = vh_nestidx_access(ni, 
										gbc_acols, slot_akeys, nl->groupby_n_cols,
										VH_NESTIDX_AM_FETCH | VH_NESTIDX_AM_INSERT,
										nia);

			if (acc_ret)
			{
				/*
				 * There was an error access this index.
				 */
			}

			niv = nia->data;

			if (nia->inserted)
			{
				vh_nestidxv_initialize(niv, 2000);
			}

			/*
			 * Call the NestLevel to compute it's aggregates.
			 */
			nl_input_ret = vh_nl_input_ht(nl, ni, niv, 
										  ht, htp, 
										  slot_akeys, 0,
										  nia->inserted);

			if (nl_input_ret)
			{
			}
		}

		vhfree(slots);

		for (j = 0; j < nl->groupby_n_cols; j++)
		{
			vh_tvs_reset(&slots[j]);
		}
	}

	return 0;
}

