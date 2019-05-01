/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/buffer/HeapBuffer.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/searchpath.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/catalog/prepcol/prepcol.h"
#include "io/utils/kset.h"
#include "io/utils/SList.h"

#include "io/catalog/PrepTup.h"


/*
 * ============================================================================
 * Data Structures
 * ============================================================================
 */

struct PrepTupColData
{
	char *target_column;
	int32_t target_column_idx;

	SearchPath *searchpaths;
	bool *chain;
	int32_t n_searchpaths;

	PrepCol prepcol;
};

struct PrepTupData
{
	HeapBufferNo hbno;
	HeapTupleDef htd;
	TableDef td;

	PrepTupCol cols;
	int32_t n_cols;

	int32_t max_searchpaths;
	int32_t count_target_columns;

	KeySet target_column_names;
};


/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

static int32_t pt_create_htd(PrepTup pt, TypeVarSlot *values);
static PrepTupCol pt_ptc_byname(PrepTup pt, const char *name);



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

PrepTup
vh_pt_create(HeapBufferNo hbno)
{
	PrepTup pt;
	size_t alloc_sz;

	alloc_sz = sizeof(struct PrepTupData);

	pt = vhmalloc(alloc_sz);
	memset(pt, 0, alloc_sz);

	pt->hbno = hbno;
	pt->target_column_names = vh_cstr_kset_create();

	return pt;
}

void
vh_pt_destroy(PrepTup pt)
{
	PrepTupCol ptc;
	int32_t i;

	for (i = 0; i < pt->n_cols; i++)
	{
		ptc = &pt->cols[i];

		if (ptc->target_column)
		{
			vhfree(ptc->target_column);
			ptc->target_column = 0;
		}

		if (ptc->searchpaths)
		{
			vhfree(ptc->searchpaths);
	   		ptc->searchpaths = 0;
		}

		if (ptc->chain)
		{
			vhfree(ptc->chain);
			ptc->chain = 0;
		}
	}

	vhfree(pt->cols);
	vhfree(pt);
}

int32_t
vh_pt_col_add(PrepTup pt, char *target_column,
			  SearchPath *paths, bool *chain,
			  int32_t n_paths,
			  PrepCol pc)
{
	PrepTupCol ptc, ptce;
	size_t alloc_sz;

	if (!target_column)
	{
		elog(WARNING,
				emsg("A target column name is a required parameter"));
		return -1;
	}

	if (!paths)
	{
		elog(WARNING,
				emsg("Atleast one SearchPath is required t add a column to a PrepTup"));
		return -2;
	}

	alloc_sz = sizeof(struct PrepTupColData);

	if (pt->n_cols)
	{
		alloc_sz *= (pt->n_cols + 1);
		pt->cols = vhrealloc(pt->cols, alloc_sz);
		ptc = &pt->cols[pt->n_cols++];	
	}
	else
	{
		pt->cols = vhmalloc(alloc_sz);
		ptc = &pt->cols[pt->n_cols++];
	}

	if (vh_kset_exists(pt->target_column_names, target_column))
	{
		ptce = pt_ptc_byname(pt, target_column);
		assert(ptce);

		ptc->target_column_idx = ptce->target_column_idx;
	}
	else
	{
		ptc->target_column_idx = pt->count_target_columns++;
	}

	ptc->target_column = vh_cstrdup(target_column);

	if (paths)
	{
		ptc->searchpaths = vhmalloc(sizeof(SearchPath) * n_paths);
		memcpy(ptc->searchpaths, paths, sizeof(SearchPath) * n_paths);
	}

	if (chain)
	{
		ptc->chain = vhmalloc(sizeof(bool) * n_paths);
		memcpy(ptc->chain, chain, sizeof(bool) * n_paths);
	}

	if (n_paths > pt->max_searchpaths)
		pt->max_searchpaths = n_paths;

	return 0;
}


int32_t
vh_pt_input_htp(PrepTup pt,
				HeapTuplePtr htp_in, HeapTuple ht_in,
				HeapTuplePtr *htp_out, HeapTuple *ht_out)
{
	size_t alloc_sz;
	PrepTupCol ptc;
	TypeVarSlot *target_cols, *searchpath_cols, **prepcol_datas, *slot;
	HeapField *hfs, hf;
	HeapTuplePtr htp;
	HeapTuple ht;
	int32_t i, j, sp_ret, htd_ret, hf_sz;

	alloc_sz = (sizeof(TypeVarSlot) * 
			   (pt->max_searchpaths + pt->count_target_columns)) +
			   (sizeof(TypeVarSlot*) * pt->max_searchpaths);
	target_cols = vhmalloc(alloc_sz);
	memset(target_cols, 0, alloc_sz);

	searchpath_cols = target_cols + pt->count_target_columns;
	prepcol_datas = (TypeVarSlot**)(searchpath_cols + pt->max_searchpaths);

	for (i = 0; i < pt->n_cols; i++)
	{
		ptc = &pt->cols[i];

		for (j = 0; j < ptc->n_searchpaths; j++)
		{
			hf = vh_sp_search(ptc->searchpaths[j], &sp_ret, 1,
				   			  VH_SP_CTX_HT, ht_in);
			
			if (!hf)
			{
			}
			
			if (ptc->chain[j])
			{
				prepcol_datas[j] = &target_cols[j];
			}
			else
			{
				prepcol_datas[j] = &searchpath_cols[j];
				vh_tvs_store_ht_hf(&searchpath_cols[j], ht_in, hf);	
			}
		}

		if (ptc->prepcol)
		{
			vh_pc_populate_slot(ptc->prepcol, &target_cols[i], 
								prepcol_datas, ptc->n_searchpaths);
		}
		else
		{
			/*
			 * We don't have a PrepCol to populate the slot with, so we'll just
			 * transfer the value over to the new tuple as is.
			 */

			vh_tvs_copy(&target_cols[i], &searchpath_cols[0]);
		}
	}

	if (!pt->htd)
	{
		/*
		 * We don't have a HeapTupleDef created yet, likely because this is our
		 * first run thru the PrepTup.
		 *
		 * Create the HeapTupleDef so that we can transfer the values stored
		 * in the target_cols TypeVarSlot array into the new HeapTuple.
		 */

		htd_ret = pt_create_htd(pt, target_cols);

		if (htd_ret)
		{
			/*
			 * There was a problem with the input HeapTuple, it likely had a null
			 * value somewhere which prevented us from detecting the underlying
			 * data type.
			 *
			 * Just clean up our mess and get out of here.
			 */

			vhfree(target_cols);

			return -1;
		}
	}

	/*
	 * Transfer the values over using a memset TAM.
	 */

	htp = vh_hb_allocht(vh_hb(pt->hbno), pt->htd, &ht);

	hf_sz = vh_SListIterator(pt->htd->fields, hfs);

	for (i = 0; i < hf_sz; i++)
	{
		hf = hfs[i];
		slot = &target_cols[i];

		if (vh_tvs_isnull(slot))
		{
			vh_htf_setnull(ht, hf);
		}
		else
		{
			vh_htf_clearnull(ht, hf);

			vh_tam_fire_memset_set(hf->types,
								   vh_tvs_value(slot),
								   vh_ht_field(ht, hf),
								   false);
		}

		vh_tvs_finalize(slot);
	}

	*htp_out = htp;
	*ht_out = ht;

	return 0;
}

static int32_t 
pt_create_htd(PrepTup pt, TypeVarSlot *values)
{
	TableDef td;
	PrepTupCol ptc;
	HashTable htbl;
	Type tys[VH_TAMS_MAX_DEPTH];
	TypeVarSlot *slot;
	int32_t i, fail = 0;
	int8_t depth;

	td = vh_td_create(false);
	htbl = vh_cstr_kset_create();

	for (i = 0; i < pt->n_cols; i++)
	{
		ptc = &pt->cols[i];

		/*
		 * Check to see if this column has already been added.
		 */

		if (vh_kset_exists(htbl, ptc->target_column))		{
			continue;
		}
	
		slot = &values[ptc->target_column_idx];
		depth = vh_tvs_fill_tys(slot, tys);

		if (depth)
		{
			vh_td_tf_add(td, tys, ptc->target_column);
			vh_kset_key(htbl, ptc->target_column);
		}
		else
		{
			elog(WARNING,
					emsg("PrepTup cannot determine the type stack for the column "
						 "at index %d for the PrepTup at [%p]",
						 i,
						 pt));
			fail++;
		}
	}

	vh_kset_destroy(htbl);

	if (fail)
	{
		vh_td_finalize(td);
		vhfree(td);
	}
	else
	{
		pt->td = td;
		pt->htd = vh_td_htd(td);
	}

	return fail;
}

static PrepTupCol 
pt_ptc_byname(PrepTup pt, const char *name)
{
	PrepTupCol ptc;
	int32_t i;
	bool found = false;

	for (i = 0; i < pt->n_cols; i++)
	{
		ptc = &pt->cols[i];

		if (ptc->target_column)
		{
			if (strcmp(ptc->target_column, name) == 0)
			{
				found = true;
				break;
			}
		}
	}

	return found ? ptc : 0;
}

