/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TypeVarAcm.h"
#include "io/catalog/sp/spht.h"
#include "io/catalog/prepcol/prepcol.h"
#include "io/catalog/PrepTup.h"
#include "io/catalog/prepcol/pcminmax.h"
#include "io/analytics/ml/mlnormalize.h"


/*
 * We need to be able to insert a HeapTuplePtr and for each column, get the
 * appropriate inputs for a PrepCol.
 *
 * For the MinMax PrepCol, we'll need to collect the Min and Max values.
 *
 * After all of the data has been run thru, we can form a PrepTup.
 */

typedef struct MLNormalizeColData *MLNormalizeCol;

struct MLNormalizeData
{
	MLNormalizeCol *cols;
	int32_t n_cols;
};

struct MLNormalizeColData
{
	char *col_name;
	SearchPath sp;
	int32_t function;
};


/*
 * ============================================================================
 * Min/Max Support
 * ============================================================================
 */

struct pcminmax_data
{
	struct MLNormalizeColData ncd;

	TypeVarAcm tvacm_min;
	TypeVarAcm tvacm_max;

	TypeVarAcmState tvacms_min;
	TypeVarAcmState tvacms_max;
};

static int32_t mln_minmax_input_htp(void *data, TypeVarSlot *slot);
static PrepCol mln_minmax_generate_prepcol(void *coldata);



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

MLNormalize
vh_mln_create(void)
{
	MLNormalize mln;

	mln = vhmalloc(sizeof(struct MLNormalizeData));
	mln->cols = 0;
	mln->n_cols = 0;

	return mln;
}

void
vh_mln_destroy(MLNormalize mln)
{
	union
	{
		MLNormalizeCol mlc;
		struct pcminmax_data *mm;
	} col;

	int32_t i;

	for (i = 0; i < mln->n_cols; i++)
	{
		col.mlc = mln->cols[i];

		switch (col.mlc->function)
		{
			case VH_MLN_FUNC_MINMAX:
				vh_acms_destroy(col.mm->tvacm_min, col.mm->tvacms_min);
				vh_acms_destroy(col.mm->tvacm_max, col.mm->tvacms_max);

				vh_acm_destroy(col.mm->tvacm_min);
				vh_acm_destroy(col.mm->tvacm_max);

				break;

		}

		vhfree(col.mlc);
	}

	if (mln->cols)
	{
		vhfree(mln->cols);
		mln->cols = 0;
		mln->n_cols = 0;
	}

	vhfree(mln);
}

void
vh_mln_add_col(MLNormalize mln, const char *col_name, int32_t function)
{
	size_t alloc_sz;
	union
	{
		MLNormalizeCol col;
		struct pcminmax_data *mm;
	} col;

	if (!mln)
	{
		elog(ERROR,
				emsg("Invalid MLNormalize pointer [%p] passed to "
					 "vh_mln_add_col.  Unable to normalize the column as requested",
					 mln));
	}

	if (!col_name)
	{
		elog(ERROR,
				emsg("Invalid column name [%p] passed to "
					 "vh_mln_add_col.  Unable to normalize the column as requested",
					 mln));
	}

	col.col = 0;

	switch (function)
	{
		case VH_MLN_FUNC_NONE:
			alloc_sz = sizeof(struct MLNormalizeColData);
			col.mm = vhmalloc(alloc_sz);
			
			break;

		case VH_MLN_FUNC_MINMAX:
			alloc_sz = sizeof(struct pcminmax_data);
			col.mm = vhmalloc(alloc_sz);

			col.mm->tvacm_min = 0;
			col.mm->tvacm_max = 0;
			col.mm->tvacms_min = 0;
			col.mm->tvacms_max = 0;

			break;

		default:

			elog(ERROR,
					emsg("Unrecognized function [%d] passed to vh_mln_add_col for the "
						 "MLNormalize at [%p].  Unable to proceed.",
						 function,
						 mln));

			break;
	}
	
	if (col.col)
	{
		col.col->col_name = vh_cstrdup(col_name);
		col.col->sp = vh_spht_tf_create(col.col->col_name);
		col.col->function = function;

		if (mln->n_cols)
		{
			alloc_sz = sizeof(MLNormalizeCol) * (mln->n_cols + 1);

			mln->cols = vhrealloc(mln->cols, alloc_sz);
		}
		else
		{
			alloc_sz = sizeof(MLNormalizeCol);

			mln->cols = vhmalloc(alloc_sz);
		}

		mln->cols[mln->n_cols++] = col.col;
	}
}

int32_t
vh_mln_input_htp(MLNormalize mln, HeapTuplePtr htp)
{
	HeapTuple ht;
	HeapField hf;
	MLNormalizeCol mlc;
	TypeVarSlot slot;
	int32_t i, sp_ret, func_ret = 0, fail_count = 0;

	vh_tvs_init(&slot);
	ht = vh_htp(htp);

	for (i = 0; i < mln->n_cols; i++)
	{
		mlc = mln->cols[i];

		hf = vh_sp_search(mlc->sp, &sp_ret, 1,
						  VH_SP_CTX_HT, ht);

		if (sp_ret <= 0)
		{
			/*
			 * There was a problem with the SearchPath, it didn't find the column
			 * we were looking for.
			 */

			fail_count++;
		}

		vh_tvs_store_ht_hf(&slot, ht, hf);

		switch (mlc->function)
		{
			case VH_MLN_FUNC_MINMAX:
				func_ret = mln_minmax_input_htp(mlc, &slot);

				if (func_ret)
					fail_count++;

				break;

			default:

				/*
				 * We shouldn't get here if our vh_mln_add_col function is working
				 * correctly.
				 */

				assert(0 == 1);

				break;
		}
	}

	return fail_count;
}

int32_t
vh_mln_generate_preptup(MLNormalize mln, HeapBufferNo hbno, PrepTup *pt_out)
{
	SearchPath paths[2];
	PrepTup pt;
	PrepCol pc;
	MLNormalizeCol mlc;
	int32_t i, pt_res;

	pt = vh_pt_create(hbno);

	for (i = 0; i < mln->n_cols; i++)
	{
		mlc = mln->cols[i];
		paths[0] = mlc->sp;

		switch (mlc->function)
		{
			case VH_MLN_FUNC_MINMAX:
				pc = mln_minmax_generate_prepcol(mlc);
			
			default:
				pc = 0;
				break;
		}

		pt_res = vh_pt_col_add(pt, mlc->col_name,
							   paths, 0, 1, pc);

		if (pt_res)
		{
		}
	}

	*pt_out = pt;

	return 0;
}


static int32_t 
mln_minmax_input_htp(void *data, TypeVarSlot *slot)
{
	struct pcminmax_data *mm = data;
	Type tys[VH_TAMS_MAX_DEPTH];
	int32_t ret;

	if (!mm->tvacm_min)
	{
		/*
		 * We need to create the ACM and ACM State.
		 */
		vh_tvs_fill_tys(slot, tys);

		mm->tvacm_min = vh_acm_min_tys(tys);
		mm->tvacm_max = vh_acm_max_tys(tys);

		mm->tvacms_min = vh_acms_create(mm->tvacm_min);
	   	mm->tvacms_max = vh_acms_create(mm->tvacm_max);	
	}

	ret = vh_acms_input(mm->tvacm_min, mm->tvacms_min, slot);

	if (ret)
	{
		return ret;
	}

	ret = vh_acms_input(mm->tvacm_max, mm->tvacms_max, slot);

	if (ret)
	{
		return ret;
	}

	return 0;
}

static PrepCol 
mln_minmax_generate_prepcol(void *coldata)
{
	TypeVarSlot min, max;
	struct pcminmax_data *mm = coldata;
	PrepCol pc;
	int32_t res;

	vh_tvs_init(&min);
	vh_tvs_init(&max);

	res = vh_acms_result(mm->tvacm_min, mm->tvacms_min, &min);

	if (res)
	{
	}

	res = vh_acms_result(mm->tvacm_max, mm->tvacms_max, &max);

	if (res)
	{
	}

	pc = vh_pcminmax_create(&min, &max);

	return pc;	
}

