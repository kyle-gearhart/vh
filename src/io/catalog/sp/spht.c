/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/catalog/searchpath.h"


/*
 * We use spht.c to do a good bit for us, but we only publicly expose the creation
 * functions.  This simplifies creation routines to be much more specific.  Under
 * the hood, we don't care about exposing complexity.
 */

static void* spht_search(SearchPath sp, int32_t *ret, int32_t nrt_args, ...);
static void* spht_next(SearchPath sp, int32_t *ret);

static const struct SearchPathFuncTableData spht_func = {
	.search = spht_search,
	.next = spht_next
};



/*
 * ============================================================================
 * SP Heap Tuple Data Structures
 * ============================================================================
 */

struct spht_data
{
	struct SearchPathData sp;

	const char *fname;

	TableDefVer tdv;
	TableField tf;
};



/*
 * ============================================================================
 * SP Heap Tuple Internal Functions
 * ============================================================================
 */
static void* spht_data(struct spht_data *sp, HeapTuple ht, TableField tf);
static TableField spht_tf(struct spht_data *sp, HeapTuple ht,
						  TableDef td, TableDefVer tdv,
						  const char *fname);



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */


/*
 * vh_spht_tf_create
 *
 * Returns a TableField
 */
SearchPath
vh_spht_tf_create(const char *name)
{
	struct spht_data *sp;

	sp = vh_sp_create(&spht_func, SPRET_TableField, sizeof(struct spht_data));	
	sp->fname = name;

	return &sp->sp;
}

/*
 * vh_spht_dat_create
 *
 * Returns a pointer to the data but does not guarantee it's a TypeVar.  It could
 * be the data pointer is to a field on a HeapTuple.
 */
SearchPath
vh_spht_dat_create(const char *fname)
{
	struct spht_data *sp;

	sp = vh_sp_create(&spht_func, SPRET_DataAt, sizeof(struct spht_data));	
	sp->fname = fname;

	return &sp->sp;
}



/*
 * ============================================================================
 * Private Interface
 * ============================================================================
 */


/*
 * spht_next
 *
 * There's not every going to be a next with this.  We've either got it or we
 * don't.  Just return 0 to indicate that there's nothing left.
 */
static void*
spht_next(SearchPath sp, int32_t *ret)
{
	*ret = 0;

	return 0;
}

/*
 * spht_search
 *
 * Yank the runtime argments off and then see what we can do.  There's no guarantee
 * the caller has provided us with the information we need to grab a field.
 */
static void* 
spht_search(SearchPath sp, int32_t *ret, int32_t nrt_args, ...)
{
	struct spht_data *sph = (struct spht_data*)sp;
	HeapTuple ht = 0;
	HeapTuplePtr htp;
	TableDef td = 0;
	TableDefVer tdv = 0;
	TableField tf = 0;
	const char *fname = 0;
	void *dat;
	int32_t i = 0, rt;
	va_list args;

	va_start(args, nrt_args);

	for (i = 0; i < nrt_args; i++)
	{
		rt = va_arg(args, int32_t);

		switch (rt)
		{
			case VH_SP_CTX_HT:
				ht = va_arg(args, HeapTuple);
				break;

			case VH_SP_CTX_HTP:
				htp = va_arg(args, HeapTuplePtr);
				ht = vh_htp(htp);

				break;

			case VH_SP_CTX_TD:
				td = va_arg(args, TableDef);
				break;

			case VH_SP_CTX_TDV:
				tdv = va_arg(args, TableDefVer);
				break;

			case VH_SP_CTX_FNAME:
				fname = va_arg(args, const char*);
				break;

			case VH_SP_CTX_TVS:
				//tvs = va_arg(args, TypeVarSlot);
				break;

			default:
				/*
				 * Call vh_sp_pull_unk_arg to move an argument off the va_list
				 * that we don't care about.  We have to do this with a function
				 * call because if we don't get the size right then we'll mess
				 * up the rest of our stack.  So this generic yank does that for
				 * us and keeps our stack inline.
				 *
				 * Without this, we'd have to implement every type imaginable in
				 * the search stack of every SearchPath ever...ugly!
				 */
				vh_sp_pull_unk_arg(rt, args);
				break;
		}
	}

	va_end(args);

	/*
	 * Based on the arguments collected, we now get to form the desired result.
	 *
	 * Since this can vary, we call a few functions internally with our context
	 * we built above.  For example, getting a TableField is simple, but filling
	 * a TypeVarSlot gets a little more complicated.
	 */

	switch (sp->spret)
	{
		case SPRET_DataAt:
			tf = spht_tf(sph, ht, td, tdv, fname);

			if (tf)
			{
				dat = spht_data(sph, ht, tf);

				*ret = 1;

				return dat;	
			}

			*ret = 0;

			return 0;
			

		case SPRET_TableField:
			tf = spht_tf(sph, ht, td, tdv, fname);

			*ret = tf ? 1 : 0;

			return tf;

		default:
			*ret = -2;
			return 0;

	}

	*ret = -2;

	return 0;
}

static TableField 
spht_tf(struct spht_data *sp, HeapTuple ht, TableDef td, TableDefVer tdv,
		const char *fname)
{
	TableField tf = 0;
 	const char *sname;
	bool loopback[2] = { 0, 0 };
	
	/*
	 * Step down our priority tree for extracting a TableField
	 */
	while (!tdv)
	{
		if (td && !loopback[0])
		{
			tdv = vh_td_tdv_lead(td);
			loopback[0] = true;
			continue;
		}

		if (ht && !loopback[1]) 
		{
			tdv = (TableDefVer)ht->htd;
			loopback[1] = true;
			continue;
		}

		break;
	}

	if (tdv)
	{
		sname = fname ? fname : sp->fname;

		if (sp->tdv == tdv &&
			sname == sp->fname)
		{
			return sp->tf;
		}

		if (sname)
		{
			tf = vh_tdv_tf_name(tdv, sname);

			if (sname == sp->fname)
			{
				sp->tdv = tdv;
				sp->tf = tf;
			}
		}
	}

	return tf;
}

static void* 
spht_data(struct spht_data *sp, HeapTuple ht, TableField tf)
{
	if (ht && tf)
	{
		return vh_ht_field(ht, &tf->heap);
	}

	return 0;
}

