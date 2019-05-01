/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/searchpath.h"
#include "io/catalog/sp/sptd.h"
#include "io/utils/SList.h"



/*
 * ============================================================================
 * Search Path Function Table
 * ============================================================================
 */

static void* sptd_search(SearchPath sp, int32_t *ret, int32_t nrt_args, ...);
static void* sptd_next(SearchPath sp, int32_t *ret);
static int32_t sptd_reset(SearchPath sp);

static void sptd_finalize(SearchPath sp);

static const struct SearchPathFuncTableData sptd_func = {
	.search = sptd_search,
	.next = sptd_next,
	.reset = sptd_reset,

	.finalize = sptd_finalize
};



/*
 * ============================================================================
 * SP Table Def Data Structures
 * ============================================================================
 */

typedef struct sptd_data sptd_data;

struct sptd_data
{
	struct SearchPathData sp;

	SList schemas;
	SList catalogs;
	const char *table_name;

	SList tables;

	int32_t version_flags;
	int32_t exact_version;
	int32_t min_version;
	int32_t max_version;

	int32_t iter_idx;

	bool unique;
};



/*
 * ============================================================================
 * Helper Functions
 * ============================================================================
 */
static TableDef sptd_td(sptd_data *sptd, TableCatalog tc,
	   					const char *sname, const char *tname);
static bool sptd_td_unique(sptd_data *sptd, TableDef td);

#define sptd_table_count(sptd)			((sptd)->tables ? vh_SListSize((sptd)->tables) : 0)
					

/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

SearchPath
vh_sptd_create(SearchPathTableDefOpts *opts, int32_t flags)
{
	sptd_data *sptd;

	sptd = vh_sp_create(&sptd_func, SPRET_TableDef, sizeof(struct sptd_data));

	if (opts->schema_name &&
		flags & VH_SPTD_OPT_SNAME)
	{
		sptd->schemas = vh_SListCreate();
		vh_SListPush(sptd->schemas, (void*)opts->schema_name);
	}

	if (opts->table_name &&
		flags & VH_SPTD_OPT_TNAME)
	{
		sptd->table_name = opts->table_name;
	}

	if (opts->tc &&
		flags & VH_SPTD_OPT_TC)
	{
		sptd->catalogs = vh_SListCreate();
		vh_SListPush(sptd->catalogs, opts->tc);
	}

	if (flags & VH_SPTD_OPT_MINVER)
		sptd->min_version = opts->min_version;
	else
		sptd->min_version = 0;

	if (flags & VH_SPTD_OPT_UNIQUE)
		sptd->unique = opts->unique;
	else
		sptd->unique = false;

	sptd->tables = vh_SListCreate();
	sptd->iter_idx = -1;

	return &sptd->sp;
}

SearchPath
vh_sptd_default(void)
{
	sptd_data *sptd;
	CatalogContext cc;

	cc = vh_ctx();

	sptd = vh_sp_create(&sptd_func, SPRET_TableDef, sizeof(struct sptd_data));

	sptd->catalogs = vh_SListCreate();
	sptd->tables = vh_SListCreate();
	sptd->unique = false;
	sptd->iter_idx = -1;

	if (cc->catalogTable)
		vh_SListPush(sptd->catalogs, cc->catalogTable);

	return &sptd->sp;
}

void
vh_sptd_schema_add(SearchPath sp, const char *schema)
{
	sptd_data *sptd = (sptd_data*)sp;

	if (!schema)
	{
		elog(WARNING, 
				emsg("Invalid schema name pointer [%p] when attempting to add the "
					 "schema to the SearchPath.",
					 schema));

		return;
	}

	if (!sptd->schemas)
		sptd->schemas = vh_SListCreate();

	vh_SListPush(sptd->schemas, (void*)schema);
}

void
vh_sptd_tc_add(SearchPath sp, TableCatalog tc)
{
	sptd_data *sptd = (sptd_data*)sp;

	if (!tc)
	{
		elog(WARNING,
				emsg("Invalid TableCatalog pointer [%p] when attempting to add the "
					 "schema to the SearchPath.",
					 tc));

		return;
	}

	if (!sptd->catalogs)
		sptd->catalogs = vh_SListCreate();

	vh_SListPush(sptd->catalogs, tc);
}

SearchPath
vh_sptdv_default(void)
{
	sptd_data *sptd;
	CatalogContext cc;

	cc = vh_ctx();

	sptd = vh_sp_create(&sptd_func, SPRET_TableDefVer, sizeof(struct sptd_data));

	sptd->catalogs = vh_SListCreate();
	sptd->tables = vh_SListCreate();
	sptd->unique = false;
	sptd->iter_idx = -1;

	if (cc->catalogTable)
		vh_SListPush(sptd->catalogs, cc->catalogTable);

	return &sptd->sp;
}


/*
 * ============================================================================
 * SearchPath Functions
 * ============================================================================
 */

static void* 
sptd_search(SearchPath sp, int32_t *ret, int32_t nrt_args, ...)
{
	sptd_data *sptd = (sptd_data*)sp;
	va_list args;
	const char *table_name = 0, *schema_name = 0;
	TableCatalog tc = 0;
	TableDef td = 0;
	int32_t i, rt;

	va_start(args, nrt_args);

	for (i = 0; i < nrt_args; i++)
	{
		rt = va_arg(args, int32_t);

		switch (rt)
		{
			case VH_SP_CTX_TC:
				tc = va_arg(args, TableCatalog);
				break;

			case VH_SP_CTX_TNAME:
				table_name = va_arg(args, const char*);
				break;

			case VH_SP_CTX_SNAME:
				schema_name = va_arg(args, const char*);
				break;

			default:
				vh_sp_pull_unk_arg(rt, args);
				break;
		}
	}

	va_end(args);

	switch (sptd->sp.spret)
	{
		case SPRET_TableDef:
			td = sptd_td(sptd, tc, schema_name, table_name);
			*ret = td ? sptd_table_count(sptd) : -1;

			return td;

		case SPRET_TableDefVer:
			td = sptd_td(sptd, tc, schema_name, table_name);
			*ret = td ? sptd_table_count(sptd) : -1;

			return vh_td_tdv_lead(td);

		default:
			elog(ERROR2,
					emsg("Corrupt sptd at [%p], invalid return type.",
						 sptd));

			break;
	}

	*ret = -1;

	return 0;
}


/*
 * sptd_next
 *
 * We could either be doing a TableDef or a TableDefVer but it really doesn't
 * matter because they both go thru void*.
 */
static void* 
sptd_next(SearchPath sp, int32_t *ret)
{
	sptd_data *sptd = (sptd_data*)sp;
	TableDef *td_head;
	int32_t td_sz;

	if (sptd->iter_idx > -1)
	{
		td_sz = vh_SListIterator(sptd->tables, td_head);

		if (sptd->iter_idx + 1 < td_sz)
		{
			sptd->iter_idx++;
			*ret = td_sz - sptd->iter_idx - 1;
			return td_head[sptd->iter_idx];

		}
	}

	*ret = 0;

	return 0;
}

static int32_t 
sptd_reset(SearchPath sp)
{
	sptd_data *sptd = (sptd_data*)sp;

	if (sptd->tables)
		vh_SListClear(sptd->tables);

	sptd->iter_idx = -1;

	return 0;
}

static void 
sptd_finalize(SearchPath sp)
{
	sptd_data *sptd = (sptd_data*)sp;

	if (sptd->schemas)
	{
		vh_SListDestroy(sptd->schemas);
		sptd->schemas = 0;
	}

	if (sptd->catalogs)
	{
		vh_SListDestroy(sptd->catalogs);
		sptd->catalogs = 0;
	}

	if (sptd->tables)
	{
		vh_SListDestroy(sptd->tables);
		sptd->tables = 0;
	}

	sptd->iter_idx = -1;
}


/*
 * ============================================================================
 * Helper Functions
 * ============================================================================
 */

/*
 * sptd_td
 *
 * Fills the array with TableDef objects if more than one is found thru the
 * established path.  Returns the first priority TableDef object found.
 */
static TableDef 
sptd_td(sptd_data *sptd, TableCatalog tc,
	   	const char *sname, const char *tname)
{
	TableDef tdret = 0, tdl;
	TableCatalog *tc_head, tcl;
	const char *search_tname;
	int32_t i, tc_sz;

	search_tname = tname ? tname : sptd->table_name;

	if (tc)
	{
		tdret = vh_cat_tbl_getbyname(tc, search_tname);	
	}

	/*
	 * Build out the rest of the matching tables so that the next function just
	 * has to read the SList.
	 */
	if (sptd->catalogs)
		tc_sz = vh_SListIterator(sptd->catalogs, tc_head);
	else
		tc_sz = 0;

	for (i = 0; i < tc_sz; i++)
	{
		tcl = tc_head[i];

		if (tcl)
		{
			tdl = vh_cat_tbl_getbyname(tcl, search_tname);

			if (tdret && tdl)
			{
				/*
				 * Check our unique flag, but be careful because we have to
				 * compare what's in the list and our current return value!
				 */

				if ((sptd->unique && sptd_td_unique(sptd, tdl)) &&
					(sptd->unique && tdl != tdret))
				{
					vh_SListPush(sptd->tables, tdl);
				}
				else if (!sptd->unique)
				{
					vh_SListPush(sptd->tables, tdl);
				}
			}
			else if (tdl)
			{
				tdret = tdl;
			}
		}
	}

	return tdret;
}


/*
 * sptd_td_unique
 *
 * Returns true if @td is not already in the tables list.  Helpful when the caller
 * expects only unique TableDef to be returned via next.
 */
static bool 
sptd_td_unique(sptd_data *sptd, TableDef td)
{
	TableDef *td_head, td_i;
	int32_t i, td_sz;

	td_sz = vh_SListIterator(sptd->tables, td_head);

	for (i = 0; i < td_sz; i++)
	{
		td_i = td_head[i];

		if (td_i == td)
		{
			return false;
		}
	}

	return true;
}

