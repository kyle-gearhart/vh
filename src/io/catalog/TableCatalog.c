/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>

#include "vh.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/TableDef.h"
#include "io/utils/htbl.h"


typedef struct TableCatalogData
{
	const char *name;
	MemoryContext mctx;
	HashTable htbl;
} TableCatalogData;


TableCatalog
vh_cat_tbl_create(const char *name)
{
	CatalogContext cc = vh_ctx();
	MemoryContext mctx_old, mctx_tc;
	TableCatalog tc;
	char* buffer;
	HashTableOpts hopts = { };

	if (cc)
	{
		mctx_tc = vh_MemoryPoolCreate(cc->memoryTop,
									  8192,
									  "Table Catalog");
		mctx_old = vh_mctx_switch(mctx_tc);

		tc = vhmalloc(sizeof(struct TableCatalogData) + strlen(name) + 1);
		buffer = (char*) (tc + 1);
		strcpy(buffer, name);

		tc->mctx = mctx_tc;
		
		hopts.key_sz = sizeof(const char*);
		hopts.value_sz = sizeof(BackEnd);
		hopts.func_hash = vh_htbl_hash_str;
		hopts.func_compare = vh_htbl_comp_str;
		hopts.mctx = mctx_tc;
		hopts.is_map = true;

		tc->htbl = vh_htbl_create(&hopts, VH_HTBL_OPT_ALL);

		vh_mctx_switch(mctx_old);

		return tc;
	}
	else
	{
		elog(FATAL,
			 emsg("Catalog context could not be found, unable to create "
				  "a new TableCatalog."));
	}

	return 0;
}

MemoryContext
vh_cat_tbl_mctx(TableCatalog tc)
{
	if (!tc)
	{
		elog(WARNING,
				emsg("Invalid TableCatalog pointer [%p] passed to vh_cat_tbl_mctx."
					 "  Unable to get the MemoryContext as requested.",
					 tc));

		return 0;
	}

	return tc->mctx;
}

/*
 * Just make sure we're in the right context if a valid TableCatalog
 * was passed.  We'll still use the underlying Create function from
 * TableDef.c
 */
TableDef
vh_cat_tbl_createtbl(TableCatalog tc)
{
	MemoryContext mctx_old;
	TableDef td;

	if (tc)
	{
		mctx_old = vh_mctx_switch(tc->mctx);

		td = vh_td_create(false);
		td->tc = tc;

		vh_mctx_switch(mctx_old);
	}
	else
	{
		td = vh_td_create(false);
	}


	return td;
}

bool vh_cat_tbl_add(TableCatalog cat, TableDef table)
{
	TableDef *td;
	int32_t ret;

	if (cat)
	{
		if (table->tname)
		{
			td = vh_htbl_get(cat->htbl, vh_str_buffer(table->tname));

			if (td)
				return false;

			td = vh_htbl_put(cat->htbl, vh_str_buffer(table->tname), &ret);
			assert(ret == 1 || ret == 2);
			*td = table;

			return true;
		}
	}

	return false;
}


void
vh_cat_tbl_remove(TableCatalog tc, TableDef td)
{
	if (!tc)
	{
		elog(WARNING,
				emsg("Invalid TableCatalog pointer [%p] passed to "
					 "vh_cat_tbl_remove.",
					 tc));

		return;
	}

	if (!td)
	{
		elog(WARNING,
				emsg("Invalid TableDef pointer [%p] passed to "
					 "vh_cat_tbl_remove.",
					 td));

		return;
	}

	vh_htbl_del(tc->htbl, vh_str_buffer(td->tname));
}

void
vh_cat_tbl_destroy(TableCatalog tc)
{
	if (tc->mctx)
	{
		vh_mctx_destroy(tc->mctx);
	}
}

TableDef
vh_cat_tbl_getbyname(TableCatalog cat, const char *name)
{
	TableDef *td;
	
	if (!cat)
	{
		elog(WARNING,
				emsg("Invalid TableCatalog pointer [%p] passed to "
					 "vh_cat_tbl_getbyname.  Unable to fetch the table named %s "
					 "as requested.",
					 cat,
					 name));

		return 0;
	}

	if (!name)
	{
		elog(WARNING,
				emsg("Invalid table name pointer [%p] passed to "
					 "vh_cat_tbl_getbyname.  Unable to proceed.",
					 name));

		return 0;
	}

	td = vh_htbl_get(cat->htbl, name);

	return td ? *td : 0;
}

bool
vh_cat_tbl_exists(TableCatalog cat, const char *name)
{
	return (vh_cat_tbl_getbyname(cat, name) != 0);
}

