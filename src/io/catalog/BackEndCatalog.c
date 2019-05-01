/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/BackEndCatalog.h"
#include "io/utils/htbl.h"

struct BackEndCatalogData
{
	MemoryContext mctx;
	HashTable htbl;
};

struct BackEndIdSearch
{
	BackEnd be;
	int32_t id;
};

static bool cat_be_search_id(HashTable htbl, const void *key, void *entry, 
							 void *dat); 

BackEndCatalog
vh_cat_be_create(void)
{
	CatalogContext cc = vh_ctx();
	MemoryContext mctx_old, mctx_bec;
	BackEndCatalog bec;
	HashTableOpts hopts = { };

	if (cc)
	{
		mctx_bec = vh_MemoryPoolCreate(cc->memoryTop,
									   8192,
									   "Back End Catalog");
		mctx_old = vh_mctx_switch(mctx_bec);
		
		bec = vhmalloc(sizeof(struct BackEndCatalogData));
		memset(bec, sizeof(struct BackEndCatalogData), 0);
		
		bec->mctx = mctx_bec;

		hopts.key_sz = sizeof(const char*);
		hopts.value_sz = sizeof(BackEnd);
		hopts.func_hash = vh_htbl_hash_str;
		hopts.func_compare = vh_htbl_comp_str;
		hopts.mctx = mctx_bec;
		hopts.is_map = true;

		bec->htbl = vh_htbl_create(&hopts, VH_HTBL_OPT_ALL);


		vh_mctx_switch(mctx_old);

		return bec;
	}
	else
	{
		elog(FATAL,
			 emsg("Back End Catalog creation failed, unable to locate "
				  "CatalogContext!"));
	}

	return 0;
}

void
vh_cat_be_destroy(BackEndCatalog bec)
{
	if (bec)
	{
		vh_mctx_destroy(bec->mctx);
	}
}

bool 
vh_cat_be_add(BackEndCatalog bec, BackEnd be)
{
	BackEnd *bep;
	int32_t ret;

	if (!bec)
	{
		elog(WARNING,
				emsg("Invalid BackEndCatalog pointer [%p] passed to "
					 "vh_cat_be_add.  Unable to add the BackEnd [%p] "
					 "as requested.",
					 bec,
					 be));

		return false;
	}

	if (!be)
	{
		elog(WARNING,
				emsg("Invalid BackEnd pointer [%p] passed to "
					 "vh_cat_be_add.  Unable to add the BackEnd [%p] "
					 "as requested.",
					 be));

		return false;
	}

	bep = vh_htbl_get(bec->htbl, be->name);
	
	if (!bep)
	{
		bep = vh_htbl_put(bec->htbl, be->name, &ret);
		assert(ret == 1 || ret == 2);

		*bep = be;
	}
	else
	{
		elog(ERROR2,
			 emsg("Backend %s already exists in the catalog!",
					  be->name));

		return false;
	}

	return true;
}

BackEnd 
vh_cat_be_getbyname(BackEndCatalog bec, const char *name)
{
	BackEnd *be;

	be = vh_htbl_get(bec->htbl, name);

	return be ? *be : 0;
}

/*
 * vh_cat_be_getbyid
 *
 * Just iterate thru the catalog until we find the identifier we want.
 */
BackEnd 
vh_cat_be_getbyid(BackEndCatalog bec, int32_t id)
{
	struct BackEndIdSearch search = { };

	search.id = id;

	vh_htbl_iterate_map(bec->htbl, cat_be_search_id, &search);

	return search.be;
}

static bool 
cat_be_search_id(HashTable htbl, const void *key, void *entry, 
				 void *dat)
{
	BackEnd be = entry;
	struct BackEndIdSearch *search = dat;

	if (search->id == be->id)
	{
		search->be = be;

		return false;
	}

	return true;
}

