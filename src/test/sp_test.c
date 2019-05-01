/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/Type.h"
#include "io/catalog/sp/spht.h"
#include "io/catalog/sp/sptd.h"


static TableCatalog tc = 0, tc2 = 0;
static TableDef td = 0;
static Type tys_string[] = { &vh_type_String, 0 };

/*
 * Search Path Testing Module
 */

static void test_sp_spawntd(void);
static void test_sp_ht_tf(void);

static void test_sp_tc_td(void);

void
test_sp_entry(void)
{
	/* Setup */
	test_sp_spawntd();

	/* SearchPath HeapTuple */
	test_sp_ht_tf();

	/* SearchPath TableDef */
	test_sp_tc_td();
}


/*
 * test_sp_spawntd
 *
 * Creates a TableDef to test the SearchPaths with.
 */
static void
test_sp_spawntd(void)
{
	tc = vh_cat_tbl_create("test SearchPath");
	td = vh_td_create(false);
	td->tname = vh_str.Convert("test_sp_td");
	
	vh_td_tf_add(td, tys_string, "first_name");
   	vh_td_tf_add(td, tys_string, "last_name");

	vh_cat_tbl_add(tc, td);

	tc2 = vh_cat_tbl_create("test SearchPath2");
	vh_cat_tbl_add(tc2, td);	
}

/*
 * test_sp_ht_tf
 *
 * Search path for HeapTuple with three return values and multiple execution
 * contexts.
 */
static void
test_sp_ht_tf(void)
{
	SearchPath sp1 = vh_spht_tf_create("first_name");
	SearchPath sp2 = vh_spht_tf_create("Last_name");
	SearchPath sp3 = vh_spht_tf_create("first_name");
	TableField tf = 0;
	HeapTuple ht;
	void *dat;
	int32_t ret;

	tf = vh_sp_search(sp1, &ret, 1, VH_SP_CTX_TD, td);
	assert(ret == 1);
	assert(tf);

	tf = vh_sp_search(sp2, &ret, 1, VH_SP_CTX_TD, td);
	assert(ret < 1);
	assert(!tf);

	tf = vh_sp_search(sp2, &ret, 2, VH_SP_CTX_TD, td, VH_SP_CTX_FNAME, "last_name");
	assert(ret == 1);
	assert(tf);

	tf = vh_sp_search(sp2, &ret, 2, VH_SP_CTX_FNAME, "last_name", VH_SP_CTX_TD, td);
	assert(ret == 1);
	assert(tf);

	/*
	 * Create a HeapTuple and then try to find the fields with only it.
	 */
	ht = vh_ht_create((HeapTupleDef)vh_td_tdv_lead(td));

	tf = vh_sp_search(sp1, &ret, 1, VH_SP_CTX_HT, ht);
	assert(ret == 1);
	assert(tf);

	tf = vh_sp_search(sp2, &ret, 1, VH_SP_CTX_HT, ht, VH_SP_CTX_FNAME, "last_name");
	assert(ret == 1);
	assert(tf);

	dat = vh_sp_search(sp3, &ret, 1, VH_SP_CTX_HT, ht);
	assert(ret == 1);
	assert(dat);
}

static void 
test_sp_tc_td(void)
{
	SearchPathTableDefOpts opts;
	SearchPath sp1;
	TableDef td;
	int32_t ret;

	sp1 = vh_sptd_create(&opts, VH_SPTD_OPT_EMPTY);
	
	td = vh_sp_search(sp1, &ret, 2, 
					  VH_SP_CTX_TC, tc,
					  VH_SP_CTX_TNAME, "test_sp_tc_td");
	assert(ret == -1);
	assert(!td);

	td = vh_sp_search(sp1, &ret, 2,
					  VH_SP_CTX_TC, tc,
					  VH_SP_CTX_TNAME, "test_sp_td");
	assert(ret == 0);
	assert(td);

	/* Test the finalize implementation */
	vh_sp_destroy(sp1);

	opts.table_name = "test_sp_td";
	opts.tc = tc;

	sp1 = vh_sptd_create(&opts, VH_SPTD_OPT_TNAME | VH_SPTD_OPT_TC);
	td = vh_sp_search(sp1, &ret, 0);
	assert(ret == 0);
	assert(td);

	/*
	 * Add another catalog with the same table in it, since we didn't specify a
	 * unique qualification, we should get back the same table twice.
	 */
	vh_sptd_tc_add(sp1, tc2);
	td = vh_sp_search(sp1, &ret, 0);
	assert(ret == 1);
	assert(td);
	vh_sp_destroy(sp1);

	/*
	 * Since the same TableDef is added into two catalogs and we're requesting 
	 * unique TableDef's, only one should come back.
	 */
	opts.unique = true;

	sp1 = vh_sptd_create(&opts, VH_SPTD_OPT_TNAME | VH_SPTD_OPT_TC | VH_SPTD_OPT_UNIQUE);
	vh_sptd_tc_add(sp1, tc2);
	td = vh_sp_search(sp1, &ret, 0);
	assert(ret == 0);
	assert(td);
}

