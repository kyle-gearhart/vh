/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/utils/btree.h"
#include "test.h"

static btRoot bt = 0;
static btRoot bt2 = 0;
static btRoot bt3 = 0;

static TableDef td_bt = 0;
static TableField tf_bt_key = 0;
static TableField tf_bt_key2 = 0;

static TableDef td_bt3 = 0;
static TableField tf_bt3_key = 0;
static TableField tf_bt3_key2 = 0;

static Type tys_int32[] = { &vh_type_int32, 0 };
//static Type tys_int64[] = { &vh_type_int64, 0 };
static Type tys_String[] = { &vh_type_String, 0 };

static void setup_bt(void);
static void setup_bt2(void);
static void setup_bt3(void);
static void setup_td(void);
static void setup_td3(void);

static HeapTuplePtr create_data(int32_t val);
static HeapTuplePtr create_data2(int32_t val1, int32_t val2);
static HeapTuplePtr create_data3(int32_t val1, const char *val2);

static void populate_bt(void);
static void populate_bt2(void);
static void populate_bt3(void);

static void scan_bt(void);

typedef struct btTestVal16Data *btTestVal16;

struct btTestVal16Data
{
	int32_t a;
	int16_t b;
	char c[10];
};

void
test_bt_entry(void)
{
	/*
	 * We test in several ways.  First, we make sure that we occupy a lot of
	 * space on each page.  We accomplish this by setting the value sz on the
	 * leaf pages to a very large value.  By doing this, we test the splits and
	 * all the mess they make.
	 *
	 * Then we gracefully drop into more and more complex key column scenarios.
	 * 		1)	Single int32 key
	 * 		2)	Dual int32 key
	 * 		3)	int32, String key
	 *
	 * To accurately test scenario #3, we have to have a way to iterate the tree,
	 * so we use a more advanced scankey technique to pop the leaf node values
	 * out in a callback routine.
	 */
	setup_td();
	setup_bt();
	populate_bt();

	setup_bt2();
	populate_bt2();

	setup_td3();
	setup_bt3();
	populate_bt3();

	scan_bt();
}

static void
setup_bt(void)
{
	bt = vh_bt_create(vh_mctx_current(), true);
	vh_bt_add_column(bt, vh_td_tdv_lead(td_bt), tf_bt_key, false); 
}

static void
setup_bt2(void)
{
	bt2 = vh_bt_create(vh_mctx_current(), true);
	vh_bt_add_column(bt2, vh_td_tdv_lead(td_bt), tf_bt_key, false);
	vh_bt_add_column(bt2, vh_td_tdv_lead(td_bt), tf_bt_key2, false);
}

static void
setup_bt3(void)
{
	bt3 = vh_bt_create(vh_mctx_current(), true);
	vh_bt_add_column(bt3, vh_td_tdv_lead(td_bt3), tf_bt3_key, false);
	vh_bt_add_column(bt3, vh_td_tdv_lead(td_bt3), tf_bt3_key2, false);
}


static void
setup_td(void)
{
	td_bt = vh_td_create(false);
	tf_bt_key = vh_td_tf_add(td_bt, tys_int32, "id");
	tf_bt_key2 = vh_td_tf_add(td_bt, tys_int32, "id2");
}

static void
setup_td3(void)
{
	td_bt3 = vh_td_create(false);
	tf_bt3_key = vh_td_tf_add(td_bt3, tys_int32, "id");
	tf_bt3_key2 = vh_td_tf_add(td_bt3, tys_String, "id2");
}

static HeapTuplePtr
create_data(int32_t val)
{
	HeapTuple ht;
	HeapTuplePtr htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general),
									 &vh_td_tdv_lead(td_bt)->heap,
									 &ht);
	int32_t *htp_val;

	if (htp)
	{
		htp_val = vh_ht_GetInt32Nm(ht, "id");
		*htp_val = val;
		vh_htf_clearnull(ht, &tf_bt_key->heap); 

		return htp;
	}

	return 0;

}

static HeapTuplePtr
create_data2(int32_t val1, int32_t val2)
{
	HeapTuple ht;
	HeapTuplePtr htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general),
									 &vh_td_tdv_lead(td_bt)->heap,
									 &ht);
	int32_t *htp_val;

	if (htp)
	{
		htp_val = vh_ht_GetInt32Nm(ht, "id");
		*htp_val = val1;
		vh_htf_clearnull(ht, &tf_bt_key->heap); 

		htp_val = vh_ht_GetInt32Nm(ht, "id2");
		*htp_val = val2;
		vh_htf_clearnull(ht, &tf_bt_key2->heap);

		return htp;
	}

	return 0;
}

static HeapTuplePtr
create_data3(int32_t val1, const char *val2)
{
	HeapTuple ht;
	HeapTuplePtr htp;
	int32_t *htp_val;
	String htp_sval;

	htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general),
						&vh_td_tdv_lead(td_bt3)->heap,
						&ht);

	if (htp)
	{
		htp_val = vh_ht_GetInt32Nm(ht, "id");
		*htp_val = val1;
		vh_htf_clearnull(ht, &tf_bt3_key->heap);

		htp_sval = vh_ht_GetStringNm(ht, "id2");
		vh_str.Assign(htp_sval, val2);
		vh_htf_clearnull(ht, &tf_bt3_key2->heap);

		return htp;
	}

	return 0;
}

static void
populate_bt(void)
{
	HeapTuplePtr htp, lookup, del1, del2;
	bool res;
	void *val;
	btTestVal16 tval;

	htp = create_data(1);

	res = vh_bt_insert_htp(bt, htp, &val);
	assert(res);

	htp = create_data(0);
	res = vh_bt_insert_htp(bt, htp, &val);

	htp = create_data(10);
	res = vh_bt_insert_htp(bt, htp, &val);

	lookup = htp = create_data(5);
	res = vh_bt_insert_htp(bt, htp, &val);

	vh_bt_print_tree(bt);
	
	tval = val;
	tval->a = 19;
	tval->b = -10;
	tval->c[0] = 'a';
	tval->c[1] = 'c';
	tval->c[2] = '\0';

	vh_bt_print_tree(bt);

	res = vh_bt_find_htp(bt, htp, &val);
	tval = val;
	assert(tval->a == 19);

	htp = create_data(7);
	res = vh_bt_insert_htp(bt, htp, &val);

	vh_bt_print_tree(bt);

	del1 = htp = create_data(3);
	res = vh_bt_insert_htp(bt, htp, &val);

	vh_bt_print_tree(bt);
	
	printf("\ninsert 4\n");
	del2 = htp = create_data(4);
	res = vh_bt_insert_htp(bt, htp, &val);

	vh_bt_print_tree(bt);
	
	res = vh_bt_find_htp(bt, lookup, &val);
	tval = val;
	assert(tval->a == 19);
	assert(tval->b == -10);

	printf("\ninsert 15");	
	htp = create_data(15);
	res = vh_bt_insert_htp(bt, htp, &val);
	vh_bt_print_tree(bt);

	printf("\ninsert 20");	
	htp = create_data(20);
	res = vh_bt_insert_htp(bt, htp, &val);
	vh_bt_print_tree(bt);
	
	printf("\ninsert 9");	
	htp = create_data(9);
	res = vh_bt_insert_htp(bt, htp, &val);
	vh_bt_print_tree(bt);
	
	printf("\ninsert 8");	
	htp = create_data(8);
	res = vh_bt_insert_htp(bt, htp, &val);
	vh_bt_print_tree(bt);
	
	printf("\ninsert 6");	
	htp = create_data(6);
	res = vh_bt_insert_htp(bt, htp, &val);
	vh_bt_print_tree(bt);

	res = vh_bt_find_htp(bt, lookup, &val);
	tval = val;
	assert(tval->a == 19);
	assert(tval->b == -10);
	
	printf("\ninsert -2");	
	htp = create_data(-2);
	res = vh_bt_insert_htp(bt, htp, &val);
	vh_bt_print_tree(bt);

	res = vh_bt_delete_htp(bt, del1);
	vh_bt_print_tree(bt);

	res = vh_bt_delete_htp(bt, del2);
	vh_bt_print_tree(bt);

	res = vh_bt_find_htp(bt, del1, &val);
	assert(!res);

	res = vh_bt_find_htp(bt, del2, &val);
	assert(!res);

	res = vh_bt_insert_htp(bt, del1, &val);
	res = vh_bt_insert_htp(bt, del2, &val);

	vh_bt_print_tree(bt);

	res = vh_bt_trim(bt, 5, false);

	vh_bt_print_tree(bt);

	htp = create_data(100);
	vh_bt_insert_htp(bt, htp, &val);
	vh_bt_print_tree(bt);
}

static void
populate_bt2(void)
{
	HeapTuplePtr htp, lookup;
	void *val;
	btTestVal16 tval;
	bool res;

	htp = create_data2(1, 0);

	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);
	assert(res);

	htp = create_data2(1, 5);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);

	htp = create_data2(1, 3);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);
	
	htp = create_data2(4, 3);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);

	/* 5th Insert */
	printf("\nadd 5, 1\n");
	lookup = htp = create_data2(5, 1);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);

	tval = val;
	tval->a = 19;
	tval->b = -10;
	tval->c[0] = 'a';
	tval->c[1] = 'c';
	tval->c[2] = '\0';

	
	res = vh_bt_find_htp(bt2, lookup, &val);
	vh_bt_print_tree(bt2);
	tval = val;

	assert(tval->a == 19);
	assert(tval->b == -10);
	assert(tval->c[0] = 'a');


	htp = create_data2(5, 0);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);


	res = vh_bt_find_htp(bt2, lookup, &val);
	vh_bt_print_tree(bt2);
	tval = val;

	assert(tval->a == 19);
	assert(tval->b == -10);
	assert(tval->c[0] = 'a');

	htp = create_data2(5, 10);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);

	res = vh_bt_find_htp(bt2, lookup, &val);
	vh_bt_print_tree(bt2);
	tval = val;

	assert(tval->a == 19);
	assert(tval->b == -10);
	assert(tval->c[0] = 'a');

	htp = create_data2(5, 3);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);

	/* 9th Insert */
	htp = create_data2(5, 8);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);

	res = vh_bt_find_htp(bt2, lookup, &val);
	vh_bt_print_tree(bt2);
	tval = val;

	assert(tval->a == 19);
	assert(tval->b == -10);
	assert(tval->c[0] = 'a');

	htp = create_data2(1, 4);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);

	htp = create_data2(1, 7);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);
	
	htp = create_data2(0, 0);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);
	
	htp = create_data2(4, 4);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);
	
	htp = create_data2(4, 5);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);
	
	htp = create_data2(4, 7);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);
	
	htp = create_data2(4, 8);
	res = vh_bt_insert_htp(bt2, htp, &val);
	vh_bt_print_tree(bt2);
}

static void
populate_bt3(void)
{
	HeapTuplePtr htp, lookup;
	void *val;
	btTestVal16 tval;
	bool res;

	htp = create_data3(1, "Kyle Gearhart");
	res = vh_bt_insert_htp(bt3, htp, &val);
	assert(res);

	lookup = htp = create_data3(1, "John Smith");
	res = vh_bt_insert_htp(bt3, htp, &val);
	assert(res);
	
	tval = val;
	tval->a = 22;
	tval->b = -30;

	htp = create_data3(1, "Amanda Smith");
	res = vh_bt_insert_htp(bt3, htp, &val);
	assert(res);
	vh_bt_print_tree(bt3);

	res = vh_bt_find_htp(bt3, lookup, &val);
	assert(res);

	tval = val;

	assert(tval->a == 22);
	assert(tval->b == -30);	

	htp = create_data3(1, "Thomas Jefferson");
	res = vh_bt_insert_htp(bt3, htp, &val);
	assert(res);
	vh_bt_print_tree(bt3);

	htp = create_data3(1, "Xavier Dye");
	res = vh_bt_insert_htp(bt3, htp, &val);
	assert(res);
	vh_bt_print_tree(bt3);
	
	htp = create_data3(1, "April Smith");
	res = vh_bt_insert_htp(bt3, htp, &val);
	assert(res);
	vh_bt_print_tree(bt3);
	
	htp = create_data3(1, "Ashley Poppyhead");
	res = vh_bt_insert_htp(bt3, htp, &val);
	assert(res);

	res = vh_bt_find_htp(bt3, lookup, &val);
	assert(res);

	tval = val;
	assert(tval->a == 22);
	assert(tval->b == -30);
	
	htp = create_data3(0, "Ashley Poppyhead");
	res = vh_bt_insert_htp(bt3, htp, &val);
	assert(res);
	vh_bt_print_tree(bt3);
}


static void
scan_bt(void)
{
	btScan scan;
	struct btScanKeyData *skey;
	int32_t count = 0;

	skey = vhmalloc(sizeof(struct btScanKeyData));
	skey->col_no = 0;
	skey->oper = VH_BT_OPER_LT;
	vh_tvs_init(&skey->tvs);
	vh_tvs_store_i32(&skey->tvs, 5);

	scan = vh_bt_scan_begin(bt, 1);

	if (vh_bt_scan_first(scan, skey, 1, true))
	{
		count++;

		while (vh_bt_scan_next(scan, true))
		{
			count++;
		}
	}

	vh_bt_scan_end(scan);

	assert(count == 5);
	printf("\nscan_bt count: %d\n", count);

	skey->oper = VH_BT_OPER_GTEQ;

	scan = vh_bt_scan_begin(bt, 1);

	if (vh_bt_scan_first(scan, skey, 1, true))
	{
		count = 1;

		while (vh_bt_scan_next(scan, true))
		{
			count++;
		}
	}

	vh_bt_scan_end(scan);

	printf("\nbt_scan count %d\n", count);
}

