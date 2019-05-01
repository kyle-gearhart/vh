/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_utils_btree_H
#define vh_io_utils_btree_H

/*
 * BTree implementation is intended to store tuples in order.  We use a
 * caching technique to void copying key values into the tree, instead
 * referring to indexes in an SList of HeapTuplePtr.
 */

#include "io/catalog/TypeVarSlot.h"

typedef struct btRootData *btRoot;
typedef struct btScanData *btScan;
typedef struct btScanKeyData *btScanKey;

#define VH_BT_OPER_LT		0x00
#define VH_BT_OPER_LTEQ		0x01
#define VH_BT_OPER_EQ 		0x02
#define VH_BT_OPER_NEQ		0x03
#define VH_BT_OPER_GTEQ		0x04
#define VH_BT_OPER_GT 		0x05

#define VH_BT_OPER_MAX		6

struct btScanKeyData
{
	TypeVarSlot tvs;			/* Compare value from the caller */
	TypeVarOpExec tvope;		/* Scan comparison function (LHS: index, RHS: tvs) */
	int32_t col_no;
	int32_t sk_flags;
	int8_t oper;
};

btRoot vh_bt_create(MemoryContext mctx, bool unique);
void vh_bt_destroy(btRoot root);

bool vh_bt_add_column(btRoot root, TableDefVer tdv,
					  TableField tf, bool allow_nulls);
bool vh_bt_add_column_tys(btRoot root, Type *tys, bool allow_nulls);

/*
 * TypeVarSlot Operations
 */
bool vh_bt_find_tvs(btRoot root, TypeVarSlot **datas, int32_t n_datas,
					void **value);
bool vh_bt_insert_tvs(btRoot root, TypeVarSlot **datas, int32_t n_datas,
					  void **value);
bool vh_bt_delete_tvs(btRoot root, TypeVarSlot **datas, int32_t n_datas);


/*
 * HeapTuplePtr Operations
 */
bool vh_bt_find_htp(btRoot root, HeapTuplePtr htp, void **value);
bool vh_bt_insert_htp(btRoot root, HeapTuplePtr htp, void **value);
bool vh_bt_delete_htp(btRoot root, HeapTuplePtr htp);

/*
 * HeapTuple Operations
 */
bool vh_bt_find_ht(btRoot root,
				   HeapTuple ht,
				   HeapTuplePtr htp,
				   void **value);
bool vh_bt_insert_ht(btRoot root,
  					 HeapTuple ht,
	   				 HeapTuplePtr htp,
   					 void **value);

/*
 * Trim Operations
 */
bool vh_bt_trim(struct btRootData *root, int32_t entries, bool forward);


/*
 * Array Operations
 */

typedef bool (*vh_bt_array_iter_cb)(void *data, int32_t index, void *user);

int32_t vh_bt_array_create(btRoot root, btScanKey *sks, int32_t n_sks, size_t value_sz);
int32_t vh_bt_array_push(btRoot root, btScanKey *sks, int32_t n_sks, void *value);
int32_t vh_bt_array_iterate(btRoot root, btScanKey *sks, int32_t n_sks,
							vh_bt_array_iter_cb cb, void *user);

/*
 * Scan Operations
 */

btScan vh_bt_scan_begin(struct btRootData *root, int32_t nskeys);
bool vh_bt_scan_first(struct btScanData *scan, struct btScanKeyData *skeys,
					  int32_t nskeys, bool forward);
bool vh_bt_scan_next(struct btScanData *scan, bool forward);
void vh_bt_scan_get(struct btScanData *scan, TypeVarSlot **keys, void **value);
void vh_bt_scan_end(struct btScanData *scan);


void vh_bt_print_tree(btRoot root);

#endif


