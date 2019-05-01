/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_test_H
#define vh_test_H

#include <stdio.h>


/*
 * Defines indivdual testing module launch points.  Each launch point is located
 * in the corresponding .c file.
 */

void test_new_json_entry(void);
void test_bt_entry(void);
void test_config_entry(void);
void test_hashtable(void);
void test_operators(void);
void test_memory(void);
void test_ml_entry(void);
void test_nest_entry(void);
void test_sp_entry(void);
void test_pc_entry(void);
void test_query(void);
void test_types(void);
void test_be_sqlite3(void);
void test_typevar_entry(void);
void test_typevaracm_entry(void);

void test_grdb_entry(void);

extern CatalogContext ctx_catalog;

#endif

