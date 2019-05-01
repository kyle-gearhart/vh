/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_TableCatalog_H
#define vh_datacatalog_TableCatalog_H

typedef struct TableCatalogData *TableCatalog;

TableCatalog vh_cat_tbl_create(const char* name);
void vh_cat_tbl_destroy(TableCatalog);

MemoryContext vh_cat_tbl_mctx(TableCatalog);

bool vh_cat_tbl_add(TableCatalog, TableDef);
TableDef vh_cat_tbl_createtbl(TableCatalog);
void vh_cat_tbl_remove(TableCatalog, TableDef);

bool vh_cat_tbl_exists(TableCatalog, const char*);
TableDef vh_cat_tbl_getbyname(TableCatalog, const char*);

#endif

