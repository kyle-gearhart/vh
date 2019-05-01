/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef VH_DATACATALOG_SQL_IMPL_INFOSCHEME_H
#define VH_DATACATALOG_SQL_IMPL_INFOSCHEME_H

/*
 * Forms HeapTupleDef for the ANSI SQL standard for the INFORMATION_SCHEMA
 * relations.  This allows for self configuration of user defined tables.
 *
 * Users would be forced to manually define HeapTupleDef in code, while this
 * facility allows for a backend to be queried for the user schema and then
 * self configure.
 *
 * vh_sqlis_GetTableCatalog returns a TableCatalog specific containing only
 * INFORMATION_SCHEMA HeapTableDef objects.  These can be accessed by name
 * using the TableCatalog functions.
 *
 * vh_sqlis_LoadSchema accesses the shard and fetches data from the
 * INFORMATION_SCHEMA to populate the desired table catalog.
 */

#include "vh.h"
#include "io/catalog/TableCatalog.h"

typedef struct BackEndConnectionData *BackEndConnection;

typedef struct SqlInfoSchemePackage
{
	SList columns;					/* HeapTuplePtr: INFORMATION_SCHEMA.COLUMNS */
	SList table_constraints;		/* HeapTuplePtr: INFORMATION_SCHEMA.TABLE_CONSTRAINTS */
	SList constraint_column_usage;	/* HeapTuplePtr: INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE */
} SqlInfoSchemePackage;

TableCatalog vh_sqlis_GetTableCatalog(void);
TableDef vh_sqlis_td_columns(void);
TableDef vh_sqlis_td_constraintcolumnusage(void);
TableDef vh_sqlis_td_tables(void);
TableDef vh_sqlis_td_tableconstraints(void);


SqlInfoSchemePackage vh_sqlis_fetchschema(BackEndConnection bec, String schema);
SqlInfoSchemePackage vh_sqlis_fetchschemas(BackEndConnection bec, SList schemas);

void vh_sqlis_loadschema(TableCatalog target_catalog, BackEndConnection bec,
						 String schema);
void vh_sqlis_loadschemas(TableCatalog target_catalog, BackEndConnection bec, 
						  SList schemas);

void vh_sqlis_loadshardschema(TableCatalog target_catalog, Shard shd,
							  String schema);
void vh_sqlis_loadshardschemas(TableCatalog target_catalog, Shard shd,
							   SList schemas);


#endif

