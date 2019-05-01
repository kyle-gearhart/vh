/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_sp_sptd_h
#define vh_catalog_sp_sptd_h

#include "io/catalog/searchpath.h"

/*
 * SearchPath for TableDef
 *
 * Traverses TableCatalog for a set of TableDef or TableDefVer.  Generally only
 * accepts one table name.
 *
 * Supported return types:
 * 		SPRET_TableDef
 * 		SPRET_TableDefVer
 */

typedef struct SearchPathTableDefOpts SearchPathTableDefOpts;

struct SearchPathTableDefOpts
{
 	const char *schema_name;
	const char *table_name;
	TableCatalog tc;
	int32_t min_version;
	int32_t max_version;
	int32_t exact_version;

	bool unique;
};


/*
 * SearchPath for TableDef
 *
 * We can attach to multiple table catalogs and schemas to search for a single
 * TableDef.  The only runtime arguments available are the table name and 
 * version.
 *
 * Runtime arguments are checked first followed by paths populated via 
 * vh_sptd_create, vh_sptd_schema_add, vh_sptd_tc_add.  By default, paths stored
 * in the SearchPath argument are checked in a FIFO (first in, first out).  You
 * may request LIFO (last in, first out) by using SearchPathTableDefOpts.lifo 
 * option when the SearchPath is created.
 *
 * Supported run time contexts:
 *		<> VH_SP_CTX_TC				TableCatalog
 *		<> VH_SP_CTX_SNAME			Schema name
 *		<> VH_SP_CTX_TNAME			Table name
 */

#define VH_SPTD_OPT_EMPTY			0x00
#define VH_SPTD_OPT_SNAME			0x01
#define VH_SPTD_OPT_TNAME			0x02
#define VH_SPTD_OPT_TC				0x04
#define VH_SPTD_OPT_UNIQUE			0x08
#define VH_SPTD_OPT_MINVER			0x10
#define VH_SPTD_OPT_MAXVER			0x20
#define VH_SPTD_OPT_EVER			0x40

SearchPath vh_sptd_create(SearchPathTableDefOpts *opts, int32_t flags);
SearchPath vh_sptd_default(void);

void vh_sptd_schema_add(SearchPath sp, const char *schema);
void vh_sptd_tc_add(SearchPath sp, TableCatalog tc);



/* 
 * Search Path for TableDefVer 
 */
SearchPath vh_sptdv_create(SearchPathTableDefOpts *opts, int32_t flags);
SearchPath vh_sptdv_default(void);

#define vh_sptdv_schema_add(sp, schema)		vh_sptd_schema_add((sp), (schema))
#define vh_sptdv_tc_add(sp, tc)				vh_sptd_tc_add((sp), (tc))



#endif

