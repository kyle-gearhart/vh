/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/sql/InfoScheme.h"
#include "io/buffer/BuffMgr.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/executor/exec.h"
#include "io/nodes/NodeJoin.h"
#include "io/nodes/NodeQual.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/shard/Shard.h"
#include "io/utils/kvmap.h"
#include "io/utils/SList.h"


#define FIELD_ADD(td, ty, nm) ( { 												\
								Type tys[VH_TAMS_MAX_DEPTH];					\
								tys[0] = &vh_type_##ty ;						\
								tys[1] = 0;										\
								vh_td_tf_add((td), &tys[0], nm); 				\
							} )

static TableCatalog catis = 0;

static void ConfigureInfoSchema(TableCatalog);
static void CIS_Columns(TableCatalog);
static void CIS_ConstraintColumnUsage(TableCatalog);
static void CIS_Tables(TableCatalog);
static void CIS_TableConstraints(TableCatalog);

static NodeQuery FormColumnsQuery(SList schemas);
static NodeQuery FormConstraintColumnUsageQuery(SList schemas);
static NodeQuery FormTableConstraintsQuery(SList schemas);

typedef struct InfoTableData
{
	/*
	 * process_info_package
	 */
	String sname;
	String tname;
	String table_ident;	/* Convience string, sname + . + tname */
	SList columns;		/* HeapTuplePtr: INFORMATION_SCHEMA.COLUMNS */

	/*
	 * process_constraints
	 */
	HeapTuplePtr constraint_primarykey;	/* INFORMATION_SCHEMA.TABLE_CONSTRAINTS */
	SList constraint_foreignkeys;		/* INFORMATION_SCHEMA.TABLE_CONSTRAINTS */

	/*
	 * process_constraint_column_usage
	 */
	SList columns_primarykey;			/* INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE */

	/*
	 * generate_tabledef
	 */
	TableDef td;
} *InfoTable;

static void process_info_package(TableCatalog target_tc, 
								 SqlInfoSchemePackage pack, 
								 BackEnd be);

static void process_table_constraints(KeyValueMap htbl, SList contraints);
static void process_constraint_column_usage(KeyValueMap htbl, SList usage);

static void iterate_infotables(KeyValueMap htbl,
							   void (*cb)(InfoTable, void*),
							   void *cb_data);

static void add_to_target_catalog(InfoTable it, void *data);
static void generate_table_def(InfoTable it, void *data);
static void set_primary_key(InfoTable it, void *data);
static void finalize_info_table(InfoTable it, void *data);


TableDef
vh_sqlis_td_columns(void)
{
	TableCatalog tc = vh_sqlis_GetTableCatalog();

	return vh_cat_tbl_getbyname(tc, "columns");
}

TableDef
vh_sqlis_td_constraintcolumnusage(void)
{
	TableCatalog tc = vh_sqlis_GetTableCatalog();

	return vh_cat_tbl_getbyname(tc, "constraint_column_usage");
}

TableDef
vh_sqlis_td_tableconstraints(void)
{
	TableCatalog tc = vh_sqlis_GetTableCatalog();

	return vh_cat_tbl_getbyname(tc, "table_constraints");
}

TableCatalog
vh_sqlis_GetTableCatalog(void)
{
	MemoryContext old, top;

	if (catis)
		return catis;

	top = vh_mctx_top();
	old = vh_mctx_switch(top);

	catis = vh_cat_tbl_create("INFORMATION_SCHEMA");

	if (catis)
		ConfigureInfoSchema(catis);

	vh_mctx_switch(old);

	return catis;
}

void
vh_sqlis_loadshardschema(TableCatalog target_catalog, Shard shd,
						 String schema)
{
	ConnectionCatalog cc = vh_ctx()->catalogConnection;
	BackEndConnection bec;

	bec = vh_ConnectionGet(cc, shd->access[0]);
	vh_sqlis_loadschema(target_catalog, bec, schema);
	vh_ConnectionReturn(cc, bec);
}

void
vh_sqlis_loadshardschemas(TableCatalog target_catalog, Shard shd,
						  SList schemas)
{
	ConnectionCatalog cc = vh_ctx()->catalogConnection;
	BackEndConnection bec;

	bec = vh_ConnectionGet(cc, shd->access[0]);
	vh_sqlis_loadschemas(target_catalog, bec, schemas);
	vh_ConnectionReturn(cc, bec);
}

void
vh_sqlis_loadschema(TableCatalog target_catalog,
					BackEndConnection bec,
					String schema)
{
	SList schemas;
   
	if (schema && vh_strlen(schema) > 0)
	{
		schemas	= vh_SListCreate();

		vh_SListPush(schemas, schema);
		vh_sqlis_loadschemas(target_catalog, bec, schemas);
		vh_SListDestroy(schemas);
	}
	else
	{
		vh_sqlis_loadschemas(target_catalog, bec, 0);
	}
}

void
vh_sqlis_loadschemas(TableCatalog target_catalog,
		 			 BackEndConnection bec,
					 SList schemas)
{
	MemoryContext mold, mworking;
	NodeQuery nquery;
	ExecResult eresult;
	BackEnd be;
	vh_beat_schema_get schema_get;
	SqlInfoSchemeContextData be_info = { };
	SqlInfoSchemePackage info_package;	
	HeapBufferNo hbno;
	PlannerOpts popts = { };

	mworking = vh_MemoryPoolCreate(vh_mctx_current(),
								   8192, 
								   "INFORMATION_SCHEMA Working");

	/*
	 * Open our own temporary HeapBuffer to store all this in.  Go ahead
	 * and swap to the current context.  We open a HeapBuffer here to avoid
	 *
	 */
	hbno = vh_hb_open(mworking);
	mold = vh_mctx_switch(mworking);

	be = bec->be;
	assert(be);

	if (vh_be_has_schema_op(be))
	{
		schema_get = be->at.schemaget;
		
		be_info.hbno = hbno;
		be_info.schemas = schemas;
		
		info_package = schema_get(bec, &be_info); 
	}
	else
	{
		/*
		 * Setup our planner options to use our locally opened HeapBuffer
		 * and the BackEndConnection given to us.  We only have to do this
		 * once since PlannerOpts is copied in the stack.
		 */
		popts.bec = bec;
		popts.hbno = hbno;

		nquery = FormColumnsQuery(schemas);
		eresult = vh_exec_node_opts(&nquery->node, popts);

		if (eresult && eresult->tups)
		{
			info_package.columns = eresult->tups;
		}

		vh_exec_result_finalize(eresult, true);

		nquery = FormTableConstraintsQuery(schemas);
		eresult = vh_exec_node_opts(&nquery->node, popts);

		if (eresult && eresult->tups)
		{
			info_package.table_constraints = eresult->tups;
		}

		vh_exec_result_finalize(eresult, true);

		nquery = FormConstraintColumnUsageQuery(schemas);
		eresult = vh_exec_node_opts(&nquery->node, popts);

		if (eresult && eresult->tups)
		{
			info_package.constraint_column_usage = eresult->tups;
		}

		vh_exec_result_finalize(eresult, true);
	}

	/*
	 * Process our information package.
	 */

	VH_TRY();
	{
		process_info_package(target_catalog, info_package, be);
	}
	VH_CATCH();
	{
		/*
		 * Let's kick up an error message and then shutdown the operation.
		 *
		 * The schema load failed.
		 */
	}
	VH_ENDTRY();
	

	/*
	 * Close out our temporary HeapBuffer we opened.
	 */
	vh_hb_printstats(vh_hb(hbno));
	vh_hb_close(hbno);

	vh_mctx_switch(mold);
	vh_mctx_destroy(mworking);
}

/*
 * process_info_package
 *
 * First we'll run thru the COLUMNS, building a hash table with the schema +
 * table name as the key, adding each of the columns to the entry. We have to be
 * careful to make sure we include the schema name.
 *
 * Then we'll process CONSTRAINTS and CONSTRAINT_COLUMN_USAGE.
 */
static void
process_info_package(TableCatalog target_catalog,
					 SqlInfoSchemePackage package,
					 BackEnd be)
{
	KeyValueMap htbl;
	HeapTuplePtr *htp_head, htp;
	String hash_key;
	uint32_t i, htp_sz;
	char *buffer;
	InfoTable infotable;
	MemoryContext mctx_old;

	hash_key = vh_str.Create();
	htbl = vh_kvmap_create_impl(sizeof(uintptr_t),
								sizeof(struct InfoTableData),
								vh_htbl_hash_str,
								vh_htbl_comp_str,
								vh_mctx_current());
	
	if (!package.columns)
		elog(ERROR2,
			 emsg("No columns could be found for the schema(s) requested "
				  "by the caller."));

	htp_sz = vh_SListIterator(package.columns, htp_head);
	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];

		if (!vh_IsNullNm(htp, "table_schema") &&
			!vh_IsNullNm(htp, "table_name"))
		{
			vh_str.AssignStr(hash_key, vh_GetStringNm(htp, "table_schema"));
			vh_str.AppendStr(hash_key, vh_GetStringNm(htp, "table_name"));
		}
		else if (!vh_IsNullNm(htp, "table_name"))
		{
			vh_str.AssignStr(hash_key, vh_GetStringNm(htp, "table_name"));
		}
		else
		{
			elog(ERROR2,
				 emsg("Expected a non-null INFORMATION_SCHEMA.COLUMNS.table_name "
					  "value, unable to continue processing the back end's schema."));
		}

		if (!vh_kvmap_exists(htbl, vh_str_buffer(hash_key)))
		{
			/*
			 * We need to make a copy of our hash_key to insert into the
			 * hash table.  We're constanly changing the buffer backing 
			 * hash_key.  The hash table is taking the pointer to this 
			 * buffer and using it to trap collisions in the hash algorithm.
			 */
			buffer = vhmalloc(vh_strlen(hash_key) + 1);
			memcpy(buffer, vh_str_buffer(hash_key), vh_strlen(hash_key));
			buffer[vh_strlen(hash_key)] = '\0';

			vh_kvmap_value(htbl, buffer, infotable);

			/*
			 * Reset this thing to zero, we'll fill in the members as
			 * required.
			 */
			memset(infotable, 0, sizeof(struct InfoTableData));

			if (!vh_IsNullNm(htp, "table_schema"))
			{
				infotable->sname = vh_str.ConstructStr(vh_GetStringNm(htp, "table_schema"));

				infotable->table_ident = vh_str.ConstructStr(infotable->sname);
				vh_str.Append(infotable->table_ident, ".");
			}
			else
			{
				infotable->sname = 0;

				infotable->table_ident = vh_str.Create();
			}

			infotable->tname = vh_str.ConstructStr(vh_GetStringNm(htp, "table_name"));
			vh_str.AppendStr(infotable->table_ident, infotable->tname);

			vh_htp_SListCreate(infotable->columns);
		}
		else
		{
			infotable = vh_kvmap_find(htbl, vh_str_buffer(hash_key));
		}

		vh_htp_SListPush(infotable->columns, htp);
	}

	vh_str.Destroy(hash_key);
	hash_key = 0;

	VH_TRY();
	{
		/*
		 * Generate the table defs and attach them to the hash table.  We do not
		 * add the TableDef to the target catalog until we have processed all of
		 * the additional meta data: primary keys, foreign keys, etc.
		 *
		 * Swap the memory context so we allocate the TableDef and TableField in
		 * the target TableCatalog's context, rather than our working set.
		 */
		mctx_old = vh_mctx_switch(vh_cat_tbl_mctx(target_catalog));
		iterate_infotables(htbl, generate_table_def, be);
		vh_mctx_switch(mctx_old);

		/*
		 * Let's roll thru the constraints and add them to the the hash table.  We
		 * take extra care to identify the PRIMARY KEY constraint and then handle
		 * the columns included in the PRIMARY KEY constraint when processing 
		 * CONSTRAINT_COLUMN_USAGE.
		 *
		 * Takes two to tango: TABLE_CONSTRAINTS and CONSTRAINT_COLUMN_USAGE are
		 * both required for anything tangible to happen with the TableDef.
		 */

		if (package.table_constraints)
		{
			process_table_constraints(htbl, package.table_constraints);
		
			if (package.constraint_column_usage)
			{
				process_constraint_column_usage(htbl, package.constraint_column_usage);

				/*
				 * Set the primary keys.  We rely entirely on a properly formed
				 * TABLE_CONSTRAINTS and CONSTRAINT_COLUMN_USAGE recordset to
				 * do this.  Without a well formed result set, we CANNOT set the
				 * primary key.
				 */
				iterate_infotables(htbl, set_primary_key, 0);
			}
			else
			{
				/*
				 * It's possible users sent us a TABLE_CONSTRAINTS table but 
				 * did not send us a corresponding CONSTRAINT_COLUMN_USAGE table.
				 *
				 * This is pretty damn pointless, so we'll warn which won't really
				 * do anything but emit a message to the logs.
				 */

				elog(WARNING,
					 emsg("A TABLE_CONSTRAINTS recordset was present but a "
						  "corresponding CONSTRAINT_COLUMN_USAGE table was not "
						  "present.  No keys: primary or foreign, will be set "
						  "from the schema"));
			}
		}

		/*
		 * Add each table to the table catalog.  We do this at the very end
		 * for particular reason other than as once a table is in a catalog,
		 * it can be found from user code.  We hold it to the end to ensure
		 * all of the configuration is set.
		 */
		iterate_infotables(htbl, add_to_target_catalog, target_catalog);
	}
	VH_CATCH();
	{
	}
	VH_ENDTRY();

	/*
	 * Cleanup our mess
	 */
	iterate_infotables(htbl, finalize_info_table, 0);
	vh_kvmap_destroy(htbl);

}

static NodeQuery
FormColumnsQuery(SList schemas)
{
	NodeQuerySelect nqsel;
	NodeFrom nfrom;
	NodeQual nqual;
	TableDef td_cis_cols;
	TableField tf;
	uint32_t sz;

	if (!catis)
		vh_sqlis_GetTableCatalog();

	td_cis_cols = vh_cat_tbl_getbyname(catis, "columns");

	nqsel = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nqsel, td_cis_cols, 0);
	vh_sqlq_sel_from_addfields(nqsel, nfrom, 0);

	if (schemas)
	{
		sz = vh_SListSize(schemas);
		tf = vh_td_tf_name(td_cis_cols, "table_schema");

		nqual = vh_nsql_qual_create(And, Eq);

		vh_nsql_qual_lhs_tf_set(nqual, tf);
		vh_nsql_qual_rhs_tvs_set(nqual);

		if (sz == 1)
			vh_tvs_store_String(vh_nsql_qual_rhs_tvs(nqual), *((String*)vh_SListFirst(schemas)));
		else
		{
		}
		
		vh_sqlq_sel_qual_add(nqsel, 0, nqual);	
	}

	return (NodeQuery)nqsel;
}


static NodeQuery
FormConstraintColumnUsageQuery(SList schemas)
{
	NodeQuerySelect nqsel;
	NodeFrom nfrom;
	NodeQual nqual;
	TableDef td_cis_cols;
	TableField tf;
	uint32_t sz;

	if (!catis)
		vh_sqlis_GetTableCatalog();

	td_cis_cols = vh_cat_tbl_getbyname(catis, "constraint_column_usage");

	nqsel = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nqsel, td_cis_cols, 0);
	vh_sqlq_sel_from_addfields(nqsel, nfrom, 0);

	if (schemas)
	{
		sz = vh_SListSize(schemas);
		tf = vh_td_tf_name(td_cis_cols, "table_schema");
		nqual = vh_nsql_qual_create(And, Eq);

		vh_nsql_qual_lhs_tf_set(nqual, tf);
		vh_nsql_qual_rhs_tvs_set(nqual);

		if (sz == 1)
			vh_tvs_store_String(vh_nsql_qual_rhs_tvs(nqual), *((String*)vh_SListFirst(schemas)));
		else
		{
		}
		
		vh_sqlq_sel_qual_add(nqsel, 0, nqual);	
	}

	return (NodeQuery)nqsel;
}

static NodeQuery
FormTableConstraintsQuery(SList schemas)
{
	NodeQuerySelect nqsel;
	NodeFrom nfrom;
	NodeQual nqual;
	TableDef td_cis_cols;
	TableField tf;
	uint32_t sz;

	if (!catis)
		vh_sqlis_GetTableCatalog();

	td_cis_cols = vh_cat_tbl_getbyname(catis, "table_constraints");

	nqsel = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nqsel, td_cis_cols, 0);
	vh_sqlq_sel_from_addfields(nqsel, nfrom, 0);

	if (schemas)
	{
		sz = vh_SListSize(schemas);
		tf = vh_td_tf_name(td_cis_cols, "table_schema");
		
		nqual = vh_nsql_qual_create(And, Eq);

		vh_nsql_qual_lhs_tf_set(nqual, tf);
		vh_nsql_qual_rhs_tvs_set(nqual);

		if (sz == 1)
			vh_tvs_store_String(vh_nsql_qual_rhs_tvs(nqual), *((String*)vh_SListFirst(schemas)));
		else
		{
		}
		
		vh_sqlq_sel_qual_add(nqsel, 0, nqual);	

	}

	return (NodeQuery)nqsel;
}

/*
 * Adds HeapTupleDef for all applicable INFORMATION_SCHEMA relations.
 */

static void
ConfigureInfoSchema(TableCatalog catalog)
{
	TableDef td;

	if (!(td = vh_cat_tbl_getbyname(catis, "columns")))
		CIS_Columns(catalog);

	if (!(td = vh_cat_tbl_getbyname(catis, "tables")))
		CIS_Tables(catalog);

	if (!(td = vh_cat_tbl_getbyname(catis, "table_constraints")))
		CIS_TableConstraints(catalog);

	if (!(td = vh_cat_tbl_getbyname(catis, "constraint_column_usage")))
		CIS_ConstraintColumnUsage(catalog);
}

static void
CIS_Columns(TableCatalog tc)
{
	TableDef td;

	if (!tc)
		return;

	td = vh_cat_tbl_createtbl(tc);
	td->sname = vh_str.Convert("INFORMATION_SCHEMA");
	td->tname = vh_str.Convert("columns");

	FIELD_ADD(td, String, "table_catalog");
	FIELD_ADD(td, String, "table_schema");
	FIELD_ADD(td, String, "table_name");
	FIELD_ADD(td, String, "column_name");
	FIELD_ADD(td, int16, "ordinal_position");
	FIELD_ADD(td, String, "data_type");

	vh_cat_tbl_add(tc, td);
}

static void
CIS_ConstraintColumnUsage(TableCatalog tc)
{
	TableDef td;

	if (!tc)
		return;

	td = vh_cat_tbl_createtbl(tc);
	td->sname = vh_str.Convert("INFORMATION_SCHEMA");
	td->tname = vh_str.Convert("constraint_column_usage");

	FIELD_ADD(td, String, "table_catalog");
	FIELD_ADD(td, String, "table_schema");
	FIELD_ADD(td, String, "table_name");
	FIELD_ADD(td, String, "column_name");
	FIELD_ADD(td, String, "constraint_catalog");
	FIELD_ADD(td, String, "constraint_schema");
	FIELD_ADD(td, String, "constraint_name");

	vh_cat_tbl_add(tc, td);
}


static void
CIS_Tables(TableCatalog tc)
{
	TableDef td;

	if (!tc)
		return;

	td = vh_cat_tbl_createtbl(tc);
	td->sname = vh_str.Convert("INFORMATION_SCHEMA");
	td->tname = vh_str.Convert("tables");

	FIELD_ADD(td, String, "table_catalog");
	FIELD_ADD(td, String, "table_schema");
	FIELD_ADD(td, String, "table_name");
	FIELD_ADD(td, String, "table_type");
	FIELD_ADD(td, String, "self_referencing_column_name");
	FIELD_ADD(td, String, "reference_generation");
	FIELD_ADD(td, String, "user_defined_type_catalog");
	FIELD_ADD(td, String, "user_defined_type_schema");
	FIELD_ADD(td, String, "user_defined_type_name");
	FIELD_ADD(td, int16, "is_insertable");
	FIELD_ADD(td, int16, "is_typed");
	FIELD_ADD(td, String, "commit_action");

	vh_cat_tbl_add(tc, td);
}

static void
CIS_TableConstraints(TableCatalog tc)
{
	TableDef td;

	if (!tc)
		return;

	td = vh_cat_tbl_createtbl(tc);
	td->sname = vh_str.Convert("INFORMATION_SCHEMA");
	td->tname = vh_str.Convert("table_constraints");

	FIELD_ADD(td, String, "constraint_catalog");
	FIELD_ADD(td, String, "constraint_schema");
	FIELD_ADD(td, String, "constraint_name");
	FIELD_ADD(td, String, "table_catalog");
	FIELD_ADD(td, String, "table_schema");
	FIELD_ADD(td, String, "table_name");
	FIELD_ADD(td, String, "constraint_type");

	vh_cat_tbl_add(tc, td);
}

static void 
iterate_infotables(KeyValueMap htbl,
				   void (*cb)(InfoTable, void*),
				   void *data)
{
	InfoTable it;
	KeyValueMapIterator kvm_it;
	const char *key;

	vh_kvmap_it_init(&kvm_it, htbl);

	while (vh_kvmap_it_next(&kvm_it, &key, &it))
	{
		cb(it, data);
	}
}

/*
 * Add columns collected from INFORMATION_SCHEMA.COLUMNS to each table.  We
 * use the InfoTable structure to track all of this.
 */
static void
generate_table_def(InfoTable it, void *data)
{
	BackEnd be = data;
	HeapTuplePtr *htp_head, htp;
	uint32_t htp_sz, i;
	String colnm, tynm;
	Type *types;

	it->td = vh_td_create(false);
	
	if (it->td->sname)
		it->td->sname = vh_str.ConstructStr(it->sname);
	else
		it->td->sname = 0;

	it->td->tname = vh_str.ConstructStr(it->tname);


	htp_sz = vh_SListIterator(it->columns, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];

		if (vh_IsNullNm(htp, "column_name") ||
			vh_IsNullNm(htp, "data_type"))
		{
			elog(WARNING,
				 emsg("Enable to add a column to table %s because the "
					  "column_name field and/or data_type field were null.  "
					  "Review back end information schema output.",
					  vh_str_buffer(it->table_ident)));
			continue;
		}

		colnm = vh_GetStringNm(htp, "column_name");
		tynm = vh_GetStringNm(htp, "data_type");

		if (!colnm || !tynm)
		{
			elog(WARNING, emsg("Unable to add column due to missing data type "
							   "specifier and/or column name on table %s.  Review "
							   "back end information schema output.",
							   vh_str_buffer(it->table_ident)));
			continue;
		}

		types = vh_be_type_getnative(be, vh_str_buffer(tynm));

		if (types && types[0])
		{
			vh_td_tf_add(it->td, &types[0], vh_str_buffer(colnm));
		}
		else
		{
			elog(WARNING, emsg("Unable to add column %s for table %s: back end provided "
							   "unrecognized data type %s at index %d", 
							   vh_str_buffer(colnm),
							   vh_str_buffer(it->table_ident),
							   vh_str_buffer(tynm),
							   i));
		}
	}
}


/*
 * INFORMATION_SCHEMA.TABLE_CONSTRAINTS
 */	
static void
process_table_constraints(KeyValueMap htbl, SList constraints)
{
	HeapTuplePtr *htp_head, htp;
	uint32_t htp_sz, i;
	String hash_key, constraint_type;
	InfoTable it;
	int32_t cmp;

	hash_key = vh_str.Create();
	htp_sz = vh_SListIterator(constraints, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];

		if (!vh_IsNullNm(htp, "table_schema") &&
			!vh_IsNullNm(htp, "table_name"))
		{
			vh_str.AssignStr(hash_key, vh_GetStringNm(htp, "table_schema"));
			vh_str.AppendStr(hash_key, vh_GetStringNm(htp, "table_name"));
		}
		else if (!vh_IsNullNm(htp, "table_name"))
		{
			vh_str.AssignStr(hash_key, vh_GetStringNm(htp, "table_name"));
		}
		else
		{
			elog(ERROR2,
				 emsg("Expected a non-null INFORMATION_SCHEMA.CONSTRAINTS.table_name "
					  "value, unable to continue adding the back end schema's constraints."));
		}
		
		if (vh_IsNullNm(htp, "constraint_type"))
		{
			elog(WARNING,
				 emsg("constraint_type missing for table %s.  Will continue "
					  "processing constraints.",
					  vh_str_buffer(hash_key)));
			continue;
		}
		else
		{
			constraint_type = vh_GetStringNm(htp, "constraint_type");
		}

		it = vh_kvmap_find(htbl, vh_str_buffer(hash_key));
	
		if (!it)
		{
			elog(ERROR2,
				 emsg("A constraint for %s could not be found in the "
					  "INFORMATION_SCHEMA.COLUMNS result set.  Unable to "
					  "continue processing...potentially corrupt "
					  "INFORMATION_SCHEMA data",
					  vh_str_buffer(it->table_ident)));
		}

		/*
		 * Handle the PRIMARY KEY constraint for the schema/table pair.
		 */	

		cmp = vh_str.Compare(constraint_type, "PRIMARY KEY");

		if (cmp == 0)
		{
			if (it->constraint_primarykey)
			{
				elog(ERROR2,
					 emsg("A primary key constraint has already beend discovered for "
						  "table %s.  Unable to continue processing TABLE_CONSTRAINTS.",
						  vh_str_buffer(it->table_ident)));
			}

			it->constraint_primarykey = htp;
			continue;
		}

		/*
		 * Handle FOREIGN KEY constraint for the schema/table pair.
		 */

		cmp = vh_str.Compare(constraint_type, "FOREIGN KEY");

		if (cmp == 0)
		{
			if (!it->constraint_foreignkeys)
				vh_htp_SListCreate(it->constraint_foreignkeys);

			vh_htp_SListPush(it->constraint_foreignkeys, htp);
			continue;
		}
	}

	vh_str.Destroy(hash_key);
	hash_key = 0;
}

static void
process_constraint_column_usage(KeyValueMap htbl, SList usage)
{
	HeapTuplePtr *htp_head, htp;
	uint32_t htp_sz, i;
	String hash_key, constraint_name;
	InfoTable it;
	int32_t cmp;

	hash_key = vh_str.Create();
	htp_sz = vh_SListIterator(usage, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];

		if (vh_IsNullNm(htp, "constraint_name"))
		{
			elog(WARNING,
				 emsg("Missing constraint_name for table %s, will process the next "
					  "constraint if one is available.",
					  vh_str_buffer(it->table_ident)));
			continue;
		}
		else
		{
			constraint_name = vh_GetStringNm(htp, "constraint_name");
		}

		if (!vh_IsNullNm(htp, "table_schema") &&
			!vh_IsNullNm(htp, "table_name"))
		{
			vh_str.AssignStr(hash_key, vh_GetStringNm(htp, "table_schema"));
			vh_str.AppendStr(hash_key, vh_GetStringNm(htp, "table_name"));
		}
		else if (!vh_IsNullNm(htp, "table_name"))
		{
			vh_str.AssignStr(hash_key, vh_GetStringNm(htp, "table_name"));
		}
		else
		{
			elog(ERROR2,
				 emsg("Expected a non-null INFORMATION_SCHEMA.CONSTRAINTS.table_name "
					  "value, unable to continue adding the back end schema's constraints."));
		}

		it = vh_kvmap_find(htbl, vh_str_buffer(hash_key));	

		if (!it)
		{
			elog(ERROR2,
				 emsg("A constraint for table %s was built but could not be found in the "
					  "INFORMATION_SCHEMA.COLUMNS result set.  Unable to continue processing "
					  "constraints...potentially corrupt INFORMATION_SCHEMA!",
					  vh_str_buffer(it->table_ident)));
		}	
		
		/*
		 * Check if we have a primary key and if we do, the constraint name
		 * should match the primary key constraint name.  If we match, then
		 * add the CONSTRAINT_COLUMN_USAGE HTP to the columns_primarykey list.
		 *
		 * We'll process the primary keys later.
		 */

		if (it->constraint_primarykey)
		{
			cmp = vh_str.CompareStr(vh_GetStringNm(it->constraint_primarykey, "constraint_name"),
									constraint_name);

			if (cmp == 0)
			{
				if (!it->columns_primarykey)
					vh_htp_SListCreate(it->columns_primarykey);

				vh_htp_SListPush(it->columns_primarykey, htp);
				continue;
			}
		}
	}

	vh_str.Destroy(hash_key);
	hash_key = 0;
}

/*
 * set_primary_key
 *
 * Iterate the list of CONSTRAINT_COLUMN_USAGE records which make up the
 * PRIMARY KEY for the given table.  If everything checks out, we'll set
 * the PK on the TableDef.
 */
static void
set_primary_key(InfoTable it, void *data)
{
	HeapTuplePtr *htp_head, htp;
	uint32_t htp_sz, i, j = 0;
	TableField tf;
	TableKey tk;
	TableDefVer tdv;
	String column_name;

	tdv = vh_td_tdv_lead(it->td);

	if (it->columns_primarykey)
	{
		htp_sz = vh_SListIterator(it->columns_primarykey, htp_head);
		memset(&tk, 0, sizeof(TableKey));
		tk.nfields = (uint16_t) htp_sz;

		if (htp_sz > VH_TABLEKEY_MAX_FIELDS)
		{
			elog(WARNING,
				 emsg("Table %s has a total of %d primary key fields, only %d "
					  "are supported by VH.IO.  No primary key information will be "
					  "available to VH.IO",
					  vh_str_buffer(it->table_ident),
					  htp_sz,
					  VH_TABLEKEY_MAX_FIELDS));
			return;
		}	

		for (i = 0; i < htp_sz; i++)
		{
			htp = htp_head[i];
			
			if (vh_IsNullNm(htp, "column_name"))
				continue;
			else
				column_name = vh_GetStringNm(htp, "column_name");
			
			tf = vh_td_tf_name(it->td, vh_str_buffer(column_name));

			if (tf)
			{
				tk.fields[j] = tf;
				j++;
			}
		}

		if (j == tk.nfields)
			tdv->key_primary = tk;
	}
}

static void
add_to_target_catalog(InfoTable it, void *data)
{
	TableCatalog catalog = data;
	bool exists;

	exists = vh_cat_tbl_exists(catalog, vh_str_buffer(it->td->tname));

	if (exists)
	{
		elog(WARNING,
			 emsg("A table called %s was already in the catalog, it will "
				  "not be replaced by the table schema fetched via the "
				  "SQL information schema infrastructure.",
				  vh_str_buffer(it->table_ident)));

		vh_td_finalize(it->td);
		it->td = 0;
	}
	else
	{
		vh_cat_tbl_add(catalog, it->td);
	}
}

static void
finalize_info_table(InfoTable it, void *data)
{
	if (it->tname)
		vh_str.Destroy(it->tname);

	if (it->sname)
		vh_str.Destroy(it->sname);

	if (it->table_ident)
		vh_str.Destroy(it->table_ident);

	if (it->columns_primarykey)
		vh_SListDestroy(it->columns_primarykey);
}

