/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "tbe.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/executor/eresult.h"
#include "io/executor/exec.h"
#include "io/nodes/NodeCreateTable.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/utils/SList.h"

static int32_t drop_table_if_exists(BackEndConnection bec, const char *table_name);
static int32_t create_table(BackEndConnection bec, const char *table_name,
							TypeVarSlot *sample_values, int32_t n_cols,
							TableDef *td_out);
static int32_t insert_table(BackEndConnection bec, TableDef td,
							TypeVarSlot **values, int32_t n_rows, int32_t n_cols);

static int32_t select_table(BackEndConnection bec, const char *table_name,
							TypeVarSlot **values, int32_t n_rows, int32_t n_cols);
static int32_t comp_expected(HeapTuplePtr htp, TypeVarSlot *ev, int32_t n_cols);

static int32_t delete_table(BackEndConnection bec, const char *table_name);


TypeVarSlot**
tbe_alloc_expectedv(int32_t n_rows, int32_t n_columns)
{
	size_t alloc_sz = 0;
	TypeVarSlot **rows, *cursor = 0;
	int32_t i;

	alloc_sz = (sizeof(TypeVarSlot*) * n_rows) +
			   (sizeof(TypeVarSlot) * (n_rows * n_columns));

	rows = vhmalloc(alloc_sz);
	memset(rows, 0, alloc_sz);

	cursor = (TypeVarSlot*)(rows + n_rows);

	for (i = 0; i < n_rows; i++)
	{
		rows[i] = cursor;
		cursor += n_columns;
	}

	return rows;
}

int32_t 
tbe_sql_command_cycle(BackEndConnection bec,
					  const char *table_name,
	   				  TypeVarSlot **expected_values,
					  int32_t n_rows, int32_t n_cols,
					  int32_t flags)
{
	/*
	 * Our goal here is to make sure that each BackEnd's implementation moves the
	 * data types correctly.  We've had situations where our Type implementation
	 * doesn't do this correctly and we get weird results down stream.  This module
	 * allows us to test every data type in about as concise of a manner as possible.
	 *
	 * @bec
	 * BackEndConnection to perform the cycle with.
	 *
	 * @table_name
	 * Name of the table to be DROPPED, CREATED, INSERTED, SELECTED AND DELETED.  We
	 * recommend using descriptive table names as that don't repeat across the test
	 * suite.
	 *
	 * @expected_values
	 * Array of array of TypeVarSlot, first array is rows and second is the columns.
	 * Thus if you want to insert more than one row you'll need to allocate sufficient
	 * space for n_rows * n_columns plus the pointers.  There's a helper function for
	 * this exact need.  We use the memset routines to move values from the TypeVarSlot
	 * to the HeapTuple, so we expect those to be tested thoroughly prior to this 
	 * module being invoked.
	 *
	 * @n_rows
	 * Number of rows in @expected_values
	 *
	 * @n_columns
	 * Number of columns in @expected_values
	 *
	 * @flags
	 * Still working on the options for this function.
	 *
	 * We don't expect to recieve column names, we'll let the module do that for us.  
	 * They really don't matter to us for this module, we do everything based off of
	 * indexes.  Thus you can get some nasty results if a particular database doesn't
	 * support a specific type.
	 *
	 * Our flow is fairly simple and looks like this:
	 *
	 * 1)	Build a DROP Table Command
	 * 2)	Issue the CREATE Table Command to the database
	 * 3)	Create and fire the INSERT Command
	 * 4)	Issue a SELECT statement, make sure n_rows comes back
	 *		a)	Compare values in the rows with the values in the table (we count
	 *			on the individual type's compare function already being unit tested
	 *			for this to work properly)
	 * 5)	Issue a DELETE statement to clear the table.  We should probably loop
	 * 		around and to the INSERT/SELECT/COMPARE again with a different INSERT
	 * 		method (i.e. binary vs. non-binary).  Since we always send parameters
	 * 		and this is a generic utility to support all backends, we're not goin
	 * 		to hard code a SQL command without parameters at the moment.
	 *
	 */

	MemoryContext mctx_old, mctx;
	TableDef td;
	int32_t res;

	assert(bec);
	assert(table_name);
	assert(n_rows);
	assert(n_rows > 0);
	assert(n_cols);
	assert(n_cols < 240);

	mctx = vh_MemoryPoolCreate(vh_mctx_current(), 8192, "Test BackEnd");
	mctx_old = vh_mctx_switch(mctx);

	VH_TRY();
	{
		res = drop_table_if_exists(bec, table_name);
		
		if (res)
		{
			res = -1;
			goto unwind;
		}

		res = create_table(bec, table_name, expected_values[0], n_cols, &td);

		if (res)
		{
			res = -2;
			goto unwind;
		}

		res = insert_table(bec, td, expected_values, n_rows, n_cols);

		if (res)
		{
			res = -3;
			goto unwind;
		}

		res = select_table(bec, table_name, expected_values, n_rows, n_cols);

		if (res)
		{
			res = -4;
			goto unwind;
		}

		res = delete_table(bec, table_name);

		if (res)
		{
			res = -5;
			goto unwind;
		}
	}
	VH_CATCH();
	{
		vh_mctx_switch(mctx_old);
		vh_mctx_destroy(mctx);

		elog(ERROR2,
				emsg("Uncaught error, aborting test of table %s",
					 table_name));

		return -1;
	}
	VH_ENDTRY();

unwind:

	vh_mctx_switch(mctx_old);
	vh_mctx_destroy(mctx);

	return res;
}

/*
 * drop_table_if_exists
 *
 * Drops a table if it exists.  We should be doing this thru the planner, but
 * for now just hard code the command.  The planner allows us to take advantage
 * of backend specific grammar.  Since we're only setup for Postgres and SQLite
 * at the moment, we can just hard code these in.
 *
 * Plus there's not infrastructure for DDL in the planner at the moment.
 */
static int32_t
drop_table_if_exists(BackEndConnection bec, const char *table_name)
{
	String str_sql;
	ExecResult er;

	str_sql = vh_str.Convert("DROP TABLE IF EXISTS ");
	vh_str.Append(str_sql, table_name);

	er = vh_exec_query_str(bec, vh_str_buffer(str_sql));
	vh_exec_result_finalize(er, true);

	return 0;
}


/*
 * create_table
 *
 * First we have to create a TableDef and then we can add in the fields.  After
 * the TD has been created, we'll have to invoke the planner to execute the
 * DDL.  We want the planner to do this, so we can get the backend's type keywords
 * for the data types.
 */
static int32_t 
create_table(BackEndConnection bec, const char *table_name,
			 TypeVarSlot *sample_values, int32_t n_cols,
			 TableDef *td_out)
{
	TableDef td;
	TableField tf;
	Type tys[VH_TAMS_MAX_DEPTH];
	NodeCreateTable nct;
	PlannerOpts popts = { };
	char field_name[3];
	int32_t i;
	int8_t ty_depth;

	/*
	 * Build out the table.  We don't really care about the column names, but
	 * there's a good chance the back end does.  So we just invent one by assuming
	 * the number of columns is less than 240 (10 * 24 characters).
	 *
	 * The first character will be 'a' + the tens spot.  The second character will
	 * be 'z' minus the ones spot.
	 *
	 * After the TD has been created, we can set it's name, etc.
	 */

	*td_out = 0;
	field_name[2] = '\0';
	td = vh_td_create(false);
	td->tname = vh_str.Convert(table_name);

	for (i = 0; i < n_cols; i++)
	{
		field_name[0] = 'a' + (i / 10);
		field_name[1] = 'k' - (10 - (i % 10));

		ty_depth = vh_tvs_fill_tys(&sample_values[i], tys);

		if (!ty_depth)
		{
			elog(WARNING,
					emsg("Unable to discern data types at row 1, column %d "
						 "of the expected values, substituting a String data "
						 "type.",
						 i));

			tys[0] = &vh_type_String;
			tys[1] = 0;
		}

		tf = vh_td_tf_add(td, tys, field_name);

		if (!tf)
		{
			elog(ERROR2,
					emsg("The TableDef did not create a new TableField for "
						 "column %d",
						 i));
		}
	}

	*td_out = td;

	/*
	 * Let's have the planner create our new table in the database.  This is much
	 * safer than trying to create a SQL command.  Since each back end has it's
	 * own reprenstation of the data types supported, we need to take advantage
	 * of the command infrastructure available to the planner.
	 */

	popts.bec = bec;
	nct = vh_sqlq_ctbl_create();
	vh_sqlq_ctbl_td(nct, td);

	vh_exec_node_opts((Node)nct, popts);


	return 0;
}

/*
 * insert_table
 *
 * Forms HeapTuplePtr and executes an INSERT statement into the target TD.
 */
static int32_t 
insert_table(BackEndConnection bec, TableDef td,
			 TypeVarSlot **values, int32_t n_rows, int32_t n_cols)
{
	SList htps;
	HeapTuplePtr htp;
	HeapTuple ht;
	HeapTupleDef htd;
	HeapField *hfs, hf;
	NodeQueryInsert query;
	PlannerOpts popts = { };
	ExecResult er;
	void *src, *tgt;
	int32_t i, j, hfs_sz;

	/*
	 * Copy the value in the TypeVarSlot to a new HeapTuple for each row in the
	 * expected values.
	 */
	vh_htp_SListCreate(htps);
	htd = vh_td_htd(td);
	hfs_sz = vh_SListIterator(htd->fields, hfs);

	assert(hfs_sz == n_cols);

	for (i = 0; i < n_rows; i++)
	{
		htp = vh_allochtp_td(td);
		ht = vh_htp(htp);

		for (j = 0; j < n_cols; j++)
		{
			hf = hfs[j];

			if (vh_tvs_isnull(&values[i][j]))
			{
				vh_htf_setnull(ht, hf);
				continue;
			}

			/*
			 * We don't have a null value in the slot for this row/column pair.
			 * Call the memset routine to transfer the value from the TypeVarSlot
			 * (@src) to the HeapTuple (@tgt).
			 */
			src = vh_tvs_value(&values[i][j]);
			tgt = vh_ht_field(ht, hf);

			vh_tam_fire_memset_set(hf->types, src, tgt, 0);
		}

		vh_htp_SListPush(htps, htp);
	}

	/*
	 * Setup a new insert query and then execute it against the backend.
	 */
	query = vh_sqlq_ins_create();
	vh_sqlq_ins_table(query, td);
	vh_sqlq_ins_htp_list(query, htps);

	popts.bec = bec;

	er = vh_exec_node_opts((Node)query, popts);
	vh_exec_result_finalize(er, true);

	return 0;
}

/*
 * select_table
 *
 * Executes a basic SELECT statement against a backend connection and then
 * calls comp_expected for each row.
 */
static int32_t
select_table(BackEndConnection bec, const char *table_name,
			 TypeVarSlot **values, int32_t n_rows, int32_t n_cols)
{
	String str_sql;
	ExecResult er;
	HeapTuplePtr htp;
	int32_t i, comp_res, res = 0;

	str_sql = vh_str.Convert("SELECT * FROM ");
	vh_str.Append(str_sql, table_name);

	er = vh_exec_query_str(bec, vh_str_buffer(str_sql));

	if (vh_exec_result_iter_first(er))
	{
		i = 0;

		do
		{
			htp = vh_exec_result_iter_htp(er, 0);
			comp_res = comp_expected(htp, values[i], n_cols);

			if (comp_res)
			{
				elog(WARNING,
						emsg("Row %d had %d columns that did not compare to the "
							 "expected values for table %s",
							 i,
							 comp_res,
							 table_name));
				res++;
			}

			i++;

		} while (vh_exec_result_iter_next(er));
	}

	if (i != n_rows)
	{
		elog(WARNING,
				emsg("Number of rows returned by the SELECT query was %d, "
					 "expected %d",
					 i,
					 n_rows));

		if (!res)
			res = -1;
	}

	vh_exec_result_finalize(er, true);
	vh_str.Destroy(str_sql);

	return res;
}

/*
 * comp_expected
 *
 * Compares every field on the HeapTuple with the expected value.  If the
 * comparison isn't true, then we increase the failure counter.
 *
 * The return value is either zero, indicating all fields match, or a number
 * greater than zero, indicating the number of fields that did not compare.
 */
static int32_t 
comp_expected(HeapTuplePtr htp, TypeVarSlot *ev, int32_t n_cols)
{
	HeapTupleDef htd;
	HeapTuple ht;
	HeapField *hfs;
	TypeVarSlot heap_value = { };
	int32_t hfs_sz, comp_fail = 0, comp_res, i;

	ht = vh_htp(htp);
	htd = ht->htd;
	hfs_sz = vh_SListIterator(htd->fields, hfs);

	assert(hfs_sz == n_cols);

	for (i = 0; i < hfs_sz; i++)
	{
		vh_tvs_store_ht_hf(&heap_value, ht, hfs[i]);
		comp_res = vh_tvs_compare(&heap_value, &ev[i]);

		if (comp_res)
		{
			elog(WARNING,
					emsg("Expected value and comparison value from BackEnd do not "
						 "match for field at index %d",
						 i));

			comp_fail++;
		}
	}

	return comp_fail;
}

/*
 * delete_table
 *
 * Deletes all records from the table.
 */
static int32_t
delete_table(BackEndConnection bec, const char *table_name)
{
	String str_sql;
	ExecResult er;

	str_sql = vh_str.Convert("DELETE FROM ");
	vh_str.Append(str_sql, table_name);

	er = vh_exec_query_str(bec, vh_str_buffer(str_sql));
	vh_exec_result_finalize(er, true);

	return 0;
}

