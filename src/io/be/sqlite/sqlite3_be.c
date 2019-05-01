/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <ctype.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/BackEndCatalog.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableField.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/executor/eplan.h"
#include "io/executor/exec.h"
#include "io/executor/htc.h"
#include "io/executor/param.h"
#include "io/nodes/Node.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/plan/pstmt_funcs.h"
#include "io/sql/InfoScheme.h"
#include "io/utils/SList.h"
#include "io/utils/stopwatch.h"

#include "io/be/sqlite/sqlite_be.h"
#include "io/be/sqlite/amalgamation/sqlite3.h"


static BackEndConnection vh_sqlite_nconn_alloc(void);
static void vh_sqlite_nconn_free(BackEndConnection);
static bool vh_sqlite_nconn_connect(BackEndConnection, BackEndCredentialVal*, String);
static bool vh_sqlite_nconn_reset(BackEndConnection);
static bool vh_sqlite_nconn_disconnect(BackEndConnection);
static bool vh_sqlite_nconn_ping(BackEndConnection);

static bool vh_sqlite_xact_begin(BackEndConnection);
static bool vh_sqlite_xact_commit(BackEndConnection);
static bool vh_sqlite_xact_rollback(BackEndConnection);


typedef struct SqliteExecPortal
{
	BackEndExecPlan beep;
	sqlite3_stmt *stmt;
	MemoryContext mctx_work;
} SqliteExecPortal;

static void vh_sqlite_exec_portal_open(SqliteExecPortal*, BackEndExecPlan);
static void vh_sqlite_exec_portal_close(SqliteExecPortal*);

static void vh_sqlite_exec(BackEndExecPlan);
static void vh_sqlite_htc(SqliteExecPortal*);

static void vh_sqlite_latebind(SqliteExecPortal*);

static SqlInfoSchemePackage vh_sqlite_schema_get(BackEndConnection bec,
												 SqlInfoSchemeContext sisc);

static size_t sqlite_normalize_decltype(const char *src, char* target, size_t tlen);

#define VH_SQLITE_DECLTYPE_LEN		128

/*
 * Node->to_sql_cmd helper functions
 *
 * We have to override the Query tag in order to build a multi-row INSERT
 * statement for almost all SQLite versions.
 */

struct QinsAddParamData
{
	String cmd;
	NodeSqlCmdContext ctx;
	HeapTuplePtr htp;
	bool first_col;
};

static void qins_cmd_add_param(Node n, void *);
static bool vh_sqlite_cmd_query(String cmd, void *node,
								NodeSqlCmdContext ctx);

static const NodeSqlCmdFuncTable sqlite_cmd_ft[] = {
	{ Query, vh_sqlite_cmd_query },
	{ NT_Invalid, 0 }
};

static String vh_sqlite_command(Node, int32_t, TypeVarSlot**, int32_t*);
static Parameter vh_sqlite_parameter(ParameterList, 
									 Type*, TamGetUnion*, void**,
									 void*, bool);


typedef struct SqliteConnectionData
{
	struct BackEndConnectionData node;
	sqlite3 *db;
	bool db_open;
	bool db_inxact;
} SqliteConnectionData, *SqliteConnection;

typedef struct SqliteParameterData
{
	struct ParameterData p;
} SqliteParameterData, *SqliteParameter;

static struct CStrAMOptionsData sqlite3_cstr_be_opts = {
	.malloc = false
};

/*
 * We've got a few convenience functions here, mostly to call the interface
 * and do the error handling.  We want to be very verbose with errors: if the 
 * Sqlite API kicks one back, we want to bubble it up to the user in the form 
 * of an emsg.  Instead of handling all that mess all over the place, we've 
 * centralized it into these functions.  It's not uncommon for us to need to 
 * build a statement, execute it (expecting no rows), and tear it back down.
 *
 * All of these functions have an optional callback function which will fire
 * in the event of an error if present.  The callback's return will be passed
 * back down the stack as it unwinds.  The signature for all of these callback's
 * is the return type with the SqliteConnection pointer and the error code as
 * parameters.
 */
static bool sqlite_stmt_finalize(SqliteConnection, sqlite3_stmt*, 
								 bool (*callback)(SqliteConnection, int32_t));
static sqlite3_stmt *sqlite_stmt_prepare(SqliteConnection, const char *sql_cmd,
										 sqlite3_stmt* (*callback)(SqliteConnection, int32_t));
static bool sqlite_step_no_rows(SqliteConnection, sqlite3_stmt*,
								bool (*callback)(SqliteConnection, int32_t));




struct BackEndData vh_be_sqlite =
{
	.id = 1,
	.name = "SQLite3",
	.tam = TAM_CStr,
	.at = { 
		.createconn = vh_sqlite_nconn_alloc,
		.freeconn = vh_sqlite_nconn_free,
		.connect = vh_sqlite_nconn_connect,
		.reset = vh_sqlite_nconn_disconnect,
		.disconnect = vh_sqlite_nconn_disconnect,
	//	.ping = vh_sqlite_nconn_ping,

		.xactbegin = vh_sqlite_xact_begin,
		.xactcommit = vh_sqlite_xact_commit,
		.xactrollback = vh_sqlite_xact_rollback,

		.exec = vh_sqlite_exec,
		.command = vh_sqlite_command,
		.param = vh_sqlite_parameter,

		.schemaget = vh_sqlite_schema_get
	},

	.native_types = 0
};

#define check_sconn(s)	do {										\
	if (!s->db_open)												\
		elog(ERROR1,												\
			 emsg("Sqlite database connection is not open!")); 		\
																	\
	if (!s->db)														\
		elog(ERROR1,												\
			emsg("Corrupt Sqlite database connection")); 			\
	} while (0)							

#define NATIVE_TYPE(tynm, ty) { 												\
	Type tys[VH_TAMS_MAX_DEPTH];												\
	vh_type_stack_init(&tys[0]);												\
	vh_type_stack_push(tys, &vh_type_##ty);										\
	vh_be_type_setnative(be, tynm, &tys[0]);									\
}

#define NATIVE_TYPE_NEST(tynm, ty1, ty2) {										\
	Type tys[VH_TAMS_MAX_DEPTH];												\
	vh_type_stack_init(&tys[0]);												\
	vh_type_stack_push(tys, &vh_type_##ty1);									\
	vh_type_stack_push(tys, &vh_type_##ty2);									\
	vh_be_type_setnative(be, tynm, &tys[0]);									\
}

#define NATIVE_TYPE_NEST3(tynm, ty1, ty2, ty3) {								\
	Type tys[VH_TAMS_MAX_DEPTH];												\
	vh_type_stack_init(&tys[0]);												\
	vh_type_stack_push(tys, &vh_type_##ty1);									\
	vh_type_stack_push(tys, &vh_type_##ty2);									\
	vh_type_stack_push(tys, &vh_type_##ty3);									\
	vh_be_type_setnative(be, tynm, &tys[0]);									\
}


void
vh_be_sqlite3_register(CatalogContext ctx)
{
	BackEnd be = &vh_be_sqlite;
	struct TypeAMFuncs tamfs = { };

	vh_cat_be_add(ctx->catalogBackEnd, be);
	
	NATIVE_TYPE("integer", int32);
	NATIVE_TYPE("int", int32);
	NATIVE_TYPE("bigint", int64);
	NATIVE_TYPE("text", String);
	NATIVE_TYPE("varchar", String);
	NATIVE_TYPE("nvarchar", String);
	NATIVE_TYPE("date", Date);
	NATIVE_TYPE("datetime", DateTime);

	tamfs.cstr_format = "yyyy-MM-dd";
	vh_be_type_setam(be, &vh_type_Date, tamfs);

	tamfs.cstr_format = "yyyy-MM-dd k:mm:ss";
	vh_be_type_setam(be, &vh_type_DateTime, tamfs);

}

static BackEndConnection
vh_sqlite_nconn_alloc(void)
{
	SqliteConnection sconn = vh_be_conn_create(&vh_be_sqlite, sizeof(SqliteConnectionData));

	sconn->db = 0;
	sconn->db_open = false;
	sconn->db_inxact = false;

	return &sconn->node;
}

static bool
vh_sqlite_nconn_connect(BackEndConnection nconn, BackEndCredentialVal *bec,
						String database)
{
	SqliteConnection sconn = (SqliteConnection)nconn;
	int32_t rc = 0;
	
	rc = sqlite3_open(bec->uri, &sconn->db);

	switch (rc)
	{
		case SQLITE_OK:
			sconn->db_open = true;
			return true;
	}

	return false;
}

static void
vh_sqlite_nconn_free(BackEndConnection nconn)
{
	SqliteConnection sconn = (SqliteConnection)nconn;

	vhfree(sconn);
}

static bool
vh_sqlite_nconn_reset(BackEndConnection nconn)
{
	return true;
}

static bool
vh_sqlite_nconn_disconnect(BackEndConnection nconn)
{
	SqliteConnection sconn = (SqliteConnection)nconn;
	int32_t rc = 0;

	if (!sconn->db_open)
	{
		elog(WARNING,
			 emsg("Sqlite connection is not open but a disconnect command has "
				  "been requested!"));

		return false;
	}

	if (!sconn->db)
	{
		elog(ERROR1,
			 emsg("Sqlite connection pointer has been corrupted!"));

		return false;
	}

	rc = sqlite3_close(sconn->db);

	if (rc == SQLITE_OK)
	{
		sconn->db = 0;
		sconn->db_open = false;

		return true;
	}
	else if (rc == SQLITE_BUSY)
	{
		elog(WARNING,
			 emsg("Unable to close Sqlite connection; outstanding prepared statements "
				  "and/or unfinished sqlite3_backup_objects exist.  Connection shutdown "
				  "delayed until these are completed!"));
		return false;
	}

	return false;
}

static bool
vh_sqlite_xact_begin(BackEndConnection nconn)
{
	static const char* begin_xact = "BEGIN TRANSACTION;";

	SqliteConnection sconn = (SqliteConnection)nconn;
	sqlite3_stmt *stmt = 0;
	bool xact_started = false;

	check_sconn(sconn);

	if (sconn->db_inxact)
	{
		elog(ERROR1,
			 emsg("Sqlite connection is already in a transaction!  "
				  "Sqlite is unable to nest transactions!"));
		return false;
	}

	stmt = sqlite_stmt_prepare(sconn, begin_xact, 0);

	if (!stmt)
		return false;

	xact_started = sqlite_step_no_rows(sconn, stmt, 0);
	sconn->db_inxact = xact_started;

	sqlite_stmt_finalize(sconn, stmt, 0);

	return xact_started;
}

static bool
vh_sqlite_xact_commit(BackEndConnection nconn)
{
	static const char* commit_xact = "COMMIT TRANSACTION;";

	SqliteConnection sconn = (SqliteConnection)nconn;
	sqlite3_stmt *stmt = 0;
	bool xact_committed = false;

	check_sconn(sconn);

	if (!sconn->db_inxact)
	{
		elog(ERROR1,
			 emsg("Sqlite transaction must be open before calling a transaction "
				  "commit!"));

		return false;
	}

	stmt = sqlite_stmt_prepare(sconn, commit_xact, 0);

	if (!stmt)
		return false;

	xact_committed = sqlite_step_no_rows(sconn, stmt, 0);
	sconn->db_inxact = !xact_committed;

	sqlite_stmt_finalize(sconn, stmt, 0);

	return xact_committed;
}

static bool
vh_sqlite_xact_rollback(BackEndConnection nconn)
{
	static const char* rollback_xact = "ROLLBACK TRANSACTION;";

	SqliteConnection sconn = (SqliteConnection)nconn;
	sqlite3_stmt *stmt = 0;
	bool xact_rolled = false;

	check_sconn(sconn);

	if (!sconn->db_inxact)
	{
		elog(ERROR1,
			 emsg("Sqlite transaction must be open before calling a transaction "
				  "rollback!"));
		return false;
	}

	stmt = sqlite_stmt_prepare(sconn, rollback_xact, 0);

	if (!stmt)
		return false;

	xact_rolled = sqlite_step_no_rows(sconn, stmt, 0);
	sconn->db_inxact = !xact_rolled;

	sqlite_stmt_finalize(sconn, stmt, 0);

	return xact_rolled;
}

static bool 
vh_sqlite_stmt_finalize(SqliteConnection sconn, sqlite3_stmt* stmt,
	   					bool (*callback)(SqliteConnection, int32_t))
{
	sqlite3_finalize(stmt);

	return true;
}



static sqlite3_stmt*
vh_sqlite_stmt_prepare(SqliteConnection sconn, const char *sql_cmd,
		   			   sqlite3_stmt* (*callback)(SqliteConnection, int32_t))
{
	sqlite3_stmt *stmt = 0;
	int32_t rc;
	const char *pztail = 0;

	assert(sconn->db);

	rc = sqlite3_prepare_v2(sconn->db, sql_cmd, -1, &stmt, &pztail);

	if (rc)
	{
		if (callback)
		{
			return callback(sconn, rc);
		}
		
		elog(ERROR2,
			 emsg("Sqlite3 statement [%s] compilation error: %s",
				  sql_cmd,
				  sqlite3_errmsg(sconn->db)));

	}

	return stmt;

}

static bool 
vh_sqlite_step_no_rows(SqliteConnection sconn, sqlite3_stmt* stmt,
	   				   bool (*callback)(SqliteConnection, int32_t))
{
	return true;
}

static String 
vh_sqlite_command(Node nq, 
				  int32_t param_offset, TypeVarSlot **param_values, int32_t *param_count)
{
	String cmd;
	TypeVarSlot *values;
	int32_t param_current = param_offset;

	cmd = vh_str.Convert("-- vh_beat_commad: sqlite_command START\n");

	param_current = vh_nsql_cmd(nq, &cmd,
								&sqlite_cmd_ft[0],
								0,
								param_offset,
								&vh_be_sqlite,
								0,
								&values,
								false);

	vh_str.Append(cmd, "\n-- vh_beat_command: sqlite_command END");

	if (param_count)
		*param_count = param_current + param_offset;

	if (param_values)
		*param_values = values;

	return cmd;
}

static Parameter 
vh_sqlite_parameter(ParameterList pl, 
					Type* tys, 
					TamGetUnion* tam, void **tam_formatters,
					void *data, bool fnull)
{
	static const struct CStrAMOptionsData cstr_opts = { .malloc = true };
	MemoryContext mctx_old;
	SqliteParameter p;
	size_t tam_param_size = 0;

	mctx_old = vh_param_switchmctx(pl);

	p = vh_param_create(pl, sizeof(struct SqliteParameterData));

	if (fnull)
	{
		p->p.null = true;
	}
	else
	{
		p->p.null = false;
		p->p.value = vh_tam_fireu_cstr_get(tys, 				/* Type stack */
	  									   tam, 				/* Functions */
	  									   &cstr_opts,		 	/* Options */
		  								   data, 				/* Source field */
	  									   0, 					/* Target */
	  									   &tam_param_size,		/* Size pointer */
	  									   0,					/* Cursor pointer */
	  									   tam_formatters); 	/* Formatters */
		p->p.size = (int32_t)tam_param_size;
	}

	vh_mctx_switch(mctx_old);

	return &p->p;
}

static void vh_sqlite_exec(BackEndExecPlan beep)
{
	SqliteExecPortal sep = { };
	SqliteConnection sc = (SqliteConnection)beep->pstmtshd->nconn;
	SqliteParameter sp;
	int i, bind_error;

	vh_sqlite_exec_portal_open(&sep, beep);

	VH_TRY();
	{
		sep.stmt = vh_sqlite_stmt_prepare(sc, vh_str_buffer(beep->pstmtshd->command), 0);

		if (beep->pstmtshd->paramcount)
		{
			i = 0;
			vh_param_it_init(beep->pstmtshd->parameters);

			while ((sp = vh_param_it_next(beep->pstmtshd->parameters)))
			{
				if (sp->p.null)
					bind_error = sqlite3_bind_null(sep.stmt, ++i);
				else
					bind_error = sqlite3_bind_text(sep.stmt, 
												   ++i, 
												   sp->p.value, 
												   sp->p.size, 
												   0);

				if (bind_error != SQLITE_OK)
					elog(WARNING,
						 emsg("Error binding parameter %d, Sqlite return %d",
							  i,
							  bind_error));
			}

			assert(i == beep->pstmtshd->paramcount);
		}

		if (vh_pstmt_is_lb(beep->pstmt))
		{
			vh_sqlite_latebind(&sep);
		}

		vh_sqlite_htc(&sep);

		sqlite3_finalize(sep.stmt);
	}
	VH_CATCH();
	{
		if (sep.stmt)
			sqlite3_finalize(sep.stmt);
	}
	VH_ENDTRY();

	vh_sqlite_exec_portal_close(&sep);
}

static void 
vh_sqlite_exec_portal_open(SqliteExecPortal *sep, BackEndExecPlan beep)
{
	sep->beep = beep;
	sep->mctx_work = vh_MemoryPoolCreate(vh_mctx_current(),
										 2048, "Sqlite3 Working Context");
}

static void 
vh_sqlite_exec_portal_close(SqliteExecPortal *sep)
{
	vh_mctx_destroy(sep->mctx_work);
}


static void vh_sqlite_htc(SqliteExecPortal* sep)
{
	HeapTuplePtr *rs_transfer, *rs_htp, htp;
	HeapTuple ht, *rs_comp;
	int32_t i, ncols = 0, rtups = 0, step_res, col_type, col_len;
	vh_be_htc htc;
	struct vh_stopwatch sw;
	const unsigned char *col_val;
	QrpTableProjection qrpt;
	QrpFieldProjection qrpf;
	QrpBackEndProjection qrpb;
	TableDefVer tdv;
	TableField tf;
	int8_t td_idx;
	void **tam_formatters;
	union TamSetUnion *tam_func;
	Type *tam_type;

	vh_stopwatch_start(&sw);
	htc = sep->beep->htc_info->htc_cb;

	if (!htc)
	{
		elog(ERROR2,
			 emsg("Critical error, a HeapTupleCollector was not passed "
				  "to the back end executor.  Review planer implementation."));
		return;
	}

	rtups = sep->beep->pstmt->qrp_ntables;
	ncols = sep->beep->pstmt->qrp_nfields;

	qrpt = sep->beep->pstmt->qrp_table;
	qrpf = sep->beep->pstmt->qrp_field;
	qrpb = sep->beep->pstmt->qrp_backend;

	rs_transfer = vhmalloc_ctx(sep->mctx_work, sizeof(HeapTuple) * rtups * 3);
	rs_comp = (HeapTuple*)rs_transfer + rtups;
	rs_htp = (HeapTuplePtr*)rs_comp + rtups;

	memset(rs_transfer, 0, sizeof(HeapTuple) * rtups);

	for (i = 0; i < rtups; i++)
		rs_htp[i] = vh_hb_allocht(vh_hb(sep->beep->htc_info->hbno),
								  (HeapTupleDef)qrpt[i].rtdv,
								  &rs_comp[i]);

	do
	{
		step_res = sqlite3_step(sep->stmt);

		if (step_res == SQLITE_DONE)
			break;

		if (step_res == SQLITE_BUSY)
		{
			/*
			 * If we're in a transaction, we need to rollback and throw an 
			 * error per the SQLite3 documentation.
			 */
		}

		for (i = 0; i < ncols; i++)
		{
			td_idx = qrpf[i].td_idx;
			tf = (TableField)qrpf[i].hf;
			tdv = qrpt[td_idx].rtdv;

			tam_type = qrpf[i].tys;
			tam_formatters = qrpb[i].tam_formatters;
			tam_func = qrpb[i].tam_func;

			ht = rs_comp[td_idx];

			if (!ht)
			{
				htp = vh_hb_allocht(vh_hb(sep->beep->htc_info->hbno),
									(HeapTupleDef)tdv,
									&ht);
				rs_comp[td_idx] = ht;
				rs_htp[td_idx] = htp;
			}

			/*
			 * Check for a NULL column
			 */

			col_type = sqlite3_column_type(sep->stmt, i);

			if (col_type == SQLITE_NULL)
			{
				vh_htf_setnull(ht, tf);
			}
			else
			{	
				col_len = sqlite3_column_bytes(sep->stmt, i);
				col_val = sqlite3_column_text(sep->stmt, i);

				vh_htf_clearnull(ht, tf);

				vh_tam_fireu_cstr_set(tam_type, 				/* Type stack */
									  tam_func,					/* Functions */
									  &sqlite3_cstr_be_opts,	/* CStrAMOptions */
									  ((const char*)(col_val)),	/* Source */
									  vh_ht_field(ht, tf),		/* Target */
									  col_len,					/* Length */
									  0,						/* Cursor */
									  tam_formatters);			/* Format */
			}
		}

		for (i = 0; i < rtups; i++)
		{
			rs_transfer[i] = rs_htp[i];
			rs_comp[i] = 0;
		}

		htc(sep->beep->htc_info, rs_comp, rs_htp);

		sep->beep->htc_info->nrows++;

	} while (1);

	vh_stopwatch_end(&sw);
	sep->beep->stat_htform += vh_stopwatch_ms(&sw);
}

/*
 * vh_sqlite_latebind
 *
 * Does the late binding of the QRP in the event the planner was presented
 * with a query it doesn't know the table structure at planning time.
 */
static void 
vh_sqlite_latebind(SqliteExecPortal* sep)
{
	PlannedStmt pstmt = sep->beep->pstmt;
	int32_t colcount = 0, i = 0;
	const char *col_decltype, *col_fname;
	char col_decltype_norm[VH_SQLITE_DECLTYPE_LEN];
	Type *tys;
	MemoryContext mctx_old;

	mctx_old = vh_mctx_switch(sep->beep->mctx_result);

	if (!vh_pstmt_lb_do_add_col(pstmt))
		return;
	
	colcount = sqlite3_column_count(sep->stmt);

	if (!colcount)
		return;

	for (i = 0; i < colcount; i++)
	{
		col_decltype = sqlite3_column_decltype(sep->stmt, i);
		col_fname = sqlite3_column_name(sep->stmt, i);

		if (!col_decltype)
			col_decltype = "text";

		sqlite_normalize_decltype(col_decltype, 
								  &col_decltype_norm[0],
								  VH_SQLITE_DECLTYPE_LEN);

		tys = vh_be_type_getnative(&vh_be_sqlite, &col_decltype_norm[0]);

		if (!tys)
		{
			vh_mctx_switch(mctx_old);
			elog(ERROR2,
				 emsg("Sqlite3 presented a unknown type, %s",
					  col_decltype));
			return;
		}

		vh_pstmt_lb_add_col(pstmt, col_fname, tys);
	}
	
	vh_pstmt_lb_qrp(pstmt);

	vh_mctx_switch(mctx_old);
}

/*
 * This is a little sloppy, we'll query the table master and then we'll
 * use the PRAGMA table_info function in SQLite to get the schema for each
 * table.  The SList we return follows the ANSI SQL standard layout for
 * columns.  We'll only populate the fields that make sense.
 */
static SqlInfoSchemePackage
vh_sqlite_schema_get(BackEndConnection bec, 
					 SqlInfoSchemeContext sisc)
{
	const char *sql_tablelist = "SELECT * FROM sqlite_master "
		"WHERE type = 'table'";

	SqlInfoSchemePackage package = { };
	ExecResult er_tablelist, er_columnlist;
	HeapTuplePtr *htph_tablelist,
				 *htph_columnlist, htp_columnlist,
				 htp_is,
				 htp_tc, htp_ccu;
	HeapTuple ht_is;
	int32_t i, j, sz_tablelist, sz_columnlist, is_primarykey;
   	const StringData *str_primarykey;
	const struct StringData *tl_tablename;
	char sql_columnlist[256];
	TableDef sqlis_td_columns, 
			 sqlis_td_tc,
			 sqlis_td_ccu;
	TableDefVer sqlis_tdv_columns,
				sqlis_tdv_tc,
				sqlis_tdv_ccu;
	MemoryContext mctx_old, mctx_work;

	vh_htp_SListCreate(package.columns);

	mctx_work = vh_MemoryPoolCreate(vh_mctx_current(),
									512,
									"vh_sqlite_schema_get work area");
	mctx_old = vh_mctx_switch(mctx_work);

	er_tablelist = vh_exec_query_str(bec, sql_tablelist);
	sz_tablelist = vh_SListIterator(er_tablelist->tups, htph_tablelist);

	/*
	 * Get the INFORMATION_SCHEMA TableDef's so we can spin up
	 * HeapTuplePtr later.
	 */
	sqlis_td_columns = vh_sqlis_td_columns();
	sqlis_td_tc = vh_sqlis_td_tableconstraints();
	sqlis_td_ccu = vh_sqlis_td_constraintcolumnusage();

	sqlis_tdv_columns = vh_td_tdv_lead(sqlis_td_columns);
	sqlis_tdv_tc = vh_td_tdv_lead(sqlis_td_tc);
	sqlis_tdv_ccu = vh_td_tdv_lead(sqlis_td_ccu);

	for (i = 0; i < sz_tablelist; i++)
	{
		tl_tablename = vh_GetImStringNm(htph_tablelist[i], "tbl_name");
		htp_tc = 0;

		snprintf(&sql_columnlist[0], 256, "PRAGMA table_info(%s);",
				 vh_str_buffer(tl_tablename));

		er_columnlist = vh_exec_query_str(bec, &sql_columnlist[0]);
		sz_columnlist = vh_SListIterator(er_columnlist->tups, htph_columnlist);

		for (j = 0; j < sz_columnlist; j++)
		{
			/*
			 * Create a new HeapTuple matching the ANSI SQL information schema
			 * standard for COLUMNS.  We already have the HeapTupleDef available,
			 * so we just need to map the fields from each record returned by
			 * the PRAGMA statement into the INFORMATION_SCHEMA.COLUMNS
			 * HeapTupleDef.
			 */
			htp_columnlist = htph_columnlist[j];

			htp_is = vh_hb_allocht(vh_hb(sisc->hbno), 
								   &sqlis_tdv_columns->heap, 
								   &ht_is);

			vh_str.Assign(vh_GetStringNm(htp_is, "table_name"), 
						  vh_str_buffer(tl_tablename));

			vh_SetNullNm(htp_is, "table_schema");

			/*
			 * Normalize the decltype by only using everything left of the parenthesis
			 * and forcing it to lower case.
			 */

			sqlite_normalize_decltype(vh_str_buffer(vh_GetImStringNm(htp_columnlist, "type")),
									  &sql_columnlist[0],
									  256);

			vh_str.Assign(vh_GetStringNm(htp_is, "column_name"), vh_str_buffer(vh_GetImStringNm(htp_columnlist, "name")));
			vh_str.Assign(vh_GetStringNm(htp_is, "data_type"), &sql_columnlist[0]);
			vh_GetInt32Nm(htp_is, "ordinal_position") = j;

			/*
			 * We've got to form a make shift PRIMARY KEY contraint.  Sqlite's
			 * PRAGMA interface indicate a primary key by the |pk| column.
			 * 
			 * If the value is non-zero, then the column is a part of the primary
			 * key.
			 *
			 * We detect this and then build a INFORMATION_SCHEMA.CONSTRAINTS
			 * record, plus add an INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE
			 * record for each column added.  We keep it simple by calling our
			 * constraint name "PK_VH_IO_SQLITE3_BACKEND" betting that no user
			 * has a CONSTRAINT called that on each table.  Otherwise, we're
			 * going to cross a bunch of stuff up.
			 *
			 * Reset htp_tc to zero at each table iteration.
			 *
			 * For whatever reason, Sqlite presents |pk| as a String, so we're
			 * going to do something dirty and call the tam directly to get
			 * us an int32.
			 */

			str_primarykey = vh_GetImStringNm(htp_columnlist, "pk");
			vh_type_int32.tam.cstr_set(0,
									   &((struct CStrAMOptionsData) { .malloc = false }),
									   vh_str_buffer(str_primarykey),
									   &is_primarykey,
									   vh_strlen(str_primarykey),
									   0,
									   0);

			if (is_primarykey)
			{
				if (!htp_tc)
				{
					htp_tc = vh_hb_allocht(vh_hb(sisc->hbno),
										   &sqlis_tdv_tc->heap,
										   0);

					vh_str.Assign(vh_GetStringNm(htp_tc, "table_name"),
								  vh_str_buffer(tl_tablename));
					vh_SetNullNm(htp_tc, "table_schema");
					vh_str.Assign(vh_GetStringNm(htp_tc, "constraint_name"),
								  "PK_VH_IO_SQLITE3_BACKEND");
					vh_str.Assign(vh_GetStringNm(htp_tc, "constraint_type"),
								  "PRIMARY KEY");

					if (!package.table_constraints)
						vh_htp_SListCreate_ctx(package.table_constraints, mctx_old);

					vh_htp_SListPush(package.table_constraints, htp_tc);
				}

				htp_ccu = vh_hb_allocht(vh_hb(sisc->hbno),
										&sqlis_tdv_ccu->heap,
										0);
				vh_str.Assign(vh_GetStringNm(htp_ccu, "table_name"),
							  vh_str_buffer(tl_tablename));
				vh_SetNullNm(htp_ccu, "table_schema");
				vh_str.Assign(vh_GetStringNm(htp_ccu, "constraint_name"),
							  "PK_VH_IO_SQLITE3_BACKEND");
				vh_str.AssignStr(vh_GetStringNm(htp_ccu, "column_name"),
	   							 vh_GetStringNm(htp_is, "column_name"));

				if (!package.constraint_column_usage)
					vh_htp_SListCreate_ctx(package.constraint_column_usage, mctx_old);

				vh_htp_SListPush(package.constraint_column_usage, htp_ccu);

			}

			vh_htp_SListPush(package.columns, htp_is);
		}

		vh_exec_result_finalize(er_columnlist, false);
	}
	
	vh_exec_result_finalize(er_tablelist, false);

	vh_mctx_switch(mctx_old);
	vh_mctx_destroy(mctx_work);

	return package;	
}

static bool 
sqlite_stmt_finalize(SqliteConnection sc, sqlite3_stmt* stmt, 
					 bool (*callback)(SqliteConnection, int32_t))
{
	int32_t err = sqlite3_finalize(stmt);

	if (err == SQLITE_OK)
		return true;

	if (callback)
		return callback(sc, err);

	return false;
}

static sqlite3_stmt*
sqlite_stmt_prepare(SqliteConnection sc, const char *sql_cmd,
					sqlite3_stmt* (*callback)(SqliteConnection, int32_t))
{
	sqlite3_stmt *stmt;
	int32_t err = sqlite3_prepare_v2(sc->db, sql_cmd, -1, &stmt, 0);

	if (err == SQLITE_OK)
		return stmt;

	if (callback)
		return callback(sc, err);

	return 0;	
}

static bool 
sqlite_step_no_rows(SqliteConnection sc, sqlite3_stmt* stmt,
					bool (*callback)(SqliteConnection, int32_t))
{
	int32_t err = sqlite3_step(stmt);

	if (err == SQLITE_DONE)
		return true;

	if (callback)
		return callback(sc, err);

	return false;
}

static size_t 
sqlite_normalize_decltype(const char *src, char* target, size_t tlen)
{
	const char *paren_at = src;
	int32_t len, i;

	paren_at = strchr(src, '(');

	if (paren_at > src)
		len = paren_at - src;
	else
		len = strlen(src);

	if (len + 1 > tlen)
		return 0;

	strncpy(target, src, len);
	target[len] = '\0';

	for (i = 0; i < len; i++)
		target[i] = tolower(target[i]);

	return (size_t)(paren_at - src);
}
/*
 * vh_sqlite_cmd_query
 *
 * We have to build a helper function in th event more thn one record is 
 * inserted during an insert query.  Most versions of Sqlite won't take
 * a comma separated values row set.  Instead, we get to mimic one with a
 * SELECT UNION style statement.  Only do this if we absolutely have to,
 * fall into the standard algorithm when there is only one HTP to be
 * inserted.
 *
 * To fix this, we override for INSERT actions only.
 */
static bool 
vh_sqlite_cmd_query(String cmd, void *node,	NodeSqlCmdContext ctx)
{
	NodeQuery q = node;
	NodeQueryInsert nq = node;
	struct QinsAddParamData qapd = { };
	HeapTuplePtr *htp_head, htp;
	HeapField *hf_head, hf;
	TableField tf;
	int32_t hf_sz, htp_sz, i, j;
	TableDefVer tdv;
	TypeVarSlot tvs;
	bool first_col = true, first_rec = true;

	/*
	 * If we are a not a BulkInsert or an Insert, just call the default
	 * command.
	 */
	if (q->action != BulkInsert &&
		q->action != Insert)
	{
		assert(ctx->default_cmd);

		return ctx->default_cmd(cmd, node, ctx);
	}

	htp_sz = vh_SListIterator(nq->into->htps, htp_head);

	if (htp_sz == 1)
		return ctx->default_cmd(cmd, node, ctx);

	if (!nq->into->htps)
		elog(ERROR2,
			 emsg("Unable to form insert query SQL command: no HeapTuplePtr were "
				  "provided in the node!"));


	vh_str.Append(cmd, "INSERT INTO ");

	if (nq->into->tdv->td->sname)
	{
		vh_str.AppendStr(cmd, nq->into->tdv->td->sname);
		vh_str.Append(cmd, ".");
	}

	vh_str.AppendStr(cmd, nq->into->tdv->td->tname);

	tdv = nq->into->tdv;


	vh_str.Append(cmd, " (");

	if (nq->fields)
	{
		vh_nsql_cmd_impl(nq->fields, cmd, ctx, true);
	}
	else
	{
		/*
		 * Create a new function to generate a field list from the TableDef
		 * object.  Place in NodeField.c
		 */
		hf_sz = vh_SListIterator(tdv->heap.fields, hf_head);

		for (i = 0; i < hf_sz; i++)
		{
			hf = hf_head[i];
			tf = (TableField)hf;

			if (first_col)
				first_col = false;
			else
				vh_str.Append(cmd, ", ");

			vh_str.AppendStr(cmd, tf->fname);
		}
	}
		
	vh_str.Append(cmd, ") SELECT ");

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];

		if (first_rec)
		{
			first_rec = false;
		}
		else
		{
			vh_str.Append(cmd, " UNION ALL SELECT ");
		}

		/*
		 * Do the parameter expansion for each field on every HeapTuplePtr.
		 */

		if (nq->fields)
		{
			qapd.ctx = ctx;
			qapd.cmd = cmd;
			qapd.htp = htp;
			qapd.first_col = true;

			vh_nsql_visit_tree(nq->fields, qins_cmd_add_param, &qapd);	
		}
		else
		{
			first_col = true;

			for (j = 0; j < hf_sz; j++)
			{
				hf = hf_head[j];

				if (first_col)
					first_col = false;
				else
					vh_str.Append(cmd, ", ");

				vh_tvs_init(&tvs);
				vh_tvs_store_htp_hf(&tvs, htp, hf);
				vh_nsql_cmd_param_placeholder(cmd, ctx, &tvs);
			}
		}
	}

	return true;
}

/*
 * qins_cmd_add_param
 *
 * Helper function to vh_sqlite3_cmd_query to recursively iterate a field
 * list and generate a parameter placeholder.
 */
static void 
qins_cmd_add_param(Node n, void *data)
{
	NodeField nf = 0;
	struct QinsAddParamData *qapd = data;
	NodeSqlCmdContext ctx = qapd->ctx;
	String cmd = qapd->cmd;
	TypeVarSlot tvs;

	if (n->tag == Field)
	{
		nf = (NodeField)n;

		if (nf->tf)
		{
			if (qapd->first_col)
				qapd->first_col = false;
			else
				vh_str.Append(cmd, ", ");

			vh_tvs_init(&tvs);
			vh_tvs_store_htp_hf(&tvs, qapd->htp, (HeapField)nf->tf);
			vh_nsql_cmd_param_placeholder(cmd, ctx, &tvs); 
		}
	}
}

