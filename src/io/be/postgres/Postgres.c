/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/be/postgres/Impl.h"
#include "io/be/postgres/Postgres.h"
#include "io/buffer/HeapBuffer.h"
#include "io/catalog/BackEndCatalog.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/tam.h"
#include "io/executor/eplan.h"
#include "io/executor/param.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/plan/pstmt.h"
#include "io/plan/pstmt_funcs.h"
#include "io/utils/SList.h"
#include "io/utils/stopwatch.h"


/*
 * BackEnd Action Table Functions
 */
static BackEndConnection pgres_nconn_alloc(void);
static void pgres_nconn_free(BackEndConnection);

static bool pgres_nconn_connect(BackEndConnection, BackEndCredentialVal*, 
								String database);
static bool pgres_nconn_reset(BackEndConnection);
static bool pgres_nconn_disconnect(BackEndConnection);
static bool pgres_nconn_ping(BackEndConnection);

static bool pgres_xact_begin(BackEndConnection);
static bool pgres_xact_commit(BackEndConnection);
static bool pgres_xact_rollback(BackEndConnection);

static bool pgres_xact_tpc_commit(BackEndConnection);
static bool pgres_xact_tpc_rollback(BackEndConnection);

static void pgres_exec(BackEndExecPlan beep);

static String pgres_command(Node node, int32_t param_offset,
							TypeVarSlot **param_values, int32_t *param_count);
static Parameter pgres_parameter(ParameterList pl, 
								 Type *tam_type, 
								 TamGetUnion *tam_funcs, void **formatters,
								 void *ptr, bool null);



static struct BinaryAMOptionData OutboundBinaryOptions = {
	false,
	true,
	true
};

static struct BinaryAMOptionData InboundBinaryOptions = {
	true,
	false,
	false
};

/*
 * Node->to_sql_cmd specialization functions
 */

static void pgres_cmd_param_ph(String cmd, NodeSqlCmdContext ctx, TypeVarSlot *tvs);
static bool pgres_cmd_to_sql_query(String cmd, void *node, NodeSqlCmdContext ctx);

static const NodeSqlCmdFuncTable pgres_nsql_cmd_ft[] = {
	{ Query, pgres_cmd_to_sql_query },
	{ NT_Invalid, 0 }
};

/*
 * Execution Functions
 */

struct PgresExecPortalData
{
	MemoryContext mctx_work;
	MemoryContext mctx_old;
	BackEndExecPlan beep;

	PGconn *pgconn;

	HeapTuplePtr *rs_transfer, *rs_htp;
	HeapTuple *rs_comp;

	QrpTableProjection qrp_table;
	QrpFieldProjection qrp_field;
	QrpBackEndProjection qrp_be;
	vh_be_htc htc;
	int32_t qrp_ntables;
};


typedef struct PgresExecPortalData *PgresExecPortal;

static void pgres_ep_open(PgresExecPortal pep, BackEndExecPlan beep);
static void pgres_ep_close(PgresExecPortal pep);
static void pgres_ep_htc(PgresExecPortal pep);
static void pgres_ep_sendcmd(PgresExecPortal pep, bool bulk);
static void pgres_ep_checkerror(PgresExecPortal pep, PGresult *pgres,
								bool in_copy);

static void pgres_latebind(PgresExecPortal pep, PGresult *res);

static void pgres_ep_htc_rp(PgresExecPortal pep);
static int pgres_ep_htc_rp_cb(PGresult *res, const PGdataValue *cols,
							  int col_count, void *data);

static int32_t pgres_ep_transferparameters(ParameterList parameters, 
		   								   char ***values, int **lengths, 
		   								   int **formats, Oid **oids);
/*
 * Bulk Insert Defintions 
 */
struct PgresCopyInDefaults
{
	bool oids;
	const char header_signature;
};

struct PgresCopyInField
{
	int32_t len;
};

struct PgresCopyInTupleHeader
{
	int16_t ncols;
};

const char pgres_copyin_signature [] = { "PGCOPY\n\377\r\n\0" };
static void pgres_ep_copytuples(TableDef td, SList htps, PGconn *conn);
static void pgres_ep_sendbuffer(PGconn *conn, void *buffer, size_t size);


/*
 * BackEnd Definition
 */

static struct BackEndData vh_be_pgres =
{
	.id = 0,
	.name = "Postgres",
	.tam = TAM_Binary,			/* always communicate with binary */
	.at =
	{
		.createconn = pgres_nconn_alloc,
		.freeconn = pgres_nconn_free,
		.connect = pgres_nconn_connect,
		.reset = pgres_nconn_reset,
		.disconnect = pgres_nconn_disconnect,
		.ping = pgres_nconn_ping,

		.xactbegin = pgres_xact_begin,
		.xactcommit = pgres_xact_commit,
		.xactrollback = pgres_xact_rollback,
		.tpccommit = pgres_xact_tpc_commit,
		.tpcrollback = pgres_xact_tpc_rollback,

		.exec = pgres_exec,
		.command = pgres_command,
		.param = pgres_parameter
	},

	.native_types = 0
};

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

struct PgresOidType
{
	Oid oid;
	Type tys[VH_TAMS_MAX_DEPTH];
};

static const struct PgresOidType pgres_oid_map[] = {
	{ 16, { &vh_type_bool, 0 } },
	{ 20, { &vh_type_int64, 0 } },
	{ 21, { &vh_type_int16, 0 } },
	{ 23, { &vh_type_int32, 0 } },
	{ 25, { &vh_type_String, 0 } },
	{ 26, { &vh_type_int32, 0 } },
	{ 700, { &vh_type_float, 0 } },
	{ 701, { &vh_type_dbl, 0 } },
	{ 1042, { &vh_type_String, 0 } },
	{ 1082, { &vh_type_Date, 0 } },
	{ 1114, { &vh_type_DateTime, 0 } },
	{ 1700, { &vh_type_numeric, 0 } }
};

static Type* pgres_oid_map_find(Oid oid);
static int32_t pgres_oid_map_comp(const void *lhs, const void *rhs);

void
vh_sql_pgres_Register(CatalogContext ctx)
{
	CatalogContext cat_ctx;
	BackEndCatalog cat_be;
	BackEnd be;

	cat_ctx = vh_ctx();

	if (cat_ctx)
	{
		be = &vh_be_pgres;
		cat_be = cat_ctx->catalogBackEnd;
		vh_cat_be_add(cat_be, be);

		NATIVE_TYPE("bigint", int64);
		NATIVE_TYPE("bigserial", int64);
		NATIVE_TYPE_NEST("bigint[]", Array, int64);
		NATIVE_TYPE("integer", int32);
		NATIVE_TYPE_NEST("ARRAY", Array, int32);
		NATIVE_TYPE("oid", int32);
		NATIVE_TYPE("text", String);
		NATIVE_TYPE_NEST("text[]", Array, String);
		NATIVE_TYPE("character varying", String);
		NATIVE_TYPE("boolean", bool);
		NATIVE_TYPE("date", Date);
		NATIVE_TYPE("timestamp without time zone", DateTime);
		NATIVE_TYPE_NEST("daterange", Range, Date);
		NATIVE_TYPE_NEST("tsrange", Range, DateTime);
		NATIVE_TYPE("double precision", dbl);

		NATIVE_TYPE("name", String);
		NATIVE_TYPE("real", float);
		NATIVE_TYPE("\"char\"", String);
		NATIVE_TYPE("character", String);
		NATIVE_TYPE("json", String);
		NATIVE_TYPE("smallint", int16);
		NATIVE_TYPE("xid", int32);
		NATIVE_TYPE("regproc", String);
		NATIVE_TYPE("numeric", numeric);

		vh_pgres_ty_array_register(be);
		vh_pgres_ty_date_register(be);
		vh_pgres_ty_datetime_register(be);
		vh_pgres_ty_range_register(be);
	}
}


static BackEndConnection 
pgres_nconn_alloc(void)
{
	PostgresConnection pconn;

	pconn = vh_be_conn_create(&vh_be_pgres, sizeof(PostgresConnectionData));

	pconn->pgconn = 0;
	pconn->pgres = 0;
	pconn->connStatus = CONNECTION_BAD;
	pconn->xactStatus = PQTRANS_IDLE;
	
	return (BackEndConnection)pconn;
}

static bool 
pgres_nconn_connect(BackEndConnection nconn, BackEndCredentialVal *bec,
					String database)
{
	PostgresConnection pconn;
	String connstr;
	bool connected = false;

	pconn = (PostgresConnection)nconn;

	connstr = vh_str.Convert("host=");;
	vh_str.Append(connstr, &bec->hostname[0]);
	vh_str.Append(connstr, " port=");
	vh_str.Append(connstr, &bec->hostport[0]);
	vh_str.Append(connstr, " user=");
	vh_str.Append(connstr, &bec->username[0]);
	vh_str.Append(connstr, " password=");
	vh_str.Append(connstr, &bec->password[0]);
	vh_str.Append(connstr, " dbname=");
	vh_str.AppendStr(connstr, database);

	pconn->pgconn = PQconnectdb(vh_str_buffer(connstr));
	pconn->connStatus = PQstatus(pconn->pgconn);

	if (pconn->connStatus == CONNECTION_OK)
		connected = true;
	else
	{
		/*
		 * anything higher than ERROR1 will cause a jumpref
		 * which may not be safe since this method will typically
		 * be invoked by a struct that is shared across the process
		 * and may have pointers which should be freed before the jumpref
		 */

		printf("\n%s\n", PQerrorMessage(pconn->pgconn));

		elog(ERROR1, emsg("Postgres connection attempt failed with host %s on port %s for database %s with user %s"
			, &bec->hostname[0]
			, &bec->hostport[0]
			, database ? vh_str_buffer(database) : "UNKNOWN DATABASE"
			, &bec->username[0]));
	}

	return connected;
}

static void
pgres_nconn_free(BackEndConnection nconn)
{
	vhfree(nconn);
}

static bool 
pgres_nconn_reset(BackEndConnection nconn)
{
	PostgresConnection pconn;

	pconn = (PostgresConnection)nconn;

	PQfinish(pconn->pgconn);
	pconn->connStatus = CONNECTION_BAD;
	pconn->xactStatus = PQTRANS_IDLE;

	return true;
}

static bool 
pgres_nconn_disconnect(BackEndConnection nconn)
{
	PostgresConnection pgres = (PostgresConnection) nconn;

	PQfinish(pgres->pgconn);
	
	return true;
}

static bool 
pgres_nconn_ping(BackEndConnection nconn)
{
	return false;
}


static bool 
pgres_xact_begin(BackEndConnection nconn)
{
	PostgresConnection pconn;
	PGresult *res;

	pconn = (PostgresConnection)nconn;
	res = PQexec(pconn->pgconn, "BEGIN;");
	PQclear(res);

	return true;
}

static bool 
pgres_xact_commit(BackEndConnection nconn)
{
	PostgresConnection pconn;
	PGresult *res;

	pconn = (PostgresConnection)nconn;
	res = PQexec(pconn->pgconn, "COMMIT;");
	PQclear(res);

	return true;
}

static bool 
pgres_xact_rollback(BackEndConnection nconn)
{
	PostgresConnection pconn;
	PGresult *res;

	pconn = (PostgresConnection)nconn;
	res = PQexec(pconn->pgconn, "ROLLBACK;");
	PQclear(res);

	return true;
}

static bool
pgres_xact_tpc_commit(BackEndConnection nconn)
{
	return false;
}

static bool
pgres_xact_tpc_rollback(BackEndConnection nconn)
{
	return false;
}



/*
 * We should probably check the back end for Type specific functions
 * before using the generic ones specified.
 */	
static Parameter
pgres_parameter(ParameterList pl, 
				Type *tam_types, TamGetUnion *tam_funcs, void **tam_formatters,
				void *ptr,
				bool null)
{
	MemoryContext mctx_old;
	PgresParameter p = 0;
	size_t tam_param_size = 0;		/* Set to zero so the TAM mallocs the whole thing */
	size_t tam_param_cursor = 0;

	mctx_old = vh_param_switchmctx(pl);	

	p = vh_param_create(pl, sizeof(struct PgresParameterData));
	p->oid = 0;

	if (null)
	{
		p->p.value = 0;
		p->p.size = 0;
	}
	else
	{
		p->p.value = vh_tam_fireu_bin_get(tam_types, 
										  tam_funcs,
										  &OutboundBinaryOptions,
										  ptr, 
										  0, 
										  &tam_param_size, 
										  &tam_param_cursor);

		p->p.size = (uint32_t) tam_param_size;
	}

	vh_mctx_switch(mctx_old);

	return (Parameter)p;
}

static bool pgres_cmd_to_sql_query(String cmd, void *node,
								   NodeSqlCmdContext ctx)
{
	NodeQuery nq = node;
	NodeQueryInsert nqins;
	NodeFrom nfrom;
	TableDef td;	

	if (nq->action == BulkInsert)
	{
		nqins = (NodeQueryInsert)nq;
		nfrom = nqins->into;

		if (!nfrom)
			elog(ERROR2,
				 emsg("Corrupt NodeQueryInsert, mising valid NodeFrom pointer while "
					  "Postgres attempted to form a bulk insert SQL statement!"));

		td = nfrom->tdv->td;

		if (!td)
		
			elog(ERROR2,
				 emsg("Corrupt NodeFrom->NodeQueryInsert, missing a valid TableDef pointer "
					  "attached to the NodeFrom when Postgres attempted to form a bulk "
					  "insert SQL statement!"));

		vh_strappd(cmd, "COPY ");
		vh_nsql_cmd_impl(&nfrom->node, cmd, ctx, false);

		/*
		 * The planner is always going to give us an explicit field list for
		 * INSERT statements, so take advantage of it and call these out.
		 */

		vh_strappd(cmd, " (");
		vh_nsql_cmd_impl(nqins->fields, cmd, ctx, false);
		vh_strappd(cmd, " ) ");

		/*
		 * Set our options and let it roll.
		 */
		vh_strappd(cmd, "FROM STDIN WITH (FORMAT BINARY, OIDS false);");
	}
	else
	{
		vh_nsql_cmd_impl_def(&nq->node, cmd, ctx, false);

		/*
		 * Handle a RETURNING statement for INSERT and UPDATE queries
		 */
		if (nq->action == Insert ||
			nq->action == Update)
		{
			nqins = (NodeQueryInsert)nq;

			if (nq->action == Insert && nqins->rfields)
			{
				vh_strappd(cmd, " RETURNING ");
				vh_nsql_cmd_impl(nqins->rfields, cmd, ctx, true);
			}
		}
	}

	return true;
}

static String
pgres_command(Node node, int32_t param_offset,
			  TypeVarSlot **param_values, int32_t *param_count)
{
	String cmd;
	int32_t param_current = param_offset;
	TypeVarSlot *values;

	cmd = vh_str.Convert("-- vh_beat_command: pgres_command START\n");
	
	param_current = vh_nsql_cmd(node, &cmd, 
								&pgres_nsql_cmd_ft[0],
								pgres_cmd_param_ph,
								param_offset,
								&vh_be_pgres,
								0,
							    &values,	
								false);

	vh_strappd(cmd, "\n-- vh_beat_command: pgres_command END");

	if (param_count)
		*param_count = param_current + param_offset;

	if (param_values)
		*param_values = values;

	return cmd;
}

static void 
pgres_cmd_param_ph(String cmd, NodeSqlCmdContext ctx, TypeVarSlot *tvs)
{
	char buffer[64];

	snprintf(&buffer[0], 64, "$%d", ++ctx->param_count);
	vh_str.Append(cmd, &buffer[0]);
}



/*
 * Transfers the ParameterList in libpq compatible structure containing
 * four arrays: values, lengths, formats, and Oids.  We do one malloc
 * up front and just offset each individual array to make this work.
 */
static int32_t
pgres_ep_transferparameters(ParameterList parameters,
				 			char ***values, int **lengths,
							int **formats, Oid **oids)
{
	PgresParameter p;
	char **p_values;
	int size, p_count, *p_lengths, *p_formats, i = 0;
	Oid *p_oids;

	p_count = vh_param_count(parameters);

	if (p_count)
	{
		size = (sizeof(char*) * p_count) +
			(sizeof(int32_t) * p_count * 2) +
			(sizeof(Oid) * p_count);

		p_values = (char**)vhmalloc(size);
		p_lengths = (int*)(p_values + p_count);
		p_formats = (int*)(p_lengths + p_count);
		p_oids = (Oid*)(p_formats + p_count);

		(*values) = p_values;
		(*lengths) = p_lengths;
		(*formats) = p_formats;
		(*oids) = p_oids;

		vh_param_it_init(parameters);

		while ((p = vh_param_it_next(parameters)))
		{
			p_values[i]= (char*)p->p.value;
			p_lengths[i] = (int)p->p.size;
			p_formats[i] = 1;				/* always transfer binary */
			p_oids[i] = p->oid;

			i++;
		}
	}
	else
	{
		*values = 0;
		*lengths = 0;
		*formats = 0;
		*oids = 0;
	}

	return p_count;
}

static void 
pgres_exec(BackEndExecPlan beep)
{
	struct PgresExecPortalData pep;
	PGresult *pgres_bi;
	NodeQueryInsert nqins;

	assert(beep);

	pgres_ep_open(&pep, beep);

	VH_TRY();
	{
		if (beep->pstmt->nquery &&
			beep->pstmt->nquery->action == BulkInsert)
		{
			/*
			 * Bulk Insert
			 *
			 * 	>	Send the command to the backend to open the window
			 * 	>	Fill a buffer and regularly send over to the back end
			 * 	>	Check for errors
			 */
			pgres_ep_sendcmd(&pep, true);
			pgres_bi = PQgetResult(pep.pgconn);
			pgres_ep_checkerror(&pep, pgres_bi, true);
			PQclear(pgres_bi);

			nqins = (NodeQueryInsert)beep->pstmt->nquery;
			pgres_ep_copytuples(nqins->into->tdv->td, nqins->into->htps, pep.pgconn);
			pgres_bi = PQgetResult(pep.pgconn);
			pgres_ep_checkerror(&pep, pgres_bi, true);
			PQclear(pgres_bi);

			while ((pgres_bi = PQgetResult(pep.pgconn)))
			{
				pgres_ep_checkerror(&pep, pgres_bi, true);
				PQclear(pgres_bi);
			}
		}
		else
		{
			pgres_ep_sendcmd(&pep, false);

			if (beep->discard)
			{
				/*
				 * We need to pull the PGresults off the connection until they're null.
				 */

				while ((pgres_bi = PQgetResult(pep.pgconn)))
				{
					pgres_ep_checkerror(&pep, pgres_bi, true);
					PQclear(pgres_bi);
				}
			}
			else
			{
				pgres_ep_htc(&pep);
			}
		}
	}
	VH_CATCH();
	{
		//pgres_ep_close(&pep);
		//vh_rethrow(); __attribute__((noreturn));
		//vh_rethrow();
	}
	VH_ENDTRY();

	pgres_ep_close(&pep);
}

static void
pgres_ep_open(PgresExecPortal pep, BackEndExecPlan beep)
{
	PostgresConnection npgconn;

	assert(pep);
	assert(beep);

	if (beep->pstmt)
	{
		//assert(beep->htc_info);
	}

	if (beep->pstmtshd)
	{
		npgconn = (PostgresConnection)beep->pstmtshd->nconn;

		if (npgconn)
		{
			pep->pgconn = npgconn->pgconn;
		}
		else
		{
			elog(ERROR2,
				 emsg("Postgres: a connection was not found when opening "
					  "the local execution portal!"));
		}
	}
	
	pep->mctx_work = vh_MemoryPoolCreate(beep->mctx_work, 512,
	   									 "Postgres working");
	pep->mctx_old = vh_mctx_switch(pep->mctx_work);
	pep->beep = beep;
}

static void
pgres_ep_close(PgresExecPortal pep)
{
	vh_mctx_switch(pep->mctx_old);
	vh_mctx_destroy(pep->mctx_work);

	pep->mctx_work = 0;
}

static void
pgres_ep_checkerror(PgresExecPortal pep, PGresult *pgres, bool in_copy)
{
	ExecStatusType est;

	if (pgres)
	{
		est = PQresultStatus(pgres);

		switch (est)
		{
		case PGRES_COMMAND_OK:
		case PGRES_SINGLE_TUPLE:
		case PGRES_TUPLES_OK:
			break;

		case PGRES_COPY_IN:
			
			if (!in_copy)
			{
				elog(ERROR2,
					 emsg("An error was discovered processing the query as a "
						  "bulk copy operation with:\nBack end error code: %d"
						  "\nBack end description: %s"
						  "\nQuery: %s",
						  est, 
						  PQresultErrorMessage(pgres),
						  vh_str_buffer(pep->beep->pstmtshd->command)));
			}

			break;

		default:
			elog(ERROR2,
				 emsg("An error was discovered processing the query!"
					  "\nBack end error code: %d"
					  "\nBack end description: %s"
					  "\nQuery: %s",
					  est, 
					  PQresultErrorMessage(pgres),
					  vh_str_buffer(pep->beep->pstmtshd->command)));
			break;
		}
	}
}

static void
pgres_ep_sendcmd(PgresExecPortal pep, bool bulk)
{
	String command;
	char **p_values;
	int *p_lengths, *p_formats, p_count, res;
	Oid *p_oids;
	struct vh_stopwatch sw;

	p_count = pgres_ep_transferparameters(pep->beep->pstmtshd->parameters,
				  						  &p_values, &p_lengths,
				  						  &p_formats, &p_oids);

	command = pep->beep->pstmtshd->command;

	vh_stopwatch_start(&sw);

	res = PQsendQueryParams(pep->pgconn,						/* Connection */
		  					vh_str_buffer(command),			/* Command */
		  					p_count, 					/* Parameter count */
		  					p_oids, 					/* Parameter OID */
			  				(const char* const*)p_values,	/* Parameter values */
		  					p_lengths, 				/* Parameter lengths */
		  					p_formats, 				/* Parameter formats */
		  					1);						/* 1: Binary, 0: Text */

	if (res)
	{
	}

	if (!pep->beep->discard)
	{
		PQsetSingleRowMode(pep->pgconn);
	}
	
	vh_stopwatch_end(&sw);
	pep->beep->stat_qexec += vh_stopwatch_ms(&sw);
}


static void 
pgres_ep_htc(PgresExecPortal pep)
{
	HeapTuplePtr *rs_transfer, *rs_htp, htp;
	HeapTuple ht, *rs_comp;
	int32_t i, j, ncols = 0, ntables;
	int8_t td_i;
	TableDefVer tdv;
	TableField tf;
	vh_be_htc htc;
	PGresult *pgres;
	struct vh_stopwatch sw;
	QrpTableProjection qrpt;
	QrpFieldProjection qrpf;
	QrpBackEndProjection qrpb;
	int64_t be_wait_count = 0;
	bool latebind, first = true;

	vh_stopwatch_start(&sw);
	htc = pep->beep->htc_info->htc_cb;

	if (!htc)
	{
		elog(ERROR2,
			 emsg("Critical error, a HeapTupleCollector was not passed "
				  "to the back end executor.  Review planer implementation."));
		return;
	}

	latebind = vh_pstmt_is_lb(pep->beep->pstmt);
	
	while ((pgres = PQgetResult(pep->pgconn)))
	{
		pgres_ep_checkerror(pep, pgres, false);

		if (!PQntuples(pgres))
		{
			PQclear(pgres);
			be_wait_count++;
			continue;
		}

		if (latebind)
		{
			pgres_latebind(pep, pgres);
			latebind = false;
		}

		if (first)
		{	
			qrpt = pep->beep->pstmt->qrp_table;
			qrpf = pep->beep->pstmt->qrp_field;
			qrpb = pep->beep->pstmt->qrp_backend;

			ntables = pep->beep->pstmt->qrp_ntables;

			rs_transfer = vhmalloc_ctx(pep->mctx_work, sizeof(HeapTuple) * ntables * 3);
			rs_comp = (HeapTuple*)rs_transfer + ntables;
			rs_htp = (HeapTuplePtr*)rs_comp + ntables;


			memset(rs_transfer, 0, sizeof(HeapTuple) * ntables);

			for (i = 0; i < ntables; i++)
				rs_htp[i] = vh_hb_allocht(vh_hb(pep->beep->htc_info->hbno),
										  (HeapTupleDef)qrpt[i].rtdv,
										  &rs_comp[i]);

			first = false;

		}

		pep->beep->htc_info->nrows++;

		if (!ncols)
		{
			ncols = PQnfields(pgres);
			assert(ncols == pep->beep->pstmt->qrp_nfields);
		}

		/*
		 * Loop thru all of the query columns, grabbing their values from
		 * the database results.
		 */
		for (j = 0; j < ncols; j++)
		{
			/* 
			 * Derived from the Query Result Projection using the
			 * current column:
			 * td - TableDef
			 * tf - TableField
			 * ty - Type
			 */
			td_i = qrpf[j].td_idx;
			tdv = qrpt[td_i].rtdv;
			tf = (TableField)qrpf[j].hf;

			ht = rs_comp[td_i];

			if (!ht)
			{
				htp = vh_hb_allocht(vh_hb(pep->beep->htc_info->hbno),
									(HeapTupleDef)tdv,
									&ht);
				rs_comp[td_i] = ht;
				rs_htp[td_i] = htp;
			}
			
			if (PQgetisnull(pgres, 0, j))
				vh_htf_setnull(ht, tf);
			else
			{
				vh_htf_clearnull(ht, tf);

				/*
				 * Do what we came here to do, fill a HeapTuple with data from
				 * the back end.  We call vh_tam_fireu_bin_set which takes the
				 * Type stack for the given field and the TamSetUnion.  We
				 * assume tam_types and tam_funcs were built by QRP which
				 * pretty much guarantees our result set column order will 
				 * match the type order and indexing.
				 */
				vh_tam_fireu_bin_set(qrpf[j].tys, qrpb[j].tam_func,
									 &InboundBinaryOptions,
									 PQgetvalue(pgres, 0, j),
									 vh_ht_field(ht, tf),
									 PQgetlength(pgres, 0, j),
									 0);
			}
		}


		/*
		 * Call the HTC, checking to see if we need to re-point the local
		 * copy of pointer to the HTC.  The HTC makes no guarantees the function
		 * pointer will remain the same when cycling (i.e. it may detect memory
		 * pressure and flush the results to disk).
		 */

		htc(pep->beep->htc_info, rs_comp, rs_htp);
		
		for (j = 0; j < ntables; j++)
		{
			rs_transfer[j] = rs_htp[j];
			rs_comp[j] = 0;
		}

		PQclear(pgres);
	}
	
	vh_stopwatch_end(&sw);
	pep->beep->stat_htform += vh_stopwatch_ms(&sw);
	pep->beep->stat_wait_count += be_wait_count;
}

/*
 * Postgres Bulk Copy interface
 *
 * http://www.postgresql.org/docs/9.6/static/sql-copy.html
 */
static void 
pgres_ep_copytuples(TableDef td, SList htps, PGconn *conn)
{
	const size_t buffersz = 1024 * 1000;
	size_t hf_len, hf_cursor;
	unsigned char *buffer, *cursor, *bufferend;
	TableField *tf_head, tf;
	TableDefVer tdv;
	HeapTuplePtr *htp_head;
	HeapTuple ht;
	uint32_t tf_sz, htp_sz, i, j;
	int32_t *n_32, pqend_err;
	struct PgresCopyInTupleHeader *cpi_tup;
	struct PgresCopyInField *cpi_field;
	Type **tam_types;
	TamUnion **tam_funcs;

	buffer = vhmalloc(buffersz);
	bufferend = buffer + buffersz;
	cursor = buffer;

	/*
	 * Let's get the Type Access Method arrays setup for the given HeapTupleDef.
	 */

	tdv = vh_td_tdv_lead(td);
	tam_types = vh_htd_type_stack(&tdv->heap);
	tam_funcs = vh_tam_htd_create(0, TAM_Binary, &tdv->heap, &vh_be_pgres, 0, true);

	assert(tam_types);
	assert(tam_funcs);


	/*
	 * Postgres Copy Header includes:
	 * 	11 byte signature
	 * 	32 bit signed integer for flags (primarily indicating OIDs present)
	 * 	32 bit signed integer for extension
	 * ========================================
	 *  19 byte header section
	 */
	memcpy(cursor, &pgres_copyin_signature[0], 11);
	cursor += 11;

	n_32 = (int32_t*)cursor;
	*n_32 = 0;
	cursor += 4;

	n_32 = (int32_t*)cursor;
	*n_32 = 0;
	cursor += 4;

	htp_sz = vh_SListIterator(htps, htp_head);
	tf_sz = vh_SListIterator(tdv->heap.fields, tf_head);

	for (i = 0; i < htp_sz; i++)
	{
		ht = vh_htp(htp_head[i]);

		if (ht)
		{
			/*
			 * Check to see if there's enough space remaining in the buffer
			 * to accomodate a PgresCopyInTupleHeader to indicate the number
			 * of fields for this row.
			 */
			if (bufferend - cursor <
				sizeof(struct PgresCopyInTupleHeader))
			{
				pgres_ep_sendbuffer(conn, buffer, cursor - buffer);
				cursor = buffer;
			}

			cpi_tup = (struct PgresCopyInTupleHeader*)cursor;
			cpi_tup->ncols = __bswap_16((int16_t)tf_sz);

			cursor = (unsigned char*)(cpi_tup + 1);

			for (j = 0; j < tf_sz; j++)
			{
				tf = tf_head[j];

				/*
				 * Check for a null value first, as the Postgres binary COPY 
				 * format allows us to just put a -1 length followed by no
				 * trailing bytes for a null value in the PgresCopyInField.
				 */
				if (vh_htf_isnull(ht, tf))
				{
					if (bufferend - cursor < sizeof(struct PgresCopyInField))
					{
						pgres_ep_sendbuffer(conn, buffer, cursor - buffer);
						cursor = buffer;
					}

					cpi_field = (struct PgresCopyInField*)cursor;
					cpi_field->len = __bswap_32(-1);
					cursor = (unsigned char*)(cpi_field + 1);
					
					continue;
				}

				/*
				 * Check to see if there's enough space left on the buffer
				 * to accomodate a PgresCopyInField plus atleast 32 bytes.
				 * 
				 * It's 32 bytes because it's nice and wide and it's probably
				 * not going to hurt much to send another stream over rather
				 * than trying to 
				 */

				if ((bufferend - cursor) < sizeof(struct PgresCopyInField) + 32)
				{
					pgres_ep_sendbuffer(conn, buffer, cursor - buffer);
					cursor = buffer;
				}

				cpi_field = (struct PgresCopyInField*)cursor;
				cursor = (unsigned char*)(cpi_field + 1);

				hf_len = bufferend - cursor;

				if (tf->heap.type_depth == 1 &&
					tam_types[j][0]->size < 256 &&				/* from the above check */
					!tam_types[j][0]->varlen)
				{
					hf_cursor = 0;

					vh_tam_fireu_bin_get(tam_types[j], ((TamGetUnion*)tam_funcs[j]),
										 &InboundBinaryOptions,
										 vh_ht_field(ht, tf), cursor,
										 &hf_len, &hf_cursor);

					cpi_field->len = __bswap_32((int32_t)hf_len);

					cursor += hf_len;
				}
				else
				{
					hf_cursor = 0;
					hf_len = bufferend - cursor;
				
					assert(hf_len > 0);

					/*
					 * We know we've got atleast one byte to jam the variable
					 * length data into.  The goal on the first call is to get
					 * the overall length of the source and set the length on
					 * PgresCopyInField.  We need the total length to move in 
					 * the same iteration of pgres_ep_sendbuffer otherwise
					 * we'll never get it set to the right value.
					 *
					 * NOTE: see the previous check that ensures we've got
					 * enough space in the buffer for a PgresCopyInField
					 * plus atleast one byte of variable data.
					 */

					vh_tam_fireu_bin_get(tam_types[j], ((TamGetUnion*)tam_funcs[j]),
										 &InboundBinaryOptions,
										 vh_ht_field(ht, tf), cursor,
										 &hf_len, &hf_cursor);

					cpi_field->len = __bswap_32((int32_t)hf_len);

					cursor += hf_cursor;

					/*
					 * The first call to bin_get is going to update the
					 * hf_len variable to the overall length and the 
					 * hf_cursor to how many bytes its traversed.  Thus
					 * if the hf_len is greater than hf_cursor, the buffer
					 * is full and it needs to be shipped.  We do this first
					 * in the loop, always.  We just keep repeating the 
					 * pgres_ep_send_buffer and bin_get calls until we've
					 * flushed the whole thing out.
					 */

					while (hf_len > hf_cursor)
					{
						pgres_ep_sendbuffer(conn, buffer, cursor - buffer); 
						cursor = buffer;

						hf_len = bufferend - cursor;

						vh_tam_fireu_bin_get(tam_types[j], ((TamGetUnion*)tam_funcs[j]),
											 &InboundBinaryOptions,
											 vh_ht_field(ht, tf), cursor,
											 &hf_len, &hf_cursor);

						cursor += hf_cursor;
					}
				}
			}
		}
	}

	/*
	 * The COPY specification requires us to send a final tuple header 
	 * with a number of columns set to -1.
	 */

	if (bufferend - cursor < sizeof(struct PgresCopyInTupleHeader))
	{
		pgres_ep_sendbuffer(conn, buffer, cursor - buffer);
		cursor = buffer;
	}

	cpi_tup = (struct PgresCopyInTupleHeader*)cursor;
	cpi_tup->ncols = __bswap_16(-1);
	cursor = (unsigned char*)(cpi_tup + 1);

	pgres_ep_sendbuffer(conn, buffer, cursor - buffer);

	pqend_err = PQputCopyEnd(conn, 0);

	if (pqend_err == -1)
		elog(ERROR2,
			 emsg("Bulk Insert wire transmission error\n%s",
				  PQerrorMessage(conn)));

	vhfree(buffer);
	vhfree(tam_funcs);
}

/*
 * Performs PQputCopyData and handles an errors that may be returned.  libpq
 * introduces a wait semantic for PQputCopyData and we'll also handle this.
 *
 * http://www.postgresql.org/docs/9.6/static/libpq-copy.html
 */
static void
pgres_ep_sendbuffer(PGconn *conn, void *buffer, size_t bytes)
{
	int32_t result;

	result = PQputCopyData(conn, buffer, bytes);

	if (result)
	{
	}
}

static int pgres_ep_htc_rp_cb(PGresult *res, const PGdataValue *cols,
							  int ncols, void *data)
{
	PgresExecPortal pep = data;
	HeapTuplePtr *rs_transfer, *rs_htp, htp;
	HeapTuple ht, *rs_comp;
	int32_t j, ntables;
	int8_t td_i;
	TableDefVer tdv;
	TableField tf;
	vh_be_htc htc;
	QrpTableProjection qrpt;
	QrpFieldProjection qrpf;
	QrpBackEndProjection qrpb;

	if (!pep->qrp_field &&
		vh_pstmt_is_lb(pep->beep->pstmt))
	{
		pgres_latebind(pep, res);
	}

	htc = pep->htc;
	qrpt = pep->qrp_table;;
	qrpf = pep->qrp_field;
	qrpb = pep->qrp_be;
	ntables = pep->qrp_ntables;

	rs_transfer = pep->rs_transfer;
	rs_htp = pep->rs_htp;
	rs_comp = pep->rs_comp;

	/*
	 * Loop thru all of the query columns, grabbing their values from
	 * the database results.
	 */
	for (j = 0; j < ncols; j++)
	{
		/* 
		 * Derived from the Query Result Projection using the
		 * current column:
		 * td - TableDef
		 * tf - TableField
		 * ty - Type
		 */
		td_i = qrpf[j].td_idx;
		tdv = qrpt[td_i].rtdv;
		tf = (TableField)qrpf[j].hf;

		ht = rs_comp[td_i];

		if (!ht)
		{
			htp = vh_hb_allocht(vh_hb(pep->beep->htc_info->hbno),
								(HeapTupleDef)tdv,
								&ht);
			rs_comp[td_i] = ht;
			rs_htp[td_i] = htp;
		}
		
		if (cols[j].len < 0)
		{
			vh_htf_setnull(ht, tf);
		}
		else
		{
			vh_htf_clearnull(ht, tf);

			/*
			 * Do what we came here to do, fill a HeapTuple with data from
			 * the back end.  We call vh_tam_fireu_bin_set which takes the
			 * Type stack for the given field and the TamSetUnion.  We
			 * assume tam_types and tam_funcs were built by QRP which
			 * pretty much guarantees our result set column order will 
			 * match the type order and indexing.
			 */
			vh_tam_fireu_bin_set(qrpf[j].tys, qrpb[j].tam_func,
								 &InboundBinaryOptions,
								 cols[j].value,
								 vh_ht_field(ht, tf),
								 cols[j].len,
								 0);
		}
	}

	for (j = 0; j < ntables; j++)
	{
		rs_transfer[j] = rs_htp[j];
		rs_comp[j] = 0;
	}
	/*
	 * Call the HTC, checking to see if we need to re-point the local
	 * copy of pointer to the HTC.  The HTC makes no guarantees the function
	 * pointer will remain the same when cycling (i.e. it may detect memory
	 * pressure and flush the results to disk).
	 */

	pep->beep->htc_info->nrows++;
	htc(pep->beep->htc_info, rs_comp, rs_htp);
	
	return 1;
}

static void
pgres_latebind(PgresExecPortal pep, PGresult *res)
{
	PlannedStmt pstmt = pep->beep->pstmt;
	int32_t colcount = 0, i = 0;
	const char *col_fname;
	Oid col_decltype;
	Type *tys;
	MemoryContext mctx_old;

	mctx_old = vh_mctx_switch(pep->beep->mctx_result);

	if (!vh_pstmt_lb_do_add_col(pstmt))
		return;

	colcount = PQnfields(res);

	if (!colcount)
		return;

	for (i = 0; i < colcount; i++)
	{
		col_decltype = PQftype(res, i);
		col_fname = PQfname(res, i);

		tys = pgres_oid_map_find(col_decltype);

		if (!tys)
		{
			vh_mctx_switch(mctx_old);
			elog(ERROR2,
					emsg("Postgres presented an unknown Oid [%d] for column index "
					     "[%d] during runtime query binding for the query [%s].",
						 col_decltype, i, "0"));
			return;
		}

		vh_pstmt_lb_add_col(pstmt, col_fname, tys);
	}

	vh_pstmt_lb_qrp(pstmt);
		
	pep->qrp_field = pep->beep->pstmt->qrp_field;
	pep->qrp_be = pep->beep->pstmt->qrp_backend;

	vh_mctx_switch(mctx_old);
}

static Type*
pgres_oid_map_find(Oid oid)
{
	struct PgresOidType *map;

	map = bsearch(&oid, pgres_oid_map,
				  sizeof(pgres_oid_map) / sizeof(struct PgresOidType),
				  sizeof(struct PgresOidType),
				  pgres_oid_map_comp);

	if (map)
	{
		return &map->tys[0];
	}

	return 0;
}

static int32_t
pgres_oid_map_comp(const void *lhs, const void *rhs)
{
	const Oid *olhs = lhs, *orhs = rhs;

	return (*olhs < *orhs ? -1 : *olhs > *orhs);
}

