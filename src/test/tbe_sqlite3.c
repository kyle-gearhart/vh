/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/be/sqlite/sqlite_be.h"
#include "io/buffer/HeapBuffer.h"
#include "io/buffer/HeapTuplePtr.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/BackEndCatalog.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/PrintTup.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/types/Array.h"
#include "io/catalog/types/Date.h"
#include "io/catalog/types/DateTime.h"
#include "io/executor/eresult.h"
#include "io/executor/exec.h"
#include "io/executor/xact.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/shard/Shard.h"
#include "io/sql/InfoScheme.h"
#include "io/sql/Query.h"
#include "io/utils/SList.h"

#include "test.h"

static Shard shd;
static ShardAccess sa;
static BackEndConnection bec;

static void setup_beacon(void);
static void configure_from_is(void);

static void run_exec_query_td(void);
static void run_exec_query_str(void);
static void run_exec_query_ins(void);
static void run_exec_query_ins_multi(void);

void test_be_sqlite3(void)
{
	vh_be_sqlite3_register(ctx_catalog);

	setup_beacon();
	run_exec_query_str();
	run_exec_query_td();
	run_exec_query_ins();
	run_exec_query_ins_multi();
}

static void setup_beacon(void)
{
	BackEndCredentialVal becredval = { };
	BackEndCredential becred;
	BackEnd be;

	strcpy(&becredval.uri[0], "/usr/local/src/vh/unit/vh_test");

	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_URI);

	becred = vh_be_cred_create(BECSM_PlainText);
	vh_be_cred_store(becred, &becredval);

	be = &vh_be_sqlite;
	sa = vh_sharda_create(becred, be);
	
	shd = vh_shard_create((ShardId){}, sa, sa);

	ctx_catalog->shard_general = shd;

	configure_from_is();
}

static void
configure_from_is(void)
{
	bec = vh_ConnectionGet(ctx_catalog->catalogConnection, sa);
	vh_sqlis_loadschema(ctx_catalog->catalogTable, bec, 0);
}

static void
run_exec_query_str(void)
{
	ExecResult er;
	HeapTuplePtr htp;
	int32_t i;
	

	er = vh_exec_query_str(bec, "SELECT * FROM test_int4;");

	if (vh_exec_result_iter_last(er))
	{
		i = 0;
		
		do
		{
			htp = vh_exec_result_iter_htp(er, 0);

			printf("\n%lld/%lld: %d",
				   vh_HTP_BLOCKNO(htp),
				   vh_HTP_ITEMNO(htp),
				   vh_GetInt32Nm(htp, "col"));

			i++;
		} while ( i < 10 && vh_exec_result_iter_prev(er));

		printf("\n\n");
	}

	er = vh_exec_query_str(bec, "INSERT INTO test_int4 VALUES ('55');");

	vh_ConnectionReturn(ctx_catalog->catalogConnection, bec);
}
static void
run_exec_query_td(void)
{
	TableDef td_test_int4 = 0,
			 td_test_int8 = 0,
			 td_test_multicol = 0,
			 td_test_dt = 0;
	NodeQuerySelect nqsel;
	ExecResult er;
	PrintTupCtx ptup;
	HeapField hf, hf2;
	char buffer[256];

	td_test_int4 = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
										"test_int4");

	assert(td_test_int4);

	td_test_int8 = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
										"test_int8");
	assert(td_test_int8);

	td_test_multicol = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
											"test_multicol");
	assert(td_test_multicol);

	td_test_dt = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
									  "test_dt");
	assert(td_test_dt);

	nqsel = vh_sqlq_sel_query_td(td_test_multicol);
	vh_sqlq_sel_limit_set(nqsel, 10);

	er = vh_exec_node(&nqsel->query.node);

	if (vh_exec_result_iter_first(er))
	{
		do
		{
			HeapTuplePtr htp = vh_exec_result_iter_htp(er, 0);
			HeapTuple ht = vh_exec_result_iter_ht(er, 0);
			String ht_fval = vh_ht_GetStringNm(ht, "a");

			printf("\nrow %lld/%lld >> ",
				   vh_HTP_BLOCKNO(htp),
				   vh_HTP_ITEMNO(htp));

			if (ht_fval)
				printf("a:%s", vh_str_buffer(ht_fval));

			ht_fval = vh_ht_GetStringNm(ht, "b");

			if (ht_fval)
				printf("\tb:%s", vh_str_buffer(ht_fval));
		} while (vh_exec_result_iter_next(er));

		printf("\n\n");
	}

	vh_exec_result_finalize(er, false);

	nqsel = vh_sqlq_sel_query_td(td_test_dt);
	vh_sqlq_sel_limit_set(nqsel, 10);

	er = vh_exec_node(&nqsel->query.node);
	
	if (vh_exec_result_iter_first(er))
	{
		ptup = vh_ptup_create(vh_td_htd(td_test_dt));

		vh_ptup_add_field(ptup,
						  hf = (HeapField)vh_td_tf_name(td_test_dt, "b"),
						  "EEEE, MMMM d, yyyy");
		vh_ptup_add_field(ptup,
						  hf2 = (HeapField)vh_td_tf_name(td_test_dt, "a"), 0);

		do
		{
			HeapTuplePtr htp = vh_exec_result_iter_htp(er, 0);

			vh_ptup_field_buffer(ptup, htp, hf2, &buffer[0], 256);
			printf("\na: %s", &buffer[0]);

			vh_ptup_field_buffer(ptup, htp, hf, &buffer[0], 256);
			printf("\tb: %s", &buffer[0]);

		} while (vh_exec_result_iter_next(er));

		vh_ptup_finalize(ptup);
		vhfree(ptup);

		printf("\n\n");
	}

	vh_exec_result_finalize(er, false);
}

static void
run_exec_query_ins(void)
{
	TableDef td_test_int4 = 0,
			 td_test_int8 = 0,
			 td_test_multicol = 0,
			 td_test_dt = 0;
	HeapTuplePtr htp_dt = 0;

	XAct xact = vh_xact_create(Immediate); 

	td_test_int4 = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
										"test_int4");

	assert(td_test_int4);

	td_test_int8 = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
										"test_int8");
	assert(td_test_int8);

	td_test_multicol = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
											"test_multicol");
	assert(td_test_multicol);

	td_test_dt = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
									  "test_dt");
	assert(td_test_dt);

	htp_dt = vh_hb_allocht(vh_hb(5), vh_td_htd(td_test_dt), 0);

	printf("\n\nAllocated HTP %ld to insert record to SQLite with vh_sync",
		   htp_dt);

	vh_GetDateNm(htp_dt, "b") = vh_ty_date2julian(2016, 12, 11);
	vh_GetInt32Nm(htp_dt, "a") = 29;
	vh_htp_assign_shard(htp_dt, shd, true);

	vh_sync_htp(htp_dt);
	vh_xact_commit(xact);
	vh_xact_destroy(xact);
}

static void
run_exec_query_ins_multi(void)
{
	TableDef td_test_dt = 0;
	HeapTuplePtr htp_dt = 0;
	SList htps;

	vh_htp_SListCreate(htps);

	XAct xact = vh_xact_create(Immediate); 

	td_test_dt = vh_cat_tbl_getbyname(ctx_catalog->catalogTable,
									  "test_dt");
	assert(td_test_dt);

	htp_dt = vh_hb_allocht(vh_hb(5), vh_td_htd(td_test_dt), 0);

	printf("\n\nAllocated HTP %ld to insert multi-record to SQLite with vh_sync",
		   htp_dt);

	vh_GetDateNm(htp_dt, "b") = vh_ty_date2julian(2016, 12, 12);
	vh_GetInt32Nm(htp_dt, "a") = 30;
	vh_htp_assign_shard(htp_dt, shd, true);

	vh_htp_SListPush(htps, htp_dt);	
	
	htp_dt = vh_hb_allocht(vh_hb(5), vh_td_htd(td_test_dt), 0);

	printf("\nAllocated HTP %ld to insert multi-record to SQLite with vh_sync",
		   htp_dt);

	vh_GetDateNm(htp_dt, "b") = vh_ty_date2julian(2016, 12, 13);
	vh_GetInt32Nm(htp_dt, "a") = 31;
	vh_htp_assign_shard(htp_dt, shd, true);

	vh_htp_SListPush(htps, htp_dt);
	
	htp_dt = vh_hb_allocht(vh_hb(5), vh_td_htd(td_test_dt), 0);

	printf("\nAllocated HTP %ld to insert multi-record to SQLite with vh_sync",
		   htp_dt);

	vh_GetDateNm(htp_dt, "b") = vh_ty_date2julian(2018, 12, 13);
	vh_GetInt32Nm(htp_dt, "a") = 1242924;
	vh_htp_assign_shard(htp_dt, shd, true);

	vh_htp_SListPush(htps, htp_dt);

	vh_sync(htps);
	vh_xact_commit(xact);
	vh_xact_destroy(xact);
}

