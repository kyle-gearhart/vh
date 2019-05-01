/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/be/postgres/Postgres.h"
#include "io/buffer/HeapBuffer.h"
#include "io/catalog/BackEndCatalog.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/types/Array.h"
#include "io/catalog/types/Date.h"
#include "io/catalog/types/DateTime.h"
#include "io/executor/eresult.h"
#include "io/executor/xact.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/shard/Shard.h"
#include "io/sql/InfoScheme.h"
#include "io/utils/SList.h"
#include "io/utils/stopwatch.h"

#include "test.h"
#include "tbe.h"

struct tuple_customer
{
	struct StringData firstname;
	struct StringData lastname;
	int32_t identifier;
};


static void setup_beacon(void);
static void configure_from_is(void);
static void create_tuple_customer(XAct xact);
static void attempt_query(XAct xact);
static void attempt_query_pgclass(XAct xact);
static void attempt_query_perf(XAct xact);
static void attempt_ins_perf(XAct xact);

static void query_date(XAct xact);
static void query_daterange(XAct xact);

static void query_datetime(XAct xact);
static void query_datetimerange(XAct xact);

static void query_intarray(XAct xact);

static void query_tbe(void);
static void query_i32(BackEndConnection bec);
static void query_i64(BackEndConnection bec);
static void query_double(BackEndConnection bec);

static void heapbuffer_allocate(void);


void test_query(void)
{
	XAct txact, lxact, sxact;
	int32_t i = 0;

	txact = vh_xact_create(Immediate);
	
	setup_beacon();
	configure_from_is();

	//heapbuffer_allocate();

	for (i = 0; i < 1; i++)
	{
		lxact = vh_xact_create(Immediate);
		//attempt_ins_perf(xact);
		attempt_query_perf(lxact);
		attempt_query(lxact);
		vh_xact_rollback(lxact);
		//vh_xact_destroy(lxact);
	}

	sxact = vh_xact_create(Immediate);

	create_tuple_customer(sxact);
	query_date(sxact);
	query_daterange(sxact);
	query_datetime(sxact);
	query_datetimerange(sxact);
	//query_intarray(xact);
	//

	vh_xact_commit(sxact);
	//vh_xact_destroy(sxact);
	
	vh_xact_commit(txact);
	vh_xact_destroy(txact);
	
	query_tbe();
}

static void
setup_beacon(void)
{
	BackEndCredentialVal becredval = { };
	BackEndCredential becred;
	ShardAccess sa;
	Shard shard;
	BackEnd be;

	strcpy(&becredval.username[0], "postgres");
	strcpy(&becredval.password[0], "^N[D.:;vH<73aq7:");
	strcpy(&becredval.hostname[0], "127.0.0.1");
	strcpy(&becredval.hostport[0], "5432");

	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_USERNAME);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_PASSWORD);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_HOSTNAME);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_HOSTPORT);

	becred = vh_be_cred_create(BECSM_PlainText);
	vh_be_cred_store(becred, &becredval);

	vh_sql_pgres_Register(ctx_catalog);

	be = vh_cat_be_getbyname(ctx_catalog->catalogBackEnd, "Postgres");
	sa = vh_sharda_create(becred, be);
	sa->database = vh_str.Convert("vh_test");

	shard = vh_shard_create((ShardId){}, sa, sa); 

	ctx_catalog->shard_general = shard;
}

static void
configure_from_is(void)
{
	String schema;

	schema = vh_str.Convert("public");
	vh_sqlis_loadshardschema(ctx_catalog->catalogTable, 
							 ctx_catalog->shard_general, 
							 schema);
	vh_str.Destroy(schema);
}

static void
create_tuple_customer(XAct xact)
{
	TableDef td_customer;
	HeapTuple ht, ht_a, ht_m;
	HeapTuplePtr htp, htp_a;
	struct tuple_customer *cust, *cust_a;

	td_customer = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "customer");
	assert(td_customer);
	assert(!strcmp("customer", vh_str_buffer(td_customer->tname)));

	if (td_customer)
	{
		htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general),
							vh_td_htd(td_customer),
							&ht);
		ht_m = vh_htp(htp);

		assert(vh_ht_flags(ht_m) & VH_HT_FLAG_MUTABLE);
		assert(~vh_ht_flags(ht) & VH_HT_FLAG_MUTABLE);

		cust = vh_ht_tuple(ht_m);
		cust->identifier = 32;
		vh_str.Assign(&cust->firstname, "test");
		vh_str.Assign(&cust->lastname, "bobby leroy sends it 290 yards thru the air");

		vh_sync_htp(htp);
		vh_xact_commit(xact);

		htp_a = vh_hb_copyht(htp, ht, &ht_a);
		cust_a = vh_htp_tuple(htp_a);
		cust_a->identifier = 45;
		vh_str.Assign(&cust_a->lastname, "TRUMP");
		vh_str.Assign(&cust_a->firstname, "THE DONALD");

		vh_sync_htp(htp_a);

		attempt_query(xact);
	}
}

static void
attempt_query(XAct xact)
{
	MemoryContext mctx, mctx_old;
	NodeQuerySelect nselect;
	NodeFrom nfrom;
	ExecResult eres;
	HeapTuplePtr *htp_head, htp;
	TableDef td_prefload;
	uint32_t htsz, i, stot;

	mctx = vh_MemoryPoolCreate(vh_mctx_current(), 8192, "attempt_query");
	mctx_old = vh_mctx_switch(mctx);

	td_prefload = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "prefload");

	nselect = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nselect, td_prefload, 0);
	vh_sqlq_sel_from_addfields(nselect, nfrom, 0);

	vh_xact_node(xact, (NodeQuery)nselect, &eres);

	if (eres && eres->tups)
	{
		htsz = vh_SListIterator(eres->tups, htp_head);

		stot = htsz && htsz > 5000 ? 5000 : 0;

		for (i = stot - 10; i < stot; i++)
		{
			htp = htp_head[i];
/*
			vh_HTP_PRINTERR(htp);

			printf("\tpref_lastname: %s\t"
				   "pref_fname: %s\n",
				   vh_str_buffer(vh_GetImStringNm(htp_head[i], "prefload_lastname")),
				   vh_str_buffer(vh_GetImStringNm(htp_head[i], "prefload_fname")));
*/
			if (htp)
			{
			}
		}

		vh_exec_result_finalize(eres, true);
	}

	vh_mctx_switch(mctx_old);
	vh_mctx_destroy(mctx);
}

static void
attempt_query_pgclass(XAct xact)
{
	NodeQuerySelect nselect;
	NodeFrom nfrom;
	ExecResult eres;
	TableDef td_pgclass;

	td_pgclass = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "pg_type");

	nselect = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nselect, td_pgclass, 0);
	vh_sqlq_sel_from_addfields(nselect, nfrom, 0);

	vh_xact_node(xact, (NodeQuery)nselect, &eres);
}

static void
attempt_query_perf(XAct xact)
{
	NodeQuerySelect nselect;
	NodeFrom nfrom;
	ExecResult eres;
	TableDef td_pgclass;

	td_pgclass = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "test_perf_int4");

	nselect = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nselect, td_pgclass, 0);
	vh_sqlq_sel_from_addfields(nselect, nfrom, 0);

	vh_xact_node(xact, (NodeQuery)nselect, &eres);
}

static void
attempt_ins_perf(XAct xact)
{
	NodeQueryInsert ninsert;
	SList tups;
	TableDef td_perf_int4 = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "test_perf_ins_int4");
	TableField tf = vh_td_tf_name(td_perf_int4, "cola");
	int32_t i, *val;
	HeapTuplePtr htp;
	HeapTuple ht;
	ExecResult eres;
	struct vh_stopwatch sw;

	ninsert = vh_sqlq_ins_create();
	vh_sqlq_ins_table(ninsert, td_perf_int4);

	vh_htp_SListCreate(tups);

	for (i = 0; i < 100000; i++)
	{
		htp = vh_xact_createht(xact, vh_td_tdv_lead(td_perf_int4));
		ht = vh_htp_flags(htp, 0);
		val = (int32_t*)vh_ht_field(ht, &tf->heap);
		vh_htf_clearnull(ht, &tf->heap);

		*val = i;
		vh_htp_SListPush(tups, htp);
	}

	vh_sqlq_ins_htp_list(ninsert, tups);
	vh_stopwatch_start(&sw);
	vh_xact_node(xact, (NodeQuery)ninsert, &eres);
	vh_xact_commit(xact);
	vh_stopwatch_end(&sw);

	printf("\ninsert on the wall clock %ld ms",
			vh_stopwatch_ms(&sw));
}

static void
query_date(XAct xact)
{
	TableDef td_date;
	NodeQuerySelect nselect;
	NodeFrom nfrom;
	ExecResult eres;
	HeapTuplePtr htp;
	HeapTuple ht;
	uint32_t ht_sz;

	td_date = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "td_date");
	assert(td_date);
	assert(!strcmp("td_date", vh_str_buffer(td_date->tname)));

	/*
	 * Create new td_date HeapTuple and then populate the start_date column to
	 * May 28, 2016.  Sync it with the back end.
	 */
	htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general), vh_td_htd(td_date), &ht);
	vh_GetDateNm(htp, "start_date") = vh_ty_date2julian(2016, 5, 28);
	vh_sync_htp(htp);

	nselect = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nselect, td_date, 0);
	vh_sqlq_sel_from_addfields(nselect, nfrom, 0);

	vh_xact_node(xact, (NodeQuery)nselect, &eres);

	if (eres && eres->tups)
	{
		ht_sz = vh_SListSize(eres->tups);
		//assert(ht_sz == 1);
		//
		
		if (ht_sz)
		{
		}
	}
}

static void
query_daterange(XAct xact)
{
	TableDef td_daterange;
	NodeQuerySelect nselect;
	NodeFrom nfrom;
	ExecResult eres;
	HeapTuplePtr *htp_head, htp;
	HeapTuple ht;
	uint32_t ht_sz;
	DateRange dr;
	const struct DateRangeData *drim;

	td_daterange = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "td_dater");
	assert(td_daterange);
	assert(!strcmp("td_dater", vh_str_buffer(td_daterange->tname)));

	/*
	 * Create new td_daterange HeapTuple and then populate the start_date column to
	 * May 28, 2016.  Sync it with the back end.
	 */
	htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general), vh_td_htd(td_daterange), &ht);
	dr = vh_GetDateRangeNm(htp, "validity"); 
	dr->start = vh_ty_date2julian(2016, 5, 28);
	dr->end = vh_ty_date2julian(2016, 12, 31);
	dr->flags = 0;
	vh_sync_htp(htp);

	nselect = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nselect, td_daterange, 0);
	vh_sqlq_sel_from_addfields(nselect, nfrom, 0);

	vh_xact_node(xact, (NodeQuery)nselect, &eres);

	if (eres && eres->tups)
	{
		ht_sz = vh_SListIterator(eres->tups, htp_head);

		if (ht_sz)
		{
			htp = htp_head[0];
			drim = vh_ReadDateRangeImNm(htp, "validity");

			assert(drim->start == vh_ty_date2julian(2016, 5, 29));
			assert(drim->end == vh_ty_date2julian(2016, 12, 31));
		}
	}
}

static void
query_datetime(XAct xact)
{
	TableDef td_datetime;
	NodeQuerySelect nselect;
	NodeFrom nfrom;
	ExecResult eres;
	HeapTuplePtr htp;
	HeapTuple ht;
	uint32_t ht_sz;
	struct DateTimeSplit dts;

	td_datetime = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "td_datetime");
	assert(td_datetime);
	assert(!strcmp("td_datetime", vh_str_buffer(td_datetime->tname)));

	/*
	 * Create new td_datetime HeapTuple and then populate the start_datetime 
	 * column to May 28, 2016.  Sync it with the back end.
	 */
	htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general), vh_td_htd(td_datetime), &ht);

	dts.year = 2016;
	dts.month = 5;
	dts.month_day = 29;
	dts.hour = 8;
	dts.minutes = 25;
	dts.seconds = 55;

	vh_GetDateTimeNm(htp, "start_datetime") = vh_ty_ts2datetime(&dts);
	vh_sync_htp(htp);

	nselect = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nselect, td_datetime, 0);
	vh_sqlq_sel_from_addfields(nselect, nfrom, 0);

	vh_xact_node(xact, (NodeQuery)nselect, &eres);

	if (eres && eres->tups)
	{
		ht_sz = vh_SListSize(eres->tups);
		//assert(ht_sz == 1);
		//
		
		if (ht_sz)
		{
		}
	}
}

static void
query_datetimerange(XAct xact)
{
	TableDef td_datetimerange;
	NodeQuerySelect nselect;
	NodeFrom nfrom;
	ExecResult eres;
	HeapTuplePtr *htp_head, htp;
	HeapTuple ht;
	uint32_t ht_sz;
	DateTimeRange dtr;
	const struct DateTimeRangeData *dtrim;
	struct DateTimeSplit dts_start = { }, dts_end = { };

	td_datetimerange = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "td_datetimer");
	assert(td_datetimerange);
	assert(!strcmp("td_datetimer", vh_str_buffer(td_datetimerange->tname)));

	/*
	 * Create new td_daterange HeapTuple and then populate the start_date column to
	 * May 28, 2016.  Sync it with the back end.
	 */
	htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general), vh_td_htd(td_datetimerange), &ht);
	dtr = vh_GetDateTimeRangeNm(htp, "validity"); 
	
	dts_start.year = 2016;
	dts_start.month = 5;
	dts_start.month_day = 29;
	dts_start.hour = 8;
	dts_start.minutes = 33;
	dts_start.seconds = 33;

	dts_end.year = 2018;
	dts_end.month = 2;
	dts_end.month_day = 28;
	dts_end.hour = 23;
	dts_end.minutes = 59;
	dts_end.seconds = 59;
	
	dtr->start = vh_ty_ts2datetime(&dts_start);
	dtr->end = vh_ty_ts2datetime(&dts_end);
	dtr->flags = VH_TY_RANGE_LInclusive | VH_TY_RANGE_UInclusive;
	vh_sync_htp(htp);

	nselect = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nselect, td_datetimerange, 0);
	vh_sqlq_sel_from_addfields(nselect, nfrom, 0);

	vh_xact_node(xact, (NodeQuery)nselect, &eres);

	if (eres && eres->tups)
	{
		ht_sz = vh_SListIterator(eres->tups, htp_head);

		if (ht_sz)
		{
			htp = htp_head[0];
			dtrim = vh_GetDateTimeRangeImNm(htp, "validity");

			if (dtrim)
			{
			}
		}
	}
}

static void
query_intarray(XAct xact)
{
	TableDef td_intarray;
	NodeQuerySelect nselect;
	NodeFrom nfrom;
	ExecResult eres;
	HeapTuplePtr *htp_head, htp;
	HeapTuple ht;
	uint32_t ht_sz;
	Array array;
	int32_t *iptr;

	td_intarray = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "td_intarr");
	assert(td_intarray);
	assert(!strcmp("td_intarr", vh_str_buffer(td_intarray->tname)));

	htp = vh_hb_allocht(vh_hb(ctx_catalog->hbno_general), vh_td_htd(td_intarray), &ht);
	ht = vh_htp(htp);
	
	array = vh_GetArrayNm(htp, "vals");
	iptr = vh_ty_array_emplace(array);
	*iptr = 10;
	iptr = vh_ty_array_emplace(array);
	*iptr = 20;

	vh_sync_htp(htp);

	nselect = vh_sqlq_sel_create();
	nfrom = vh_sqlq_sel_from_add(nselect, td_intarray, 0);
	vh_sqlq_sel_from_addfields(nselect, nfrom, 0);

	vh_xact_node(xact, (NodeQuery)nselect, &eres);

	if (eres && eres->tups)
	{
		ht_sz = vh_SListIterator(eres->tups, htp_head);

		if (ht_sz)
		{
			htp = htp_head[0];
		}
	}
}

static void
heapbuffer_allocate(void)
{
	HeapTuple ht;
	TableDef td_prefload;
	TableDefVer tdv_prefload;
	int i;

	td_prefload = vh_cat_tbl_getbyname(ctx_catalog->catalogTable, "prefload");
	tdv_prefload = vh_td_tdv_lead(td_prefload);

	for (i = 0; i < 50000; i++)
		vh_hb_allocht(vh_hb(vh_ctx()->hbno_general), &tdv_prefload->heap, &ht); 
}

static void
query_tbe(void)
{
	BackEndConnection bec;

	bec = vh_ConnectionGet(ctx_catalog->catalogConnection, 
						   ctx_catalog->shard_general->access[0]);

	if (bec)
	{
		query_i32(bec);
		query_i64(bec);
		query_double(bec);
	}

	vh_ConnectionReturn(ctx_catalog->catalogConnection, bec);
}

static void
query_i32(BackEndConnection bec)
{
	TypeVarSlot **slots;
	int32_t res;

	slots = tbe_alloc_expectedv(1, 4);

	vh_tvs_store_i32(&slots[0][0], 1);
	vh_tvs_store_i32(&slots[0][1], -10);
	vh_tvs_store_i32(&slots[0][2], 48518922);
	vh_tvs_store_i32(&slots[0][3], 0);

	res = tbe_sql_command_cycle(bec, "test_i32", slots, 1, 4, 0);
}

static void
query_i64(BackEndConnection bec)
{
	TypeVarSlot **slots;
	int32_t res;

	slots = tbe_alloc_expectedv(1, 4);

	vh_tvs_store_i64(&slots[0][0], 2147483648);
	vh_tvs_store_i64(&slots[0][1], -2147483649);
	vh_tvs_store_i64(&slots[0][2], 394902010201029);
	vh_tvs_store_i64(&slots[0][3], 0);

	res = tbe_sql_command_cycle(bec, "test_i64", slots, 1, 4, 0);
}

static void 
query_double(BackEndConnection bec)
{
	TypeVarSlot **slots;
	int32_t res;

	slots = tbe_alloc_expectedv(1, 2);

	vh_tvs_store_double(&slots[0][0], 15.12);
	vh_tvs_store_double(&slots[0][1], -249.99);

	res = tbe_sql_command_cycle(bec, "test_double", slots, 1, 2, 0);
}

