/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <stdio.h>

#include "io/vh.h>
#include "io/Launch.h>
#include "io/be/postgres/Postgres.h>
#include "io/catalog/BackEndCatalog.h>
#include "io/catalog/HeapTuple.h>
#include "io/catalog/TableDef.h>
#include "io/catalog/TableField.h>
#include "io/catalog/TableSet.h>
#include "io/executor/eplan.h>
#include "io/executor/exec.h>
#include "io/nodes/NodeQuerySelect.h>
#include "io/sql/InfoScheme.h>
#include "io/utils/stopwatch.h>
#include "io/utils/SList.h>

#define tbl_by_name(name) 	(vh_cat_table.GetTableByName(io_ctx->catalogTable, name))

static CatalogContext io_ctx = 0;
static ShardAccess io_sa = 0;
static Shard io_shd = 0;

static TableSet io_ts = 0;
static TableSetRel io_tsr_knb1 = 0;
static TableSetRel io_tsr_knvv = 0;
static TableSetRel io_tsr_knvp = 0;
static TableSetRel io_tsr_knvi = 0;

static void fetch_schema(void);
static void setup_shard_access(void);
static void setup_table_set(void);
static void fetch_root_records(void);
static void fetch_child_records(void);

int main(int argc, char *argv[])
{
	io_ctx = vh_Start(0, 0);

	if (!io_ctx)
		exit(1);

	setup_shard_access();
	fetch_schema();
	setup_table_set();
	fetch_root_records();
	fetch_child_records();
}


/*
 * setup_shard_access
 *
 * Hard-coded ShardAccess directive for accessing a Postgres database containing
 * the IDOC schema.
 */
static void
setup_shard_access(void)
{
	vh_sql_pgres_Register(io_ctx);

	io_shd = vhmalloc(sizeof(struct ShardData) + sizeof(ShardAccessData));
	io_sa = (ShardAccess)(io_shd + 1);

	io_sa-> be = vh_cat_be.GetByName(io_ctx->catalogBackEnd, "Postgres");
	io_sa->instance = vh_str.Convert("nest_dev");
	io_sa->port = vh_str.Convert("5432");
	io_sa->host = vh_str.Convert("127.0.0.1");
	io_sa->username = vh_str.Convert("postgres");
	io_sa->password = vh_str.Convert("c7M2wy4gt36d3YD");

	io_shd->access[0] = io_sa;
	io_shd->access[1] = io_sa;
	
	io_ctx->shard_general = io_shd;
}

/*
 * fetch_schema
 *
 * Fetches all tables in the IDOC schema on the target database and adds them to
 * the table catalog.
 */
static void
fetch_schema(void)
{
	SList schemas = vh_SListCreate();

	vh_SListPush(schemas, vh_str.Convert("idoc"));
	vh_sqlis_LoadSchema(io_ctx->catalogTable, io_shd, schemas);
}

/*
 * setup_table_set
 *
 * Sets up a table set for the CREMAS IDOC.
 */
static void
setup_table_set(void)
{
	TableDef td_outter, td_inner;
	TableField tf_outter, tf_inner;

	td_outter = tbl_by_name("cust_e1kna1m_ext");

	io_ts = vh_ts_create(td_outter, "E1KNA1M");

	if (io_ts)
	{
		/* KNB1 */
		td_inner = tbl_by_name("cust_e1knb1m_ext");
		tf_inner = vh_tbldef.FieldByName(td_inner, "_kunnr");
		tf_outter = vh_tbldef.FieldByName(td_outter, "_kunnr");

		io_tsr_knb1 = vh_tsr_root_create_child(io_ts, td_inner, "e1knb1m");
		vh_tsr_push_qual(io_tsr_knb1, tf_outter, tf_inner);

		/* KNVV */
		td_inner = tbl_by_name("cust_e1knvvm_ext");
	   	tf_inner = vh_tbldef.FieldByName(td_inner, "_kunnr");

		io_tsr_knvv = vh_tsr_root_create_child(io_ts, td_inner, "e1knvv1m");
		vh_tsr_push_qual(io_tsr_knvv, tf_outter, tf_inner);

		/* KNVP */
		td_outter = td_inner;
		td_inner = tbl_by_name("cust_e1knvpm_ext");
		tf_inner = vh_tbldef.FieldByName(td_inner, "_kunnr");
		tf_outter = vh_tbldef.FieldByName(td_outter, "_kunnr");

		io_tsr_knvp = vh_tsr_create_child(io_tsr_knvv, td_inner, "e1knvpm");
		vh_tsr_push_qual(io_tsr_knvp, tf_outter, tf_inner);

		tf_inner = vh_tbldef.FieldByName(td_inner, "vkorg");
	   	tf_outter = vh_tbldef.FieldByName(td_outter, "vkorg");
		vh_tsr_push_qual(io_tsr_knvp, tf_outter, tf_inner);	

		tf_inner = vh_tbldef.FieldByName(td_inner, "vtweg");
		tf_outter = vh_tbldef.FieldByName(td_outter, "vtweg");
		vh_tsr_push_qual(io_tsr_knvp, tf_outter, tf_inner);

		tf_inner = vh_tbldef.FieldByName(td_inner, "spart");
		tf_outter = vh_tbldef.FieldByName(td_outter, "spart");
		vh_tsr_push_qual(io_tsr_knvp, tf_outter, tf_inner);
		
		/* KNVI */
		td_inner = tbl_by_name("cust_e1knvim_ext");
		tf_inner = vh_tbldef.FieldByName(td_inner, "_kunnr");
		tf_outter = vh_tbldef.FieldByName(td_outter, "_kunnr");

		io_tsr_knvi = vh_tsr_create_child(io_tsr_knvv, td_inner, "e1knvim");
		vh_tsr_push_qual(io_tsr_knvi, tf_outter, tf_inner);

		tf_inner = vh_tbldef.FieldByName(td_inner, "vkorg");
	   	tf_outter = vh_tbldef.FieldByName(td_outter, "vkorg");
		vh_tsr_push_qual(io_tsr_knvi, tf_outter, tf_inner);	

		tf_inner = vh_tbldef.FieldByName(td_inner, "vtweg");
		tf_outter = vh_tbldef.FieldByName(td_outter, "vtweg");
		vh_tsr_push_qual(io_tsr_knvi, tf_outter, tf_inner);

		tf_inner = vh_tbldef.FieldByName(td_inner, "spart");
		tf_outter = vh_tbldef.FieldByName(td_outter, "spart");
		vh_tsr_push_qual(io_tsr_knvi, tf_outter, tf_inner);	
	}
}

/*
 * fetch_root_records
 *
 * Selects all records from the cust_e1kna1m_ext table and puts them in the
 * io_ts TableSet.
 */
static void
fetch_root_records(void)
{
	NodeQuerySelect nqsel;
	TableDef td;
	NodeFrom nf;
	ExecResult er;

	td = tbl_by_name("cust_e1kna1m_ext");

	nqsel = vh_sqlq_sel_create();
	nf = vh_sqlq_sel_from_add(nqsel, td, 0);
	vh_sqlq_sel_from_addfields(nqsel, nf, 0);

	er = vh_exec_node((Node)nqsel);

	vh_ts_root(io_ts, er->tups);
}

/*
 * fetch_child_records
 *
 * Fetches all child relationship records via vh_tsr_fetch rather than
 * vh_tsr_fetchall.
 */
static void
fetch_child_records(void)
{
	bool res;
	TableSetIter tsi;
	struct vh_stopwatch t1 = { };
	int counter = 0, root_count = 0;

	vh_stopwatch_start(&t1);
	res = vh_tsr_fetch(io_tsr_knb1);
	res = vh_tsr_fetch(io_tsr_knvv);
	res = vh_tsr_fetch(io_tsr_knvi);
	res = vh_tsr_fetch(io_tsr_knvp);
	vh_stopwatch_end(&t1);

	printf("\nProcessed all fetch operations in %ld milliseconds\n",
		   vh_stopwatch_ms(&t1));

	tsi = vh_tsi_first(io_ts);
	root_count = vh_tsi_root_count(tsi);

	vh_stopwatch_start(&t1);

	while (vh_tsi_next(tsi))
	{
		HeapTuple ht_kna1;
		HeapTuplePtr htp_kna1, htp_knb1, *htp_knb1_head;
		uint32_t knb1_sz, i;
		SList htps_knb1;

		counter++;

		htp_kna1 = vh_tsi_root_htp(tsi);
		htps_knb1 = vh_tsi_child_htps(tsi, io_tsr_knb1);

		const struct StringData* str_kna1_kunnr = vh_GetImStringNm(htp_kna1, "_kunnr");
		//printf("\n%s", str_kna1_kunnr->buffer);

		if (!htps_knb1)
			continue;

		knb1_sz = vh_SListIterator(htps_knb1, htp_knb1_head);
		for (i = 0; i < knb1_sz; i++)
		{
			const struct StringData *str_knb1_kunnr = vh_GetImStringNm(htp_knb1_head[i], "_kunnr");
			//printf("\n\t%s", str_knb1_kunnr->buffer);
		}
	}

	vh_stopwatch_end(&t1);

	printf("\nIteration for %d loops out of %d took %ld milliseconds\n", 
		   counter, 
		   root_count,
		   vh_stopwatch_ms(&t1));

	vh_tsi_destroy(tsi);
}

