/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/executor/xact.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQual.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/nodes/NodeQueryUpdate.h"
#include "io/utils/kvlist.h"
#include "io/utils/SList.h"

typedef struct SyncContextData *SyncContext;

/*
 * Key: TableDef, Value: HeapTuplePtr
 */
struct SyncContextData
{
	MemoryContext mctx_sc, mctx_old;
	KeyValueList kvl_ins;
	KeyValueList kvl_upd;
	XAct xact;
};

static bool sync_context_open(SyncContext sc);
static void sync_context_close(SyncContext sc);
static bool sync_htps_impl(SyncContext sc, SList htps);
static bool sync_fill_context(SyncContext sc, SList htps);
static bool sync_query(SyncContext sc);
static bool sync_query_update(SyncContext sc, TableDefVer tdv, SList htps);


static HeapTuplePtr create_heaptupleptr(CatalogContext cc, TableDefVer tdv);

HeapTuplePtr
vh_allochtp_nm(const char *table_name)
{
	CatalogContext cc;
	TableCatalog tc;
	TableDef td;
	HeapTuplePtr htp;

	cc = vh_ctx();
	tc = cc->catalogTable;
	td = vh_cat_tbl_getbyname(tc, table_name);

	if (!td)
	{
		elog(WARNING,
				emsg("Unable to find table named %s in the default catalog to "
					 "allocate a new HeapTuplePtr.",
					 table_name));

		return 0;
	}

	htp = create_heaptupleptr(cc, vh_td_tdv_lead(td));

	return htp;
}

HeapTuplePtr
vh_allochtp_td(TableDef td)
{
	CatalogContext cc;
	HeapTuplePtr htp;

	cc = vh_ctx();
	htp = create_heaptupleptr(cc, vh_td_tdv_lead(td));

	return htp;
}

static HeapTuplePtr
create_heaptupleptr(CatalogContext cc, TableDefVer tdv)
{
	XAct xact;
	
	xact = cc->xactCurrent;

	if (xact)
	{
		return vh_xact_createht(xact, tdv);
	}

	return vh_hb_allocht(vh_hb(cc->hbno_general), &tdv->heap, 0);
}


/*
 * Takes a list of HeapTuplePtr and determines how to get them flushed to the
 * underlying back ends.  This may be an insert or an update.
 *
 * Attributes on the TableDef may also impact how this works.  Can be called
 * with or without a transaction context.
 */
bool
vh_sync(SList htps)
{
	struct SyncContextData sc = { };

	if (sync_context_open(&sc))
	{
		sync_fill_context(&sc, htps);
		sync_query(&sc);
		sync_context_close(&sc);

		return true;
	}
	else
	{
		elog(ERROR2,
			 emsg("vh_sync could not open a context to attempt a syncronization"));
	}

	return false;
}

bool
vh_sync_htp(HeapTuplePtr htp)
{
	bool sync;
	SList htps;

	vh_htp_SListCreate(htps);
	vh_htp_SListPush(htps, htp);
	sync = vh_sync(htps);
	vh_SListDestroy(htps);

	return sync;
}

static bool
sync_context_open(SyncContext sc)
{
	CatalogContext cc = vh_ctx();

	if (cc)
	{
		sc->mctx_old = vh_mctx_current();
		sc->mctx_sc = vh_MemoryPoolCreate(sc->mctx_old, 1024,
										  "Sync context");
		sc->xact = cc->xactCurrent;
		vh_mctx_switch(sc->mctx_sc);

		if (sc->xact)
			return true;

		vh_mctx_switch(sc->mctx_old);
		vh_mctx_destroy(sc->mctx_sc);

		sc->mctx_sc = 0;

		elog(ERROR2,
			 emsg("No open transaction to perform a syncronization with!"));
	}

	return false;
}

static void
sync_context_close(SyncContext sc)
{
	if (sc->mctx_sc)
		vh_mctx_destroy(sc->mctx_sc);

	vh_mctx_switch(sc->mctx_old);
}

static bool
sync_fill_context(SyncContext sc, SList htps)
{
	HeapTuplePtr *htp_head, htp;
	uint32_t htp_sz, i;
	HeapTuple ht;
	TableDefVer tdv;
	SList values;

	htp_sz = vh_SListIterator(htps, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];
		ht = vh_htp(htp);
		tdv = (TableDefVer) ht->htd;

		if (!(vh_ht_flags(ht) & VH_HT_FLAG_FETCHED))
		{
			if (!sc->kvl_ins)
				sc->kvl_ins = vh_htp_kvlist_create();

			/*
			 * We should call the key generator to create a new key if the
			 * TableDef defines one.
			 */

			vh_kvlist_value(sc->kvl_ins, &tdv, values);		
			vh_htp_SListPush(values, htp);
		}
		else
		{
			if (!sc->kvl_upd)
				sc->kvl_upd = vh_htp_kvlist_create();
			
			vh_kvlist_value(sc->kvl_upd, &tdv, values);
			vh_htp_SListPush(values, htp);
		}
	}

	return true;
}

static bool
sync_query(SyncContext sc)
{
	NodeQueryInsert nqins;
	NodeFrom nfrom;
	TableDefVer *tdv;
	SList *htps;
	ExecResult er = 0;
	KeyValueListIterator it;
	bool query_queued = true;

	if (sc->kvl_ins)
	{
		/*
		 * We should really order the inserts based on their relationships in
		 * the back end just in case constraints have been defined on the 
		 * tables.  We'd do this using the relationships, inserting the top
		 * level "parents" first and then children.
		 */
		vh_kvlist_it_init(&it, sc->kvl_ins);

		while (vh_kvlist_it_next(&it, &tdv, &htps))
		{
			nqins = vh_sqlq_ins_create();
			nfrom = vh_sqlq_ins_table(nqins, (*tdv)->td);
			nfrom->htps = *htps;
			
			query_queued &= vh_xact_node(sc->xact, (NodeQuery)nqins, &er);
		}
	}

	if (sc->kvl_upd)
	{
		vh_kvlist_it_init(&it, sc->kvl_upd);

		while (vh_kvlist_it_next(&it, &tdv, &htps))
		{
			if (!sync_query_update(sc, *tdv, *htps))
			{
				query_queued &= false;
			}
		}
	}

	return query_queued;
}

static bool
sync_query_update(SyncContext sc, TableDefVer tdv, SList htps)
{
	NodeQueryUpdate nq_upd;
	NodeQual nq_upd_qual;
	HeapTuplePtr htp, *htp_head;
	TableDef td;
	ExecResult er;
	int32_t htp_sz, i, j;

	td = tdv->td;
	htp_sz = vh_SListIterator(htps, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];

		nq_upd = vh_sqlq_upd_create();
		(void)vh_sqlq_upd_from(nq_upd, td);

		nq_upd->htp = htp;

		/*
		 * Add the WHERE clause based on the Primary Key
		 */
		if (tdv->key_primary.nfields)
		{
			for (j = 0; j < tdv->key_primary.nfields; j++)
			{
				nq_upd_qual = vh_nsql_qual_create(And, Eq);

				vh_nsql_qual_lhs_tf_set(nq_upd_qual, tdv->key_primary.fields[j]);
				vh_nsql_qual_rhs_tvs_set(nq_upd_qual);

				vh_tvs_store_htp_hf(vh_nsql_qual_rhs_tvs(nq_upd_qual),
									htp,
									(HeapField)tdv->key_primary.fields[j]);

				vh_sqlq_upd_qual_add(nq_upd, nq_upd_qual);
			}
		}

		if (!vh_xact_node(sc->xact, (NodeQuery)nq_upd, &er))
		{
			//vh_nsql_destroytree((Node)nq_upd, 0);

			return false;
		}
	}

	return true;
}

