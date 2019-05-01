/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/plan/pstmt.h"
#include "io/executor/estep.h"
#include "io/executor/estep_conn.h"
#include "io/utils/kvmap.h"

/*
 * ScanPutConn Infrastructure
 *
 * Used to put NodeConnection objects on a PlannedStmtShard node.
 */


struct ScanPutConnState
{
	ConnectionCatalog cc;
	volatile KeyValueMap kvm;
	vh_es_putconns_cb cb;
	void *cb_data;
	bool add_to_kvm;

	bool dropped;
};

static void es_putconns_recurse(ExecStep es, void *pcstate);


/*
 * ScanRelConn Infrastructure
 */

struct ScanRelConnState
{
	ConnectionCatalog cc;
	KeyValueMap kvm_exclude;
};

static void es_relconns_recurse(ExecStep es, void *rcstate);


/*
 * ScanPutConn Infrastructure
 */

bool
vh_es_putconns(ExecStep root, ConnectionCatalog cc,
			   KeyValueMap *kvm,
			   vh_es_putconns_cb cb,
			   void *cb_data,
			   bool add_to_kvm)
{
	struct ScanPutConnState spcs = { };
	vh_es_visit_tree_func vt_funcs[2];
	void *vt_data[2];
	KeyValueMap kvm_lcl;
	bool kvm_islcl;

	if (kvm)
	{
		if (*kvm)
		{
			kvm_islcl = false;
			kvm_lcl = *kvm;
			spcs.add_to_kvm = add_to_kvm;
		}
		else
		{
			kvm_lcl = vh_kvmap_create();
			*kvm = kvm_lcl;
			kvm_islcl = false;
			spcs.add_to_kvm = add_to_kvm;
		}
	}
	else
	{
		kvm_lcl = vh_kvmap_create();
		kvm_islcl = true;
		spcs.add_to_kvm = true;
	}

	vt_funcs[0] = es_putconns_recurse;
	vt_funcs[1] = 0;
	vt_data[0] = &spcs;
	vt_data[1] = 0;

	spcs.cc = cc;
	spcs.cb = cb;
	spcs.cb_data = cb_data;
	spcs.kvm = kvm_lcl;
	spcs.dropped = false;

	vh_es_visit_tree(root, vt_funcs, vt_data);

	if (kvm_islcl)
		vh_kvmap_destroy(kvm_lcl);

	return !spcs.dropped;
}

static void
es_putconns_recurse(ExecStep estep, void *data)
{
	struct ScanPutConnState *spcs = data;
	PlannedStmt pstmt;
	PlannedStmtShard pstmtshd;
	BackEndConnection *nconn_cc, *nconn_kvm, nconn;
	bool pstmt_found;

	pstmt_found = vh_es_pstmt(estep, &pstmt, &pstmtshd);

	if (pstmt_found)
	{
		if (!pstmtshd->nconn)
		{
			if (spcs->kvm)
				nconn_cc = vh_kvmap_find(spcs->kvm, &pstmtshd->sharda);
			else
				nconn_cc = 0;

			if (nconn_cc)
			{
				pstmtshd->nconn = *nconn_cc;

				if (spcs->cb)
					(spcs->cb)(spcs->cb_data, pstmt, pstmtshd,
							   pstmtshd->nconn, false);

				if (spcs->add_to_kvm)
				{
					vh_kvmap_value(spcs->kvm, &pstmtshd->sharda, nconn_kvm);
					*nconn_kvm = *nconn_cc;
				}
			}
			else
			{
				/*
				 * We're going to need to fetch a connection from the catalog.
				 */

				if (pstmtshd->sharda)
				{
					nconn = vh_ConnectionGet(spcs->cc, pstmtshd->sharda);	
				}
				else
				{
					nconn = 0;
					spcs->dropped = true;
				}

				if (nconn)
				{
					pstmtshd->nconn = nconn;

					if (spcs->cb)
						spcs->cb(spcs->cb_data, pstmt, pstmtshd, 
									 nconn, true);

					if (spcs->add_to_kvm)
					{
						vh_kvmap_value(spcs->kvm, &pstmtshd->sharda, nconn_kvm);
						*nconn_kvm = nconn;
					}
				}
			}
		}
	}
}

void
vh_es_relconns(ExecStep root, ConnectionCatalog cc,
			   KeyValueMap kvm_exclude)
{
	struct ScanRelConnState srcs = { };
	vh_es_visit_tree_func esv_funcs[2];
	void *esv_data[2];

	esv_funcs[0] = es_relconns_recurse;
	esv_funcs[1] = 0;
	esv_data[0] = &srcs;
	esv_data[1] = 0;

	srcs.kvm_exclude = kvm_exclude;
	srcs.cc = cc;

	vh_es_visit_tree(root, esv_funcs, esv_data);
}

static void
es_relconns_recurse(ExecStep root, void *data)
{
	struct ScanRelConnState *srcs = data;
	PlannedStmt pstmt;
	PlannedStmtShard pstmtshd;
	BackEndConnection nconn;
	bool pstmt_found, nconn_exists;

	pstmt_found = vh_es_pstmt(root, &pstmt, &pstmtshd);

	if (pstmt_found)
	{
		if (pstmtshd)
			nconn = pstmtshd->nconn;
		else
			nconn = 0;

		if (srcs->kvm_exclude && nconn)
			nconn_exists = vh_kvmap_exists(srcs->kvm_exclude, &nconn);
		else
			nconn_exists = false;

		if (!nconn_exists && nconn)
			vh_ConnectionReturn(srcs->cc, nconn);
	}
}

