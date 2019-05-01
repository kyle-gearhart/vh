/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/executor/param.h"
#include "io/plan/pstmt.h"
#include "io/plan/pstmt_funcs.h"
#include "io/shard/Shard.h"
#include "io/utils/SList.h"


PlannedStmt
vh_pstmt_generate_from_query(NodeQuery nq, BackEnd be)
{
	PlannedStmt pstmt;

	pstmt = vhmalloc(sizeof(struct PlannedStmtData));
	pstmt->shards = vh_SListCreate();
	pstmt->nquery = nq;
	pstmt->be = be;
	pstmt->latebinding = false;
	pstmt->latebindingset = false;

	return pstmt;
}

PlannedStmtShard
vh_pstmtshd_generate(PlannedStmt pstmt, 
					 Shard shd, ShardAccess shda)
{
	PlannedStmtShard pstmtshd;
	BackEnd be;
	TypeVarSlot *param_values;
	Parameter param;
	int32_t i;

	pstmtshd = vhmalloc(sizeof(struct PlannedStmtShardData));
	pstmtshd->shard = shd;
	pstmtshd->sharda = shda;
	pstmtshd->paramcount = 0;
	pstmtshd->parameters = 0;
	pstmtshd->nconn = 0;

	be = pstmt->be;

	if (vh_be_command(be, &pstmt->nquery->node, &pstmtshd->command,
					  /* Parameters */
					  0, &param_values, &pstmtshd->paramcount))
	{
		/*
		 * We were successful generating a command.
		 *
		 * Now all we have to do is unpack the parameters.
		 */

		if (pstmtshd->paramcount)
		{
			pstmtshd->parameters = vh_param_createlist();
		}

		for (i = 0; i < pstmtshd->paramcount; i++)
		{
			if (vh_be_param(be, 
							pstmtshd->parameters, 
							&param_values[i], 
							&param))
			{
				vh_param_add(pstmtshd->parameters, param);
			}
		}

		if (param_values)
			vhfree(param_values);
	}

	return pstmtshd;
}

bool
vh_pstmt_generate_from_query_str(BackEndConnection bec,
								 const char *query,
								 PlannedStmt *ps,
								 PlannedStmtShard *psa,
								 TypeVarSlot *params,
								 int32_t n_params)
{
	PlannedStmt pstmt;
	PlannedStmtShard pstmtshd;
	TableDef td;
	Parameter param;
	int32_t i;

	pstmt = vhmalloc(sizeof(struct PlannedStmtData));

	vh_pstmt_init(pstmt);

	td = vh_td_create(false);

	pstmt->latebinding = true;
	pstmt->qrp_ntables = 1;
	pstmt->qrp_table = vhmalloc(sizeof(struct QueryResultTableProjectionData));
	pstmt->qrp_table[0].rtdv = vh_td_tdv_lead(td);
	pstmt->qrp_table[0].nfrom = 0;

	pstmt->be = bec->be;

	pstmtshd = vhmalloc(sizeof(struct PlannedStmtShardData));

	vh_pstmtshard_init(pstmtshd);

	pstmtshd->command = vh_str.Convert(query);
	pstmtshd->nconn = bec;

	*ps = pstmt;
	*psa = pstmtshd;

	if (n_params)
	{
		/*
		 * Build out our parameters if necessary.
		 */

		pstmtshd->parameters = vh_param_createlist();

		for (i = 0; i < n_params; i++)
		{
			if (vh_be_param(pstmt->be, pstmtshd->parameters, &params[i], &param))
			{
				vh_param_add(pstmtshd->parameters, param);
			}
		}
	}

	return 0;
}

void
vh_pstmt_init(PlannedStmt pstmt)
{
	memset(pstmt, 0, sizeof(struct PlannedStmtData));
}

void
vh_pstmtshard_init(PlannedStmtShard pstmts)
{
	memset(pstmts, 0, sizeof(struct PlannedStmtShardData));
}

/*
 * Takes a PlannedStmt and attempts to generate a query result projection (QRP).
 *
 * Returns 0 if succesful; anything else indicates the QRP was not succesful.
 */
int32_t
vh_pstmt_qrp(PlannedStmt pstmt)
{
	struct QueryResultProjectionData qrp;

	if (pstmt && pstmt->nquery && pstmt->be)
	{
		qrp = vh_plan_qrp(pstmt->nquery,
						  pstmt->be,
						  VH_PLAN_QRP_TABLES | VH_PLAN_QRP_FIELDS | VH_PLAN_QRP_BACKEND);

		pstmt->qrp_ntables = qrp.ntables;
		pstmt->qrp_nfields = qrp.nfields;
		pstmt->qrp_field = qrp.fields;
		pstmt->qrp_table = qrp.tables;
		pstmt->qrp_backend = qrp.backend;

		return 0;
	}

	return -1;
}

int32_t
vh_pstmt_qrp_be(PlannedStmt pstmt,
				QrpTableProjection qrpt, QrpFieldProjection qrpf,
				int32_t n_qrpt, int32_t n_qrpf)
{
	struct QueryResultProjectionData qrp = { };

	if (pstmt && pstmt->nquery && pstmt->be)
	{
		qrp = vh_plan_qrp(pstmt->nquery,
						  pstmt->be,
						  VH_PLAN_QRP_BACKEND);

		pstmt->qrp_ntables = n_qrpt;
		pstmt->qrp_nfields = n_qrpf;
		pstmt->qrp_table = qrpt;
		pstmt->qrp_field = qrpf;
		pstmt->qrp_backend = qrp.backend;

		return 0;
	}

	return -1;
}


int32_t
vh_pstmt_lb_qrp(PlannedStmt pstmt)
{
	return vh_plan_qrp_lb(pstmt);
}

/*
 * vh_pstmt_lb_add_col
 *
 * Add the column to the HeapTupleDef, then call vh_pstmt_lb_qrp.
 */
int32_t
vh_pstmt_lb_add_col(PlannedStmt pstmt, const char* fname, Type *tys)
{
	TableField tf;

	tf = vh_tdv_tf_add(pstmt->qrp_table[0].rtdv, tys, fname);

	return tf ? 0 : -1;
}

