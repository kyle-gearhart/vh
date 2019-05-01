/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include "vh.h"
#include "io/buffer/HeapBuffer.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQual.h"
#include "io/nodes/NodeQueryUpdate.h"
#include "io/nodes/NodeUpdateField.h"
#include "io/utils/SList.h"


/*
 * Node Ops Funcs
 */

static Node nsql_qupd_copy(struct NodeCopyState *cstate, Node n_source,
						   uint64_t flags);
static bool nsql_qupd_check(Node node);
static void nsql_qupd_destroy(Node node);
static bool nsql_qupd_to_sql(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_qupd_funcs = {
	.copy = nsql_qupd_copy,
	.check = nsql_qupd_check,
	.destroy = nsql_qupd_destroy,
	.to_sql_cmd = nsql_qupd_to_sql
};


/*
 * to_sql_cmd helper data structures and functions
 *
 * We have to iterate the NodeField nodes a little differently due to the
 * assignment nature of an UPDATE statement.  We choose to do this internally
 * rather than rely on external functions do the qual assignments.
 */

struct NodeQueryUpdateFieldCtx
{
	String cmd;
	NodeSqlCmdContext cmdctx;
	bool first;
};

static void nsql_qupd_to_sql_field(Node node, void *data);

static Node
nsql_qupd_copy(struct NodeCopyState *cstate, Node n_source,
			   uint64_t flags)
{
	return 0;
}

static bool
nsql_qupd_check(Node node)
{
	return true;
}

static void
nsql_qupd_destroy(Node node)
{
}

NodeQueryUpdate
vh_sqlq_upd_create(void)
{
	NodeQueryUpdate nqupd = vh_sqlq_create(Update, &nsql_qupd_funcs,
										   sizeof(struct NodeQueryUpdateData));

	nqupd->nfields = 0;
	nqupd->nfrom = 0;
	nqupd->nquals = 0;

	return nqupd;
}

NodeFrom
vh_sqlq_upd_from(NodeQueryUpdate nqupd, TableDef td)
{
	NodeFrom nfrom;
	TableDefVer tdv = vh_td_tdv_lead(td);

	if (nqupd->nfrom)
	{
		nfrom = nqupd->nfrom;
	
		if (nfrom->tdv == tdv)
		{
			return nfrom;
		}
		else
		{
			elog(WARNING,
				 emsg("NodeQueryUpdate [%p] already has a FROM node set",
					  nqupd));

			return 0;
		}
	}
	else
	{
		nfrom = vh_nsql_from_create();
		nfrom->tdv = tdv;

		nqupd->nfrom = nfrom;

		vh_nsql_child_rappend((Node)nqupd, (Node)nfrom);
	}

	return nfrom;
}

void 
vh_sqlq_upd_qual_add(NodeQueryUpdate nq, NodeQual nqual)
{
	if (!nq->nquals)
	{
		nq->nquals = vh_nsql_create(QualList, 0, sizeof(struct NodeData));
		nq->nquals->tag = QualList;
	}

	vh_nsql_child_rappend(nq->nquals, (Node)nqual);
}

void 
vh_sqlq_upd_field_add(NodeQueryUpdate nq, NodeUpdateField nuf)
{
	if (!nq->nfields)
	{
		nq->nfields = vh_nsql_create(FieldList, 0, sizeof(struct NodeData));
		nq->nfields->tag = FieldList;
	}

	vh_nsql_child_rappend(nq->nfields, (Node)nuf);
}

static bool 
nsql_qupd_to_sql(String cmd, void *node, NodeSqlCmdContext ctx)
{
	NodeQueryUpdate nqu = node;

	if (!nqu->nfrom)
		elog(ERROR2,
			 emsg("Missing FROM table during NodeQueryUpdate to_sql_cmd!"));

	vh_str.Append(cmd, "UPDATE ");
	vh_nsql_cmd_impl(&nqu->nfrom->node, cmd, ctx, false);
	vh_str.Append(cmd, " SET ");

	if (nqu->nfields)
	{
		vh_nsql_cmd_impl(nqu->nfields, cmd, ctx, true);
	}

	if (nqu->nquals)
	{
		vh_str.Append(cmd, " WHERE ");
		vh_nsql_cmd_impl(nqu->nquals, cmd, ctx, true);
	}

	return true;
}

static void 
nsql_qupd_to_sql_field(Node node, void *data)
{
	NodeUpdateField nf = 0;
	struct NodeQueryUpdateFieldCtx *fctx = data;

	if (node->tag != Field)
		return;

	nf = (NodeUpdateField) node;

	if (!nf->tf)
		elog(ERROR2,
			 emsg("Corrupt NodeField node detected while forming an UPDATE "
				  "SQL statement!"));

	if (fctx->first)
		fctx->first = false;
	else
		vh_str.Append(fctx->cmd, ", ");

	vh_nsql_cmd_impl(node, fctx->cmd, fctx->cmdctx, false);
	vh_str.Append(fctx->cmd, " = ");
	vh_nsql_cmd_param_placeholder(fctx->cmd, fctx->cmdctx, &nf->tvs);
}

