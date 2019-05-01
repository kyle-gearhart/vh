/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <stdio.h>

#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeQual.h"


static const char* QualOperatorStr [] = {
	" < ", " <= ", " = "
	, " != ", " > ", " >= "
	, " IN ", " @> ", " <@ "
	, " && ", " -|- "
};

static const char* QualChainMethodStr [] = {
	" AND ", " OR "
};


/*
 * Node Funcs Ops
 */

static Node nsql_qual_copy(struct NodeCopyState *cstate, Node n_source,
						   uint64_t flags);
static bool nsql_qual_check(Node node);
static void nsql_qual_destroy(Node node);
static bool nsql_qual_to_sql_cmd(String cmd, void *node,
								 NodeSqlCmdContext ctx);
static bool nsql_quals_to_sql_cmd(String cmd, NodeQualS nqs,
								  NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_qual_funcs = {
	.copy = nsql_qual_copy,
	.check = nsql_qual_check,
	.destroy = nsql_qual_destroy,
	.to_sql_cmd = nsql_qual_to_sql_cmd
};


NodeQual
vh_nsql_qual_create(QualChainMethod cm, QualOperator oper)
{
	NodeQual nq;

	nq = vh_nsql_create(Qual, &nsql_qual_funcs, sizeof(struct NodeQualData));
	nq->cm = cm;
	nq->oper = oper;
	
	return nq;
}


static Node
nsql_qual_copy(struct NodeCopyState *cstate, Node n_source,
			   uint64_t flags)
{
	return 0;
}

static bool
nsql_qual_check(Node node)
{
	return true;
}

static void
nsql_qual_destroy(Node node)
{
	NodeQual nqual = (NodeQual)node;
	int32_t i, sz;

	if (vh_nsql_quals_istvs(&nqual->lhs))
	{
		vh_tvs_reset(&nqual->lhs.tvs);
	}
	else if (vh_nsql_quals_istvslist(&nqual->lhs))
	{
		sz = vh_nsql_qual_sz(&nqual->lhs);
		
		for (i = 0; i < sz; i++)
		{
			vh_tvs_reset(&nqual->lhs.tvslist[i]);
		}
	}
	
	if (vh_nsql_quals_istvs(&nqual->rhs))
	{
		vh_tvs_reset(&nqual->rhs.tvs);
	}
	else if (vh_nsql_quals_istvslist(&nqual->rhs))
	{
		sz = vh_nsql_qual_sz(&nqual->rhs);
		
		for (i = 0; i < sz; i++)
		{
			vh_tvs_reset(&nqual->rhs.tvslist[i]);
		}
	}
}

static bool 
nsql_qual_to_sql_cmd(String cmd, void *node,
					 NodeSqlCmdContext ctx)
{
	NodeQual nq = node;

	if (ctx->last_processed_tag == Qual)
		vh_str.Append(cmd, QualChainMethodStr[nq->cm]);

	vh_str.Append(cmd, "(");
	nsql_quals_to_sql_cmd(cmd, &nq->lhs, ctx);

	vh_str.Append(cmd, QualOperatorStr[nq->oper]);
	
	nsql_quals_to_sql_cmd(cmd, &nq->rhs, ctx);
	vh_str.Append(cmd, ")");

	return true;
}

static bool 
nsql_quals_to_sql_cmd(String cmd, NodeQualS nqs,
					  NodeSqlCmdContext ctx)
{
	switch (vh_nsql_qual_fmt(nqs))
	{
	case VH_NQUAL_FMT_TVSLIST:
		vh_str.Append(cmd, "ANY (");
		vh_nsql_cmd_param_placeholder(cmd, ctx, vh_nsql_quals_tvslist(nqs));
		vh_str.Append(cmd, ") ");
		break;

	case VH_NQUAL_FMT_TF:
		vh_str.AppendStr(cmd, nqs->tf->tdv->td->tname);
		vh_str.Append(cmd, ".");
		vh_str.AppendStr(cmd, nqs->tf->fname);
		break;

	case VH_NQUAL_FMT_NF:
		vh_str.AppendStr(cmd, nqs->nf->tf->fname);
		break;

	case VH_NQUAL_FMT_FUNC:
		vh_str.Append(cmd, nqs->func);

	case VH_NQUAL_FMT_TVS:

		vh_nsql_cmd_param_placeholder(cmd, ctx, vh_nsql_quals_tvs(nqs));
		
		break;
	}

	return true;
}

