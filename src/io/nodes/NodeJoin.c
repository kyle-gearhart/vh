/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include "vh.h"
#include "io/catalog/TableField.h"
#include "io/nodes/NodeJoin.h"
#include "io/nodes/NodeQual.h"

/*
 * Node Funcs Ops
 */

static Node nsql_join_copy(struct NodeCopyState *cstate, Node n_source,
						   uint64_t flags);
static bool nsql_join_check(Node node);
static void nsql_join_destroy(Node node);
static bool nsql_join_to_sql_cmd(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_join_funcs = {
	.copy = nsql_join_copy,
	.check = nsql_join_check,
	.destroy = nsql_join_destroy,
	.to_sql_cmd = nsql_join_to_sql_cmd
};

static Node
nsql_join_copy(struct NodeCopyState *cstate, Node n_source,
			   uint64_t flags)
{
	return 0;
}

static bool
nsql_join_check(Node node)
{
	return true;
}

static void
nsql_join_destroy(Node node)
{
}

NodeJoin
vh_nsql_join_create(void)
{
	NodeJoin njoin;

	njoin = vh_nsql_create(Join, &nsql_join_funcs,
						   sizeof(struct NodeJoinData));
	memset(njoin, 0, sizeof(struct NodeJoinData));

	vh_nsql_from_init(&njoin->join_table);

	return njoin;
}

NodeQual
vh_nsql_join_qual_addtf(NodeJoin nj, TableField tf_outter, TableField tf_inner)
{
	NodeQual nq;

	if (!tf_outter || !tf_inner)
	{
		elog(WARNING,
			 emsg("Could not add join, either the outter or inner TableField was null!"));

		return 0;
	}

	nq = vh_nsql_qual_create(And, Eq);

	vh_nsql_qual_lhs_tf_set(nq, tf_outter);
	vh_nsql_qual_rhs_tf_set(nq, tf_inner);

	vh_nsql_child_rappend(&nj->quals, &nq->node);

	return nq;	
}


static bool 
nsql_join_to_sql_cmd(String cmd, void *node, NodeSqlCmdContext ctx)
{
	NodeJoin nj = node;

	switch (nj->join_type)
	{
		case Inner:
			vh_str.Append(cmd, "INNER JOIN ");
			break;

		case Left:
			vh_str.Append(cmd, "LEFT JOIN ");
			break;
	}

	/*
	 * Add the table name by using the desired table name function.
	 */
	vh_nsql_cmd_impl(&nj->join_table.node, cmd, ctx, false);

	/*
	 * Add the join quals by using the desired qual functions.
	 */
	vh_str.Append(cmd, "ON (");
	vh_nsql_cmd_impl(&nj->quals, cmd, ctx, true);
	vh_str.Append(cmd, ") ");

	return true;
}

