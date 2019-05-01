/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeOrderBy.h"

/*
 * Node Ops Funcs
 */
static Node nsql_orderby_copy(struct NodeCopyState *cstate, Node n_source,
							  uint64_t flags);
static bool nsql_orderby_check(Node node);
static void nsql_orderby_destroy(Node node);
static bool nsql_orderby_to_sql(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_orderby_funcs = {
	.copy = nsql_orderby_copy,
	.check = nsql_orderby_check,
	.destroy = nsql_orderby_destroy,
	.to_sql_cmd = nsql_orderby_to_sql
};

static const char* nsql_orderby_operator_str[] = { "ASC", "DESC" };


NodeOrderBy
vh_nsql_orderby_create(void)
{
	NodeOrderBy order;

	order = vh_nsql_create(OrderBy, &nsql_orderby_funcs,
						   sizeof(struct NodeOrderByData));
	memset(order, 0, sizeof(struct NodeOrderByData));

	return order;
}

static Node
nsql_orderby_copy(struct NodeCopyState *cstate, Node n_source,
   				  uint64_t flags)
{
	return 0;
}

bool
nsql_orderby_check(Node node)
{
	return true;
}

void
nsql_orderby_destroy(Node node)
{
}

static bool 
nsql_orderby_to_sql(String cmd, void *node, NodeSqlCmdContext ctx)
{
	NodeOrderBy no = node;
	bool set = false;
	TableField tf = 0;
	TableDef td = 0;

	if (ctx->last_processed_tag == OrderBy)
		vh_str.Append(cmd, ", ");

	if (no->nfield)
	{
		vh_nsql_cmd_impl(&no->nfield->node, cmd, ctx, false);
		set = true;
	}
	
	if (no->tfield && !set)
	{
		tf = no->tfield;
		td = tf->tdv->td;
		
		if (td->sname)
		{
			vh_str.AppendStr(cmd, td->sname);
			vh_str.Append(cmd, ".");
		}

		if (td->tname)
		{
			vh_str.AppendStr(cmd, td->tname);
			vh_str.Append(cmd, " ");
		}

		set = true;
	}

	if (set)
	{
		vh_str.Append(cmd, nsql_orderby_operator_str[no->oop]);
		vh_str.Append(cmd, " ");
	}

	return set;
}

