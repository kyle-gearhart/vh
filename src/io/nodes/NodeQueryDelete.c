/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQueryDelete.h"

/*
 * NodeOpsFuncs
 */
static Node nsql_qdel_copy(struct NodeCopyState *cstate, Node n_source, 
		 				   uint64_t flags);
static bool nsql_qdel_check(Node node);
static void nsql_qdel_destroy(Node node);
static bool nsql_qdel_to_sql_cmd(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_qdel_funcs = {
	.copy = nsql_qdel_copy,
	.check = nsql_qdel_check,
	.destroy = nsql_qdel_destroy,
	.to_sql_cmd = nsql_qdel_to_sql_cmd
};

NodeQueryDelete
vh_sqlq_del_create(void)
{
	NodeQueryDelete qdel;

	qdel = vh_sqlq_create(Delete, &nsql_qdel_funcs,
						  sizeof(struct NodeQueryDeleteData));
	qdel->from = 0;
	qdel->quals = 0;

	return qdel;	
}

/*
 * If a NodeFrom has already been created and the TableDef does not match the
 * TableDef |td|, we'll warn and then replace with an entirely new NodeFrom.
 * Otherwise, we'll just spin up a new NodeFrom and return it.
 */

NodeFrom
vh_sqlq_del_table(NodeQueryDelete nqdel, TableDef td)
{
	if (td && nqdel->from && td == nqdel->from->tdv->td)
	{
		/*
		 * Replace
		 */

	}
	else
	{
		nqdel->from  = vh_nsql_from_create();
		nqdel->from->tdv = vh_td_tdv_lead(td);

		vh_nsql_child_rappend((Node)nqdel, (Node)nqdel->from);
	}

	return nqdel->from;
}

/*
 * Creates a NodeQual structure and add its to the DELETE query.  If the
 * dereferenced qual_list is non-null, we'll attempt to add the qual as a child
 * of the qual_list.  Otherwise, if qual_list is provided, we'll set it to
 * the chain we add the qual to.
 */
NodeQual
vh_sqlq_del_qual(NodeQueryDelete nqdel, Node *qual_list)
{
	return 0;
}

static Node 
nsql_qdel_copy(struct NodeCopyState *cstate, Node n_source, 
			   uint64_t flags)
{
	return 0;
}

static bool 
nsql_qdel_check(Node node)
{
	return true;
}

static void 
nsql_qdel_destroy(Node node)
{
}


static bool 
nsql_qdel_to_sql_cmd(String cmd, void *node, NodeSqlCmdContext ctx)
{
	NodeQueryDelete nqd = node;
	bool has_table = false;

	if (nqd->from)
	{
		vh_str.Append(cmd, "DELETE ");
		vh_nsql_cmd_impl(&nqd->from->node, cmd, ctx, false);
		has_table = true;
	}

	if (nqd->quals && has_table)
	{
		vh_str.Append(cmd, "WHERE ");
		vh_nsql_cmd_impl(nqd->quals, cmd, ctx, true);
	}

	return has_table;
}

