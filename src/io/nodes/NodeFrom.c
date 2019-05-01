/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/utils/SList.h"

/*
 * NodeOpsFuncs
 */
static Node nsql_from_copy(struct NodeCopyState *cstate, Node n_source,
						   uint64_t flags);
static bool nsql_from_check(Node node);
static void nsql_from_destroy(Node node);
static bool nsql_from_to_sql(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_from_funcs = {
	.copy = nsql_from_copy,
	.check = nsql_from_check,
	.destroy = nsql_from_destroy,
	.to_sql_cmd = nsql_from_to_sql
};


NodeFrom
vh_nsql_from_create(void)
{
	NodeFrom nfrom;

	nfrom = vh_nsql_create(From, &nsql_from_funcs,
						   sizeof(struct NodeFromData));
	nfrom->htps = 0;
	nfrom->alias = 0;
	nfrom->temporary = false;
	nfrom->result = false;
	nfrom->result_index = 0;
	nfrom->lock_level = 0;
	nfrom->lock_mode = 0;

	nfrom->transient = false;
	nfrom->transient_schema = 0;
	nfrom->transient_table = 0;

	return nfrom;
}

void
vh_nsql_from_init(NodeFrom nf)
{
	nf->node.funcs = &nsql_from_funcs;
}

void
vh_nsql_from_append_queryname(NodeFrom nf, String target)
{
	if (nf->transient)
	{
		if (nf->transient_schema)
		{
			vh_str.AppendStr(target, nf->transient_schema);
			vh_str.Append(target, ".");
		}

		vh_str.AppendStr(target, nf->transient_table);

		return;
	}

	if (nf->tdv)
	{
		if (nf->tdv->td->sname)
		{
			vh_str.AppendStr(target, nf->tdv->td->sname);
			vh_str.Append(target, ".");
		}

		vh_str.AppendStr(target, nf->tdv->td->tname);
	}
}

static Node 
nsql_from_copy(struct NodeCopyState *cstate, Node n_source,
			   uint64_t flags)
{
	NodeFrom nfrom, source = (NodeFrom)n_source;

	nfrom = vh_nsql_from_create();
	nfrom->lock_mode = source->lock_mode;
	nfrom->lock_level = source->lock_level;
	nfrom->result = source->result;
	nfrom->result_index = source->result_index;
	nfrom->temporary = source->temporary;
	nfrom->tdv = source->tdv;

	if (flags & VH_NSQL_COPYFLAG_DEEP)
	{
		if (source->htps)
			nfrom->htps = vh_SListCopy(source->htps);
		else
			nfrom->htps = 0;

		if (source->alias)
			nfrom->alias = vh_str.ConstructStr(source->alias);
		else
			nfrom->alias = 0;
	}
	else
	{
		nfrom->htps = 0;
		nfrom->alias = 0;
	}

	return (Node)nfrom;
}

static bool 
nsql_from_check(Node node)
{
	return true;
}

static void 
nsql_from_destroy(Node node)
{
}

static bool 
nsql_from_to_sql(String cmd, void *node, NodeSqlCmdContext ctx)
{
	NodeFrom nf = node;
	TableDef td = nf->tdv->td;

	if (nf->transient)
	{
		if (nf->transient_schema)
		{
			vh_str.AppendStr(cmd, nf->transient_schema);
			vh_str.Append(cmd, ".");
		}

		if (nf->transient_table)
		{
			vh_str.AppendStr(cmd, nf->transient_table);
		}
		else
		{
			elog(ERROR2,
				 emsg("Transient NodeFrom specified but transient_table was null"));
		}

		if (nf->alias)
		{
			vh_str.Append(cmd, " AS ");
			vh_str.AppendStr(cmd, nf->alias);
		}

		return true;
	}

	if (!td)
		elog(ERROR2,
			 emsg("TableDef pointer is null on NodeFrom and the NodeFrom was not "
				  "specified as transient!  Unable to proceed with building the "
				  "SQL command from the current NodeFrom node."));

	if (!td->tname)
		return false;

	if (td->sname)
	{
		vh_str.AppendStr(cmd, td->sname);
		vh_str.Append(cmd, ".");
	}

	vh_str.AppendStr(cmd, td->tname);
	
	if (nf->alias)
	{
		vh_str.Append(cmd, " AS ");
		vh_str.AppendStr(cmd, nf->alias);
	}

	return true;
}

