/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeJoin.h"

/*
 * Node Funcs
 */

static Node nsql_field_copy(struct NodeCopyState *cstate, Node n_source,
							uint64_t flags);
static bool nsql_field_check(Node nfield);
static void nsql_field_destroy(Node nfield);
static bool nsql_field_to_sql(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_field_funcs = {
	.copy = nsql_field_copy,
	.check = nsql_field_check,
	.destroy = nsql_field_destroy,
	.to_sql_cmd = nsql_field_to_sql
};

static const char* TableAliasFromIndex[] = { "t0", "t1", "t2", "t3", "t4", "t5",
	"t6", "t7", "t8", "t9", "t10" };


static Node
nsql_field_copy(struct NodeCopyState *cstate, Node n_source,
				uint64_t flags)
{
	NodeField nfield = vh_nsql_field_create();
	NodeField nsrc = (NodeField)n_source;

	cstate->map(cstate, (Node)nfield, (Node)nsrc);

	return 0;
}

static bool
nsql_field_check(Node n_source)
{
	return true;
}

static void
nsql_field_destroy(Node node)
{
	NodeField nfield = (NodeField)node;

	if (nfield->alias)
		vh_str.Destroy(nfield->alias);
}

String 
vh_nsql_field_queryname(NodeField field)
{
	String nm;

	nm = vh_str.Create();
	vh_nsql_field_append_queryname(field, nm, true);

	return nm;
}

void
vh_nsql_field_append_queryname(NodeField field, String nm, bool fq)
{
	NodeFrom nfrom;

	if (field->nfrom->node.tag == From)
		nfrom = field->nfrom;
	else if (field->nfrom->node.tag == Join)
		nfrom = &field->njoin->join_table;

	if (field->wildcard)
	{
		vh_nsql_from_append_queryname(nfrom, nm);
		vh_str.Append(nm, ".");
		vh_str.Append(nm, "*");

		return;
	}

	if (!field->tf->db)
	{
		if (fq)
		{
			if (field->nfrom->alias)
			{
				vh_str.AppendStr(nm, nfrom->alias);
				vh_str.Append(nm, ".");
			}
			else if (field->nfrom->tdv)
			{
				vh_str.AppendStr(nm, nfrom->tdv->td->tname);
				vh_str.Append(nm, ".");
			}
			else
			{
				vh_str.Append(nm, TableAliasFromIndex[field->nfrom->result_index]);
				vh_str.Append(nm, ".");
			}
		}

		vh_str.AppendStr(nm, field->tf->fname);

		if (field->alias)
		{
			vh_str.Append(nm, " AS ");
			vh_str.AppendStr(nm, field->alias);
		}
	}
	else
	{
		vh_str.AppendStr(nm, field->tf->fname);
	}
}

NodeField
vh_nsql_field_create(void)
{
	NodeField nf;

	nf = vh_nsql_create(Field, &nsql_field_funcs,
						sizeof(struct NodeFieldData));

	return nf;
}

static bool 
nsql_field_to_sql(String cmd, void *node, NodeSqlCmdContext ctx)
{
	NodeField nf = node;

	if (ctx->previous_tag == Field)
	   vh_str.Append(cmd, ", ");

	vh_nsql_field_append_queryname(nf, cmd, ctx->fq);

	return true;
}


