/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/nodes/NodeCreateTable.h"
#include "io/utils/SList.h"

static bool nsql_ctbl_to_sql_cmd(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_ctbl_funcs = {
	.to_sql_cmd = nsql_ctbl_to_sql_cmd
};

/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

NodeCreateTable
vh_sqlq_ctbl_create(void)
{
	NodeCreateTable nct;

	nct = vh_sqlq_create(DDLCreateTable, &nsql_ctbl_funcs, sizeof(NodeCreateTableData));

	return nct;
}

void
vh_sqlq_ctbl_tdv(NodeCreateTable nct, TableDefVer tdv)
{
	nct->td = tdv->td;
	nct->tdv = tdv;
}

void
vh_sqlq_ctbl_td(NodeCreateTable nct, TableDef td)
{
	nct->td = td;
	nct->tdv = vh_td_tdv_lead(td);
}


/*
 * ============================================================================
 * Node Implementation
 * ============================================================================
 */

static bool
nsql_ctbl_to_sql_cmd(String cmd, void *node, NodeSqlCmdContext ctx)
{
	NodeCreateTable ctbl = node;
	TableDef td = ctbl->td;
	TableDefVer tdv = ctbl->tdv;
	TableField *tfs, tf;
	int32_t i, tfs_sz;
	bool first = true;

	vh_str.Append(cmd, "CREATE TABLE ");
	
	if (td->sname)
	{
		vh_str.AppendStr(cmd, td->sname);
		vh_str.Append(cmd, ".");
	}

	if (td->tname)
	{
		vh_str.AppendStr(cmd, td->tname);
	}

	vh_str.Append(cmd, " ( ");
	
	tfs_sz = vh_SListIterator(tdv->heap.fields, tfs);

	for (i = 0; i < tfs_sz; i++)
	{
		tf = tfs[i];

		if (first)
			first = false;
		else
			vh_str.Append(cmd, ", ");

		if (tf->fname)
		{
			vh_str.AppendStr(cmd, tf->fname);
			vh_str.Append(cmd, " ");
		}

		vh_nsql_cmd_datatype(cmd, ctx, tf->heap.types, tf->heap.type_depth);
	}

	vh_str.Append(cmd, " )");

	return true;
}

