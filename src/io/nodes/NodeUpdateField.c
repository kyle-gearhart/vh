/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/nodes/NodeUpdateField.h"

static bool nsql_updfield_to_sql(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_updfield_funcs = {
	.to_sql_cmd = nsql_updfield_to_sql
};


NodeUpdateField
vh_nsql_updfield_create(TableField tf, HeapTuplePtr htp)
{
	NodeUpdateField nuf;

	nuf = vh_nsql_create(Field, &nsql_updfield_funcs, sizeof(NodeUpdateFieldData));
	nuf->tf = tf;

	vh_tvs_init(&nuf->tvs);
	vh_tvs_store_htp_hf(&nuf->tvs, htp, &tf->heap);

	return nuf;
}

static bool 
nsql_updfield_to_sql(String cmd, void *node, NodeSqlCmdContext ctx)
{
	NodeUpdateField nuf = node;

	vh_str.AppendStr(cmd, nuf->tf->fname);
	vh_str.Append(cmd, " = ");
	vh_nsql_cmd_param_placeholder(cmd, ctx, &nuf->tvs);

	return true;
}

