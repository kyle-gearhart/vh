/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeJoin.h"
#include "io/nodes/NodeOrderBy.h"
#include "io/nodes/NodeQual.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/utils/SList.h"


/*
 * Node Funcs
 */

static Node nsql_qsel_copy(struct NodeCopyState *cstate, Node n_source,
						   uint64_t flags);
static bool nsql_qsel_check(Node node);
static void nsql_qsel_destroy(Node node);
static bool nsql_qsel_to_sql_cmd(String cmd, void *node, NodeSqlCmdContext ctx);

static const struct NodeOpsFuncs nsql_qsel_funcs = {
	.copy = nsql_qsel_copy,
	.check = nsql_qsel_check,
	.destroy = nsql_qsel_destroy,
	.to_sql_cmd = nsql_qsel_to_sql_cmd
};


static NodeField AddFieldImpl(NodeQuerySelect nqsel, 
				 			  NodeFrom nfrom, 
			 				  TableField field, 
			 				  String alias);
static void AllocSelectQueryList(NodeQuerySelect query,
								 Node *node, 
								 NodeTag tag);




static Node 
nsql_qsel_copy(struct NodeCopyState *cstate, Node n_source,
			   uint64_t flags)
{
	NodeQuerySelect qsel = vh_sqlq_sel_create();
	NodeQuerySelect qsrc = (NodeQuerySelect)n_source;

	qsel->query.hasTemporaryTables = qsrc->query.hasTemporaryTables;
	qsel->query.clusterPref = qsrc->query.clusterPref;

	return (Node)qsel;
}

static bool 
nsql_qsel_check(Node node)
{
	return true;
}

/*
 * Don't do anything, as all of the nodes will be on the tree anyways.
 */
static void 
nsql_qsel_destroy(Node node)
{
}

/* 
 * Allocates a list node head to the desired node.  Sets the node's
 * parent to the query object.  Useful for allocating query->from,
 * query->fields on the fly rather than upfront at create time
 */
static void
AllocSelectQueryList(NodeQuerySelect query, 
					 Node *node, 
					 NodeTag tag)
{
	Node target;

	target = vh_nsql_create(tag, 0, sizeof(struct NodeData));
	target->tag = tag;
	vh_nsql_child_rappend((Node)query, target);
	(*node) = target;
}

NodeFrom
vh_sqlq_sel_from_add(NodeQuerySelect nqsel, TableDef td, String alias)
{
	NodeFrom from;

	if (!nqsel->from)
		AllocSelectQueryList(nqsel, 
							 &nqsel->from, 
							 FromList);

	from = vh_nsql_from_create();

	from->tdv = vh_td_tdv_lead(td); 
	from->result = false;
	from->result_index = 0;
	from->alias = alias;

	vh_nsql_child_rappend((Node)nqsel->from, (Node)from);

	return from;
}

NodeJoin
vh_sqlq_sel_join_add(NodeQuerySelect nqsel, TableDef td, 
					 String alias)
{
	NodeJoin join;

	if (!nqsel->joins)
	{
		nqsel->joins = vh_nsql_create(JoinList, 0, sizeof(struct NodeData));
	//	vh_nsql_child_rappend((Node)nqsel, (Node)nqsel->joins);
	}

	join = vh_nsql_join_create();

	join->join_table.tdv = vh_td_tdv_lead(td);
	join->join_table.result = false;
	join->join_table.result_index = 0;
	join->join_table.alias = alias;

	vh_nsql_child_rappend((Node)nqsel->joins, (Node)join);

	return join;
}

NodeField 
vh_sqlq_sel_field_add(NodeQuerySelect nqsel, NodeFrom nfrom, 
					  TableField tfield, String alias)
{
	NodeField nfield;

	nfield = AddFieldImpl(nqsel, nfrom, tfield, alias);

	return nfield;
}

static NodeField
AddFieldImpl(NodeQuerySelect nqsel, 
			 NodeFrom nfrom, 
			 TableField tfield, 
			 String alias)
{
	NodeField nfield;

	if (!nqsel->fields)
		AllocSelectQueryList(nqsel, &nqsel->fields, FieldList);

	nfield = vh_nsql_field_create();
	vh_nsql_child_rappend(nqsel->fields, (Node)nfield);

	nfield->tf = tfield;
	nfield->nfrom = nfrom;
	nfield->alias = alias;

	return nfield;
}

NodeField
vh_sqlq_sel_from_addfields(NodeQuerySelect nqsel, NodeFrom nfrom,
						   uint32_t *nfields)
{
	TableField *head, tfield;
	uint32_t fieldsz, i;
	TableDef td;
	TableDefVer tdv;
	NodeField nfield, nfield_first = 0;

	td = nfrom->tdv->td;

	if (td)
	{
		tdv = nfrom->tdv;

		if (!nqsel->fields)
			AllocSelectQueryList(nqsel, &nqsel->fields, FieldList);

		fieldsz = vh_SListIterator(tdv->heap.fields, head);

		for (i = 0; i < fieldsz; i++)
		{
			tfield = head[i];
			
			nfield = AddFieldImpl(nqsel, nfrom, tfield, 0);
			nfield->isjoin = false;

			if (i == 0)
				nfield_first = nfield;
		}

		if (!nfrom->result)
		{
			nfrom->result = true;
			nfrom->result_index = nqsel->result_table_selections++;
		}

		if (nfields)
			*nfields = fieldsz;
	}

	return nfield_first;
}


NodeQuerySelect
vh_sqlq_sel_create(void)
{
	NodeQuerySelect select;

	select = vh_sqlq_create(Select, &nsql_qsel_funcs,
							sizeof(struct NodeQuerySelectData));

	select->fields = 0;
	select->from = 0;
	select->joins = 0;
	select->limit = -1;
	select->offset = -1;
	select->orderBy = 0;
	select->quals = 0;
	select->result_table_selections = 0;

	return select;
}

bool
vh_sqlq_sel_qual_add(NodeQuerySelect nqsel, Node *nest,
					 NodeQual nqual)
{
	bool contained = false;

	if (nest && (*nest))
	{
		/*
		 * We should really check to see if the qual is in the NodeQuery
		 * structure before adding it.
		 */
		contained = vh_nsql_tree_contains((Node)nqsel, *nest);

		if (contained)
		{
			vh_nsql_child_rappend(*nest, (Node)nqual);
			
			return true;
		}
		else
		{
			elog(ERROR1,
				 emsg("NodeQual nest does not belong to the current query.  "
					  "Could not insert NodeQual into the query.  Ensure the "
					  "pointer pointed to by nest is null or a valid QualList."));

			return false;
		}
	}
	else
	{
		if (!nqsel->quals)
			AllocSelectQueryList(nqsel, &nqsel->quals, QualList);

		vh_nsql_child_rappend(nqsel->quals, (Node)nqual);

		if (nest)
			*nest = nqsel->quals;

		return true;
	}

	return false;
}

static bool 
nsql_qsel_to_sql_cmd(String cmd, void *node,
					 NodeSqlCmdContext ctx)
{
	NodeQuerySelect sel = node;
	char buffer[20];
	size_t b_len = 20, b_cur = 0;

	vh_str.Append(cmd, "SELECT ");

	if (!sel->fields)
		vh_str.Append(cmd, "* ");
	else
		vh_nsql_cmd_impl(sel->fields, cmd, ctx, true);

	if (sel->from)
	{
		vh_str.Append(cmd, " FROM ");
		vh_nsql_cmd_impl(sel->from, cmd, ctx, true);
	}

	if (sel->joins)
	{
		vh_nsql_cmd_impl(sel->joins, cmd, ctx, true);
	}

	if (sel->quals)
	{
		vh_str.Append(cmd, " WHERE ");
		vh_nsql_cmd_impl(sel->quals, cmd, ctx, true);
	}

	if (sel->orderBy)
	{
		vh_str.Append(cmd, " ORDER BY ");
		vh_nsql_cmd_impl(sel->orderBy, cmd, ctx, true);
	}

	if (sel->limit > 0)
	{
		vh_str.Append(cmd, " LIMIT ");
		vh_type_int32.tam.cstr_get(0,				/* Tamstack */
								   &((struct CStrAMOptionsData) { .malloc = false }),
								   &sel->limit,		/* Source */
								   &buffer[0],		/* Target */
								   &b_len,			/* Length */
								   &b_cur,			/* Cursor */
								   0);				/* Formatter */
		vh_str.Append(cmd, &buffer[0]);
   		vh_str.Append(cmd, " ");

		if (sel->offset > 0)
		{
			vh_str.Append(cmd, " OFFSET ");
			vh_type_int32.tam.cstr_get(0,				/* Tamstack */
									   &((struct CStrAMOptionsData) { .malloc = false }),
									   &sel->offset,	/* Source */
									   &buffer[0],		/* Target */
									   &b_len,			/* Length */
									   &b_cur,			/* Cursor */
									   0);				/* Formatter */
			vh_str.Append(cmd, &buffer[0]);
			vh_str.Append(cmd, " ");
		}
	}

	return true;
}

int32_t 
vh_sqlq_sel_limit_set(NodeQuerySelect nq, int32_t limit)
{
	if (limit > 0)
		nq->limit = limit;
	else
	{
		nq->limit = -1;

		elog(WARNING,
			 emsg("When setting a LIMIT for NodeQuerySelect, the limit must be "
				  "greater than zero.  A limit of %d was requested",
				  limit));
	}

	return nq->limit;
}

void 
vh_sqlq_sel_limit_clear(NodeQuerySelect nq)
{
	nq->limit = -1;
}

int32_t 
vh_sqlq_sel_offset_set(NodeQuerySelect nq, int32_t offset)
{
	if (offset >= 0)
		nq->offset = offset;
	else
	{
		nq->offset = -1;

		elog(WARNING,
			 emsg("The OFFSET clause for a NodeQuerySelect must be zero or greater.  "
				  "The requeted OFFSET was %d",
				  offset));
	}

	return nq->offset;
}

void 
vh_sqlq_sel_offset_clear(NodeQuerySelect nq)
{
	nq->offset = -1;
}

