/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/utils/SList.h"


/*
 * NodeOpsFuncs
 */
static Node nsql_qins_copy(struct NodeCopyState *cstate, Node n_source,
						   uint64_t flags);
static bool nsql_qins_check(Node node);
static void nsql_qins_destroy(Node node);
static bool nsql_qins_to_sql_cmd(String cmd, void *node,
								 NodeSqlCmdContext ctx);

struct QinsAddParamData
{
	String cmd;
	NodeSqlCmdContext ctx;
	HeapTuplePtr htp;
	HeapTuple ht;
	bool first_col;
};

static void nsql_qins_cmd_add_param(Node n, void *);

static const struct NodeOpsFuncs nsql_qins_funcs = {
	.copy = nsql_qins_copy,
	.check = nsql_qins_check,
	.destroy = nsql_qins_destroy,
	.to_sql_cmd = nsql_qins_to_sql_cmd
};

static Node 
nsql_qins_copy(struct NodeCopyState *cstate, Node n_source,
			   uint64_t flags)
{
	return 0;
}

static bool 
nsql_qins_check(Node node)
{
	return true;
}


static void
nsql_qins_destroy(Node root)
{
}

NodeFrom
vh_sqlq_ins_table(NodeQueryInsert nqins, TableDef td)
{
	if (!nqins->into)
	{
		nqins->into = vh_nsql_from_create();
		nqins->into->tdv = vh_td_tdv_lead(td);
		
		return nqins->into;
	}

	return 0;
}

NodeQueryInsert
vh_sqlq_ins_create(void)
{
	NodeQueryInsert qins;

	qins = vh_sqlq_create(Insert, &nsql_qins_funcs,
						  sizeof(struct NodeQueryInsertData));

	qins->rfields = 0;
	qins->fields = 0;
	qins->into = 0;

	return qins;
}

NodeField
vh_sqlq_ins_field_add(NodeQueryInsert nqins, NodeFrom nfrom, HeapField hf)
{
	NodeField nf;

	assert(nqins);
	assert(nfrom);
	assert(hf);

	/*
	 * Check to make sure the desired HeapField is included in the the HeapTupleDef
	 * we've already got attached to this query.
	 */

	if (nqins->into != nfrom)
	{
		elog(WARNING,
				emsg("NodeFrom pointer [%p] passed to vh_sqlq_ins_field_add does not "
					 "match the target table attached to the INSERT query at [%p].",
					 nfrom, nqins));
		return 0;
	}

	nf = vh_nsql_field_create();
	nf->tf = (TableField)hf;
	nf->nfrom = nfrom;
	nf->wildcard = false;
	nf->isjoin = false;

	if (!nqins->fields)
	{
		nqins->fields = vh_nsql_create(FieldList, 0, sizeof(struct NodeData));
	}

	vh_nsql_child_rappend(nqins->fields, (Node)nf);

	return nf;
}

NodeField
vh_sqlq_ins_rfield_add(NodeQueryInsert nqins, NodeFrom nfrom, HeapField hf)
{
	NodeField nf;

	assert(nqins);
	assert(nfrom);
	assert(hf);

	/*
	 * Check to make sure the desired HeapField is included in the the HeapTupleDef
	 * we've already got attached to this query.
	 */

	if (nqins->into != nfrom)
	{
		elog(WARNING,
				emsg("NodeFrom pointer [%p] passed to vh_sqlq_ins_field_add does not "
					 "match the target table attached to the INSERT query at [%p].",
					 nfrom, nqins));
		return 0;
	}

	nf = vh_nsql_field_create();
	nf->tf = (TableField)hf;
	nf->nfrom = nfrom;
	nf->wildcard = false;
	nf->isjoin = false;

	if (!nqins->rfields)
	{
		nqins->rfields = vh_nsql_create(FieldList, 0, sizeof(struct NodeData));
	}

	vh_nsql_child_rappend(nqins->rfields, (Node)nf);

	return nf;
}

/*
 * vh_sqlq_ins_htp_list
 *
 * We just want to transfer the list of HTP to the back end.  If there's more than
 * 10 then set the bulk flag.
 */
void
vh_sqlq_ins_htp_list(NodeQueryInsert query,
			   		 SList list)
{
	if (query->into->htps)
	{
		elog(WARNING,
				emsg("Replacing HTP SList for Insert Query [%p].  Could result in "
					 "dangling memory pointers.",
					 query));

	}

	query->into->htps = list;
}

void
vh_sqlq_ins_htp(NodeQueryInsert query, HeapTuplePtr htp)
{
	if (!query->into->htps)
		vh_htp_SListCreate(query->into->htps);

	vh_htp_SListPush(query->into->htps, htp);
}


static bool nsql_qins_to_sql_cmd(String cmd, void *node,
								 NodeSqlCmdContext ctx)
{
	NodeQueryInsert nq = node;
	struct QinsAddParamData qapd = { };
	HeapTuplePtr *htp_head, htp;
	HeapField *hf_head, hf;
	TableField tf;
	int32_t hf_sz, htp_sz, i, j;
	TableDefVer tdv;
	TypeVarSlot tvs;
	bool first_col = true, first_rec = true, fq_old;

	if (!nq->into->htps)
		elog(ERROR2,
			 emsg("Unable to form insert query SQL command: no HeapTuplePtr were "
				  "provided in the node!"));


	vh_str.Append(cmd, "INSERT INTO ");

	if (nq->into->tdv->td->sname)
	{
		vh_str.AppendStr(cmd, nq->into->tdv->td->sname);
		vh_str.Append(cmd, ".");
	}

	vh_str.AppendStr(cmd, nq->into->tdv->td->tname);
	vh_str.Append(cmd, " (");

	if (nq->fields)
	{
		fq_old = ctx->fq;
		ctx->fq = false;
		vh_nsql_cmd_impl(nq->fields, cmd, ctx, true);
		ctx->fq = fq_old;
	}
	else
	{
		/*
		 * Create a new function to generate a field list from the TableDef
		 * object.  Place in NodeField.c
		 */
		tdv = nq->into->tdv;
	
		hf_sz = vh_SListIterator(tdv->heap.fields, hf_head);

		for (i = 0; i < hf_sz; i++)
		{
			hf = hf_head[i];
			tf = (TableField)hf;

			if (first_col)
				first_col = false;
			else
				vh_str.Append(cmd, ", ");

			vh_str.AppendStr(cmd, tf->fname);
		}
	}
		
	vh_str.Append(cmd, ") VALUES ");

	htp_sz = vh_SListIterator(nq->into->htps, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];

		if (first_rec)
			first_rec = false;
		else
			vh_str.Append(cmd, ", ");

		vh_str.Append(cmd, "(");

		/*
		 * Do the parameter expansion for each field on every HeapTuplePtr.
		 */

		if (nq->fields)
		{
			qapd.ctx = ctx;
			qapd.cmd = cmd;
			qapd.first_col = true;
			qapd.htp = htp;
			qapd.ht = 0;

			vh_nsql_visit_tree(nq->fields, nsql_qins_cmd_add_param, &qapd);	
		}
		else
		{
			first_col = true;

			for (j = 0; j < hf_sz; j++)
			{
				hf = hf_head[j];

				if (first_col)
					first_col = false;
				else
					vh_str.Append(cmd, ", ");

				vh_tvs_init(&tvs);
				vh_tvs_store_htp_hf(&tvs, htp, hf);
				vh_nsql_cmd_param_placeholder(cmd, ctx, &tvs); 
			}
		}

		vh_str.Append(cmd, ")");
	}

	return true;
}

static void 
nsql_qins_cmd_add_param(Node n, void *data)
{
	NodeField nf = 0;
	struct QinsAddParamData *qapd = data;
	NodeSqlCmdContext ctx = qapd->ctx;
	String cmd = qapd->cmd;
	TypeVarSlot tvs;

	if (n->tag == Field)
	{
		nf = (NodeField)n;

		if (nf->tf)
		{
			if (qapd->first_col)
				qapd->first_col = false;
			else
				vh_str.Append(cmd, ", ");

			vh_tvs_init(&tvs);
			vh_tvs_store_htp_hf(&tvs, qapd->htp, (HeapField)nf->tf);
			vh_nsql_cmd_param_placeholder(cmd, ctx, &tvs); 
		}
	}
}

