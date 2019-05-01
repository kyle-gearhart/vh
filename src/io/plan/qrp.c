/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/tam.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeJoin.h"
#include "io/nodes/NodeQueryDelete.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/nodes/NodeQueryUpdate.h"
#include "io/plan/pstmt.h"
#include "io/plan/qrp.h"
#include "io/utils/kvlist.h"
#include "io/utils/SList.h"

#define qrp_table_size(c) 	(sizeof(struct QueryResultTableProjectionData) * c)
#define qrp_field_size(c)	(sizeof(struct QueryResultFieldProjectionData) * c)
#define qrp_backend_size(c)	(sizeof(struct QueryResultBackEndProjectionData) * c)

typedef struct QueryResultProjectionData QueryResultProjection, QRP;

static QRP QRP_Delete(NodeQueryDelete nqdel, BackEnd be, bool funcs_only);
static QRP QRP_Insert(NodeQueryInsert nqins, BackEnd be, bool funcs_only);
static QRP QRP_Select(NodeQuerySelect nqsel, BackEnd be, bool funcs_only);
//static QRP QRP_Update(NodeQueryUpdate nqupd, BackEnd be, bool funcs_only);

static void qrp_fill_table(TableDef td, QrpTableProjection);
static void qrp_fill_field(HeapField, QrpFieldProjection);
static void qrp_fill_backend(HeapField, BackEnd, QrpBackEndProjection);

static void qrp_nfields_recurse(Node parent, void *data);


struct QrpNFieldsContext
{
	BackEnd be;

	QrpTableProjection tables;
	QrpFieldProjection fields;
	QrpBackEndProjection backend;

	int32_t idx;
	int32_t flags;
};

QRP
vh_plan_qrp(NodeQuery nq, BackEnd be, bool funcs_only)
{
	switch (nq->action)
	{
	case Delete:
		return QRP_Delete((NodeQueryDelete)nq, be, funcs_only);

	case Insert:
		return QRP_Insert((NodeQueryInsert)nq, be, funcs_only);

	case Select:
		return QRP_Select((NodeQuerySelect)nq, be, funcs_only);

	default:
		return (QRP) { };
	}

	return (QRP) { };
}

/*
 * vh_plan_qrp_lb
 *
 * Query Result Projection for late binding statements.  We expect
 * PlannedStmt->qrp_ntables to be one and  PlannedStmt->qrp_tables to be a single
 * member array.  The first member of tds is the TableDef we need to iterate to 
 * add the fields and tam binding.
 */

int32_t
vh_plan_qrp_lb(PlannedStmt pstmt)
{
	TableDefVer tdv = 0;
	TableField tf, *tf_head;
	int32_t tf_sz, i, flags = 0;
	BackEnd be;

	if (pstmt->qrp_ntables != 1)
		elog(ERROR2,
			 emsg("Corrupt PlannedStmt presented to late binding QRP.  Expected only "
				  "1 TableDef in the result set, %d were declared!",
				  pstmt->qrp_ntables));

	if (!pstmt->qrp_table)
		elog(ERROR2,
			 emsg("Corrupt PlannedStmt passed to late binding QRP.  Expected an array "
				  "of TableDef but none were available"));

	tdv = pstmt->qrp_table[0].rtdv;
	tf_sz = vh_SListIterator(tdv->heap.fields, tf_head);

	if (!pstmt->latebindingset)
	{
		flags = VH_PLAN_QRP_FIELDS;

		pstmt->qrp_field = vhmalloc(qrp_field_size(tf_sz));
		pstmt->qrp_nfields = tf_sz;
	}


	be = pstmt->be;
	flags |= VH_PLAN_QRP_BACKEND;

	pstmt->qrp_backend = vhmalloc(qrp_backend_size(tf_sz));


	for (i = 0; i < tf_sz; i++)
	{
		tf = tf_head[i];

		if (flags & VH_PLAN_QRP_FIELDS)
		{
			qrp_fill_field(&tf->heap, &pstmt->qrp_field[i]);
			pstmt->qrp_field[i].td_idx = 0;
		}

		if (flags & VH_PLAN_QRP_BACKEND)
			qrp_fill_backend(&tf->heap, be, &pstmt->qrp_backend[i]);
	}

	if (!pstmt->latebindingset)
		pstmt->latebindingset = true;

	return 0;
}

static QRP
QRP_Insert(NodeQueryInsert nqins, BackEnd be, bool flags)
{
	QRP qrp = { };
	struct QrpNFieldsContext qrpc = { };

	if (nqins->rfields)
	{
		if (flags & VH_PLAN_QRP_TABLES)
		{
			qrp.ntables = 1;
			qrp.tables = vhmalloc(qrp_table_size(qrp.ntables));

			qrp_fill_table(nqins->into->tdv->td, &qrp.tables[0]);
		}

		qrp.nfields = vh_nsql_child_count(nqins->rfields);

		if (qrp.nfields && (flags & VH_PLAN_QRP_FIELDS))	
			qrp.fields = vhmalloc(qrp_field_size(qrp.nfields));

		if (qrp.nfields && (flags & VH_PLAN_QRP_BACKEND))
			qrp.backend = vhmalloc(qrp_backend_size(qrp.nfields));

		if ((flags & VH_PLAN_QRP_FIELDS) ||
			(flags & VH_PLAN_QRP_BACKEND))
		{
			qrpc.be = be;
			qrpc.tables = qrp.tables;
			qrpc.fields = qrp.fields;
			qrpc.backend = qrp.backend;
			qrpc.flags = flags;

			vh_nsql_visit_tree((Node)nqins->rfields,
							   qrp_nfields_recurse,
							   &qrpc);

		}
	}
	
	return qrp;
}

/*
 * We need to call vh_htd_type_stack to set the type stack and then
 * vh_be_tam_sethtd to get the TamSetUnion stack.
 */

static QRP
QRP_Select(NodeQuerySelect nqsel, BackEnd be, bool flags)
{
	QRP qrp = { };
	NodeFrom nfrom;
	NodeJoin njoin;
	struct QrpNFieldsContext qrpc = { };

	assert(sizeof(TamSetUnion) == sizeof(uintptr_t));

	qrp.ntables = nqsel->result_table_selections;

	if (flags & VH_PLAN_QRP_TABLES)
	{
		qrp.tables = vhmalloc(qrp_table_size(qrp.ntables));

		if (nqsel->from)
		{
			nfrom = (NodeFrom)nqsel->from->firstChild;

			while (nfrom)
			{
				if (nfrom->result)
					qrp_fill_table(nfrom->tdv->td, &qrp.tables[nfrom->result_index]);

				nfrom = (NodeFrom) nfrom->node.nextSibling;
			}
		}

		if (nqsel->joins)
		{
			njoin = (NodeJoin)nqsel->joins->firstChild;

			while (njoin)
			{
				if (njoin->join_table.result)
					qrp_fill_table(njoin->join_table.tdv->td, &qrp.tables[njoin->join_table.result_index]);

				njoin = (NodeJoin) njoin->node.nextSibling;
			}
		}
	}

	qrp.nfields = vh_nsql_child_count(nqsel->fields);

	if (qrp.nfields && (flags & VH_PLAN_QRP_FIELDS))	
		qrp.fields = vhmalloc(qrp_field_size(qrp.nfields));

	if (qrp.nfields && (flags & VH_PLAN_QRP_BACKEND))
		qrp.backend = vhmalloc(qrp_backend_size(qrp.nfields));

	if ((flags & VH_PLAN_QRP_FIELDS) ||
		(flags & VH_PLAN_QRP_BACKEND))
	{
		qrpc.be = be;
		qrpc.tables = qrp.tables;
		qrpc.fields = qrp.fields;
		qrpc.backend = qrp.backend;
		qrpc.flags = flags;

		vh_nsql_visit_tree((Node)nqsel->fields,
						   qrp_nfields_recurse,
						   &qrpc);

	}
	
	return qrp;
}

static QRP
QRP_Delete(NodeQueryDelete nqdel, BackEnd be,
		   bool flags)
{
	return (QRP) { };
}

static void
qrp_nfields_recurse(Node root, void *data)
{
	struct QrpNFieldsContext *ctx = data;
	NodeField nfield = (NodeField)root;
	HeapField hf;

	if (root->tag == FieldList)
		return;

	assert(root->tag == Field);

	hf = (HeapField)nfield->tf;

	if (ctx->flags & VH_PLAN_QRP_FIELDS)
	{
		qrp_fill_field(hf, &ctx->fields[ctx->idx]);

		if (nfield->nfrom)
		{
			if (nfield->nfrom->node.tag == From )
				ctx->fields[ctx->idx].td_idx = nfield->nfrom->result_index;
			else if (nfield->nfrom->node.tag == Join)
				ctx->fields[ctx->idx].td_idx = nfield->njoin->join_table.result_index;
		}
	}

	if (ctx->flags & VH_PLAN_QRP_BACKEND)
		qrp_fill_backend(hf, ctx->be, &ctx->backend[ctx->idx]);

	ctx->idx++;
}

static void 
qrp_fill_table(TableDef td, QrpTableProjection qrpt)
{
	qrpt->rtdv = vh_td_tdv_lead(td);
}

static void 
qrp_fill_field(HeapField hf, QrpFieldProjection qrpf)
{
	qrpf->hf = hf;
	qrpf->tys = &hf->types[0];
	qrpf->ty_depth = hf->type_depth;
	qrpf->td_idx = -1;
}

static void 
qrp_fill_backend(HeapField hf, BackEnd be, QrpBackEndProjection qrpbe)
{
	qrpbe->tam_func = vhmalloc(sizeof(TamSetUnion) * (hf->type_depth + 1));

	if (be->tam == TAM_CStr)
		qrpbe->tam_formatters = vhmalloc(sizeof(void**) * (hf->type_depth + 1));
	else
		qrpbe->tam_formatters = 0;

	vh_tam_be_field_fill_set(be->tam, be, hf, qrpbe->tam_func, qrpbe->tam_formatters);
}

void
vh_plan_qrp_table_finalize(QrpTableProjection qrpt)
{
}

void
vh_plan_qrp_field_finalize(QrpFieldProjection qrpf)
{
}

void
vh_plan_qrp_be_finalize(QrpFieldProjection qrpf, 
						   QrpBackEndProjection qrpb)
{
	vh_tam_cstr_fmt_destroy f;
	int32_t i;

	for (i = 0; i < qrpf->ty_depth; i++)
	{
		f = qrpf->tys[i]->tam.cstr_fmt_destroy;

		if (f &&
			qrpb->tam_formatters &&
			qrpb->tam_formatters[i])
			f(qrpf->tys[i], qrpb->tam_formatters[i]);

		if (qrpb->tam_formatters)
			vhfree(qrpb->tam_formatters);
	}
}

