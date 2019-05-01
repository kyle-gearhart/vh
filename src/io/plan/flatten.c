/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/plan/flatten.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeJoin.h"
#include "io/nodes/NodeQual.h"
#include "io/utils/SList.h"

/*
 * vh_nsql_visit_tree_funcs passed to visit_tree
 */
static void plan_flatten_recurse(Node root, void *data);
static void plan_flatten_qual_recurse(Node root, void *data);


/*
 * Qual Flattening
 */
static void plan_flatten_qual(NodeQual nqual);
static void plan_flatten_qual_htplist(NodeQual nqual, NodeQualS nquals);


/*
 * We want to flatten the query into something each backend can generate a
 * statement for.  Users have the ability to form statements that don't 
 * translate to SQL.  Flatten takes those foreign concepts and rewrites
 * them to be compatible for ANSI SQL.
 */
void
vh_plan_flatten(NodeQuery nquery)
{
	vh_nsql_visit_tree((Node)nquery, plan_flatten_recurse, 0);
}


static void 
plan_flatten_recurse(Node root, void *data)
{
	NodeJoin njoin;
	NodeQual nqual;

	switch (root->tag)
	{
	case Join:
		njoin = (NodeJoin)root;
		vh_nsql_visit_tree(&njoin->quals, plan_flatten_recurse, 0);

		break;

	case Qual:
		nqual = (NodeQual)root;
		plan_flatten_qual(nqual);
		break;

	default:
		return;
	}
}


/*
 * Scan a given nodes::Node recursively for transformable
 * quals.  Assisted by an array of quals, representing the
 * lhs [0] and rhs [1] to shorten the code base and maintenance
 * requirements in case things expand.
 */

static void plan_flatten_qual(NodeQual nqual)
{
	typedef void (*replace_action)(NodeQual parent,
								   NodeQualS nquals);

	NodeQualS sides[2];
	replace_action actions[2];
	uint32_t i;

	sides[0] = &nqual->lhs;
	sides[1] = &nqual->rhs;

	for (i = 0; i < 2; i++)
	{
		switch (sides[i]->format)
		{
		case HeapTupleList:
			actions[i] = plan_flatten_qual_htplist;
			break;

		default:
			actions[i] = 0;
		}

		if (actions[i])
			actions[i](nqual, sides[i]);
	}

}

/*
 * Replaces a qual of ObjectList with a FieldValueList.
 * The goal is to extract the corresponding values from
 * a SList of object.  Typically the underlying engine
 * will then subimt a parameter of an array type.
 *
 * Avoids inserting duplicates with an AVL tree which
 * is allocated in the PrepareWorkMem working context.
 */
static void 
plan_flatten_qual_htplist(NodeQual nqual, NodeQualS nquals)
{
	NodeQualS op_quals;
	SList orig_values, new_values = 0;
	TableDef table_def;
	TableField extract_field, comp_field;
	uintptr_t **obj_head;
	uint32_t objsz, i;
	TableDefVer tdv;


	extract_field = 0;
	orig_values = nquals->list;
	
	if (nquals->ht)
		table_def = 0; 
	else
		table_def = 0;


	if (table_def)
	{
		/*
		 * We need to do some quick sanity checks to make sure the other
		 * side of the qual is a field and it references the table
		 * we just obtained from the object list.
		 */

		if (&nqual->lhs == nquals)
			op_quals = &nqual->rhs;
		else
			op_quals = &nqual->lhs;

		if (op_quals->format == QueryFieldRef)
			comp_field = op_quals->field->tf;
		else if (op_quals->format == TableFieldRef)
			comp_field = op_quals->table_field;
		else
			comp_field = 0;

		if (comp_field)
		{
			if (comp_field->tdv->td == table_def)
			{
				extract_field = comp_field;
			}
			else if (comp_field->related == table_def)
			{

			}
			else
			{
				/*
				 * Since the comparison field on the opposite NodeQualS
				 * is not related to the table contained in the SList,
				 * we should check to see if the comparison field is atleast
				 * the ID field for its table.  When it is, we can scan
				 * all of the fields contained in the SList table to
				 * see if they are related to the table containing the
				 * field on the opposite NodeQualS.
				 */

				if (comp_field->id)
				{
					TableField *field_head, field;
					uint32_t fieldsz;

					tdv = vh_td_tdv_lead(table_def);
					fieldsz = vh_SListIterator(tdv->heap.fields, field_head);

					for (i = 0; i < fieldsz; i++)
					{
						field = field_head[i];

						if (field->related == comp_field->tdv->td)
						{
							extract_field = field;
							break;
						}
					}
				}
			}
		}
		else
		{
			elog(ERROR2,
				 emsg("Query improperly formatted, expected a FieldRef or "
					  "TableFieldRef during qual replacement and "
					  "transformation for FieldValueList"));
		}
		

		if (extract_field)
		{
			objsz = vh_SListIterator(orig_values, obj_head);
			
			if (obj_head)
			{
			}

			if (objsz > 0)
			{
				/*
				new_values = vh_SListCreate_ctxcap(work->ctx_work, 
												   vh_SListSize(orig_values));


				tracker_alloc = vh_MemoryContextAllocAVL_ctx(work->ctx_work);
				tracker = rb_create(STMTK_Compare,
									0,
									tracker_alloc);

				for (uint32_t i = 0; i < objsz; i++)
				{
					obj = obj_head[i];

					if (obj)
					{
						extract_field->GetMemSetValue(obj,
											   &obj_id);

						tracker_res = (int64_t**)rb_probe(tracker,
														  (void*)obj_id);
					*/

						/*
						 * Only insert unique values into the tree, this
						 * could allow us to reduce the transmission of query
						 * parameters if duplicates exist in the list.
						 */

				/*
						if (*tracker_res == (void*)obj_id)
							SListPush(new_values,
									  (void*)obj_id);
					}
				}

				rb_destroy(tracker, 0);
				vhfree(tracker_alloc);
			}
			else
				new_values = 0;
			*/
			}
		}

		if (new_values)
		{
			vh_nsql_qual_rhs_TFieldList(nqual, nqual->oper, 
										extract_field, new_values);
		}
		else
		{
			elog(ERROR2, 
				 emsg("Unable to replace and transform quals as desired for an "
					  "ObjectList.  Please review originating query format"));
		}
	}
}

