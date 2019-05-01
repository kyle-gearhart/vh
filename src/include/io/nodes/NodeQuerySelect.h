/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeQuerySelect_H
#define vh_datacatalog_sql_nodes_NodeQuerySelect_H

#include "io/nodes/NodeQuery.h"

typedef struct NodeQuerySelectData
{
	struct NodeQueryData query;
	Node fields;
	Node from;
	Node joins;
	Node quals;
	Node orderBy;
	int32_t limit;
	int32_t offset;
	uint16_t nfields;
	uint16_t result_table_selections;
} *NodeQuerySelect;

NodeQuerySelect vh_sqlq_sel_create(void);
NodeFrom vh_sqlq_sel_from_add(NodeQuerySelect nqsel, TableDef td,
							  String alias);
NodeFrom vh_sqlq_sel_from_add_transient(NodeQuerySelect nqsel, String schema,
										String table_name, String alias);
NodeJoin vh_sqlq_sel_join_add(NodeQuerySelect nqsel, TableDef td,
							  String alias);
NodeJoin vh_sqlq_sel_join_add_transient(NodeQuerySelect nqsel, String schema,
										String table_name, String alias);

NodeField vh_sqlq_sel_field_add(NodeQuerySelect nqsel, NodeFrom nfrom,
   								TableField tfield, String alias);
NodeField vh_sqlq_sel_join_addfields(NodeQuerySelect nqsel, NodeJoin njoin, 
									 uint32_t *nfields);
NodeField vh_sqlq_sel_from_addfields(NodeQuerySelect nqsel, NodeFrom nfrom, 
									 uint32_t *nfields);

bool vh_sqlq_sel_qual_add(NodeQuerySelect nqsel, Node *nest,
						  NodeQual nqual);

NodeOrderBy vh_sqlq_sel_orderby_add(NodeQuerySelect nqsel);

int32_t vh_sqlq_sel_limit_set(NodeQuerySelect nq, int32_t limit);
void vh_sqlq_sel_limit_clear(NodeQuerySelect nq);

int32_t vh_sqlq_sel_offset_set(NodeQuerySelect nq, int32_t offset);
void vh_sqlq_sel_offset_clear(NodeQuerySelect nq);


#define vh_sqlq_sel_query_td(td)												\
	( td ? ( {																	\
		NodeQuerySelect nq_sel = vh_sqlq_sel_create();							\
		NodeFrom nf = vh_sqlq_sel_from_add(nq_sel, td, 0);						\
		vh_sqlq_sel_from_addfields(nq_sel, nf, 0);								\
		nq_sel;																	\
		} ) : 0																	\
	)

#define vh_sqlq_sel_equery_td(td)												\
	vh_exec_node((Node) vh_sqlq_sel_query_td(td));								\

#endif

