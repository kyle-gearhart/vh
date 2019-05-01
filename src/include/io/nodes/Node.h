/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_Node_H
#define vh_datacatalog_sql_nodes_Node_H

#include "vh.h"

typedef struct NodeQueryData NodeQueryData, *NodeQuery;

typedef enum NodeTag
{
	/*
	 * We use some basic markers to help us with any function table
	 * lookups we do per tag.  This way users don't have to maintain
	 * all of the tags in the exact order listed below to make their
	 * function tables work.
	 */
	NT_Invalid = -1,
	
	Query,
	DDLCommand,
	With,
	Field,
	FieldList,
	From,
	FromList,
	Join,
	JoinList,
	Where,
	OrderByList,
	OrderBy,
	Limit,
	Offset,
	Qual,
	QualList,

	/*
	 * NT_Last indicates the last member of the NodeTag enum.
	 */
	NT_Last

} NodeTag;

/*
 * NodeCopyState provides public access to functions a specific node implementation
 * may call during copying.  Nodes may reference each other outside of the predefined
 * tree structure (parent, child, sibling).  The |map| function maps an address in
 * the source tree to its corresponding address in the new tree.  The |log| function
 * takes the address of the new target and the address the source is pointing to.
 * When vh_nsql_copytree exits, it will iterate all log entries and search the map
 * for the corresponding new value.  The n_created is the dereferenced to the mapped
 * value.
 */
struct NodeCopyState
{
	bool (*map)(struct NodeCopyState *cstate, Node n_source, Node n_copy);
	bool (*log)(struct NodeCopyState *cstate, Node n_source, Node *n_created);
};

#define VH_NSQL_COPYFLAG_DEEP		0x01

/*
 * node_to_sql_cmd
 *
 * We never want to call the NodeOpsFuncs->node_to_sql_cmd function directly.
 * This should always occur thru the vh_nsql_cmd function globally defined
 * further below.  This is because we want to provide the caller with the
 * ability to pass a function table to specialize functions for specific
 * backends.  However, we also don't want to directly associate the 
 * specialization to a backend.  So we've typedef'd a function pointer called 
 * vh_nsql_cmd_cb.  Thus the planner or user don't have to know the backend
 * or any specialization required to form a SQL command when the node is
 * created.
 *
 * All specialization in the nodes module for generating text SQL commands
 * should follow vh_nsql_cmd_cb signature.
 *
 * |cmd|	SQL command to be appended to (will never be null)
 * |node|	Node to do the append with (will never be null)
 * |ctx|	Information about vh_nsql_cmd's state (will never be null)
 */

typedef struct NodeSqlCmdContextData *NodeSqlCmdContext;
typedef bool (*vh_nsql_cmd_cb)(String cmd, void *node, NodeSqlCmdContext ctx);
typedef void (*vh_nsql_cmd_param_cb)(String cmd, NodeSqlCmdContext ctx, TypeVarSlot *tvs);

typedef struct NodeSqlCmdFuncTable
{
	const NodeTag tag;
	const vh_nsql_cmd_cb func;
} NodeSqlCmdFuncTable;

struct NodeSqlCmdContextData
{
	const NodeSqlCmdFuncTable *override_table;
	vh_nsql_cmd_param_cb param_placeholder;
	vh_nsql_cmd_cb default_cmd;
	BackEnd be;
	void *caller_data;
	NodeTag previous_tag;
	NodeTag last_processed_tag;
	TypeVarSlot *param_values;
	int32_t param_count;
	int32_t param_capacity;
	bool fq;
};

struct NodeOpsFuncs
{
	Node (*copy)(struct NodeCopyState *cstate, Node n_source, uint64_t flags);
	bool (*check)(Node n_source);
	void (*destroy)(Node node);
	vh_nsql_cmd_cb to_sql_cmd;
};

struct NodeData
{
	NodeTag tag;
	Node parent;
	Node nextSibling;
	Node firstChild;
	Node lastChild;
	const struct NodeOpsFuncs *funcs;	
};

Node vh_nsql_copy(Node root, uint64_t flags);
Node vh_nsql_copytree(Node root, uint64_t flags);
void* vh_nsql_create(NodeTag tag, const struct NodeOpsFuncs *funcs, size_t size);
Node vh_nsql_destroytree(Node root, uint64_t flags);

bool vh_nsql_tree_contains(Node root, Node node);

typedef void (*vh_nsql_visit_tree_func)(Node, void*);
void vh_nsql_visit_tree(Node root, vh_nsql_visit_tree_func visitor, void *data);

int32_t vh_nsql_child_count(Node node);
void vh_nsql_child_rappend(Node parent, Node child);
void vh_nsql_child_lappend(Node parent, Node child);


/*
 * vh_nsql_cmd
 *
 * Starting point for creating a SQL command string from a node.  The caller
 * is expected to provide a few required data elements: |root| and |output_cmd|.
 *
 * The rest are optional, but do affect how the SQL command is constructed.  The
 * |override_table| allows for overriding the default to_sql_cmd behavior for
 * a given tag.  The |param_ph| overrides the default behavior for generating
 * parameter placeholders.  The |caller_data| is available within the
 * |override_table| and |param_ph| call stack.
 *
 * The |recurse| option generally won't be used, as it's expected the |root| tag
 * will be a Query.  The implemented to_sql_cmd function will likely explicitly
 * define how to walk the tree internally.
 */

vh_nsql_cmd_cb vh_nsql_cmd_ft_lkp(const NodeSqlCmdFuncTable *table, NodeTag tag);
int32_t vh_nsql_cmd(Node root, String *output_cmd,
					const NodeSqlCmdFuncTable *override_table,
					vh_nsql_cmd_param_cb param_ph,
					int32_t param_offset,
					BackEnd be,
					void *caller_data,
					TypeVarSlot **param_values,
	   				bool recurse);

/*
 * vh_nsql_cmd_impl
 *
 * We assume a NodeSqlCmdContext has already been established, so we have
 * a slimmed down parameter list compared to vh_nsql_cmd.  This allows for
 * us to do the function table lookup internally (i.e. from a vh_nsql_cmd_cb
 * function).
 *
 * The only place this should be called is from within a vh_nsql_cmd_cb function
 * stack, as it's the only guarantee the NodeSqlCmdContext will be initialized
 * properly.
 */
int32_t vh_nsql_cmd_impl(Node root, String cmd,	
			   			 NodeSqlCmdContext ctx, bool recurse);

/*
 * vh_nsql_cmd_impl_def
 *
 * Same as above except we go straight to the default command attached to
 * the node, rather than favoring the override table.  Allows us to avoid 
 * an infinite loop for the override function to only specialize an extremely
 * unique scenario and falling back to the standard function for all others.
 */
int32_t vh_nsql_cmd_impl_def(Node root, String cmd,
							 NodeSqlCmdContext ctx, bool recurse);

void vh_nsql_cmd_param_placeholder(String cmd, NodeSqlCmdContext ctx, 
								   TypeVarSlot *tvs);
void vh_nsql_cmd_datatype(String cmd, NodeSqlCmdContext ctx,
						  Type *tys, int8_t tys_size);

#endif

