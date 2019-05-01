/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include "io/nodes/Node.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/utils/SList.h"


typedef struct CopyTreeStateData
{
	struct NodeCopyState ncs;
	MemoryContext mctx;
	void *map;
	SList remaps;
} *CopyTreeState;

typedef struct CopyTreeMapEntryData
{
	Node old;
	Node new;
} *CopyTreeMapEntry;

typedef struct CopyTreeLogEntryData
{
	Node *n_created;
	Node n_source;
} *CopyTreeLogEntry;

typedef bool (*copytree_map_func)(struct NodeCopyState*, Node, Node);
typedef bool (*copytree_log_func)(struct NodeCopyState*, Node, Node*);

static Node nsql_copytree_recurse(CopyTreeState cstate, Node parent, 
								  Node root, uint64_t flags);

static bool nsql_copytree_map(CopyTreeState cstate, Node n_source,
							  Node n_copy);
static bool nsql_copytree_log(CopyTreeState cstate, Node n_source,
							  Node *n_created);

static int32_t nsql_copytree_comp(const CopyTreeMapEntry a, 
								  const CopyTreeMapEntry b, void *params);


struct TreeContainsState
{
	Node find;
	bool found;
};

static void nsql_tree_contains_recurse(Node node, void *data);

static void NodeDestroyChildrenImpl(Node node, Node top, void (*destroy)(Node node));
static void NodeDestroyImpl(Node node, void (*destroy)(Node node));

struct NodeSqlCmdRecurse
{
	String cmd;
	NodeSqlCmdContext ctx;
};

static void nsql_cmd_recurse(Node root, void *data);


void*
vh_nsql_create(NodeTag tag, const struct NodeOpsFuncs *funcs,
			   size_t size)
{
	Node node;

	if (size >= sizeof(struct NodeData))
	{
		node = vhmalloc(size);
		memset(node, 0, size);

		node->tag = tag;
		node->funcs = funcs;

		return node;
	}

	return 0;
};

Node
vh_nsql_copy(Node root, uint64_t flags)
{
	struct CopyTreeStateData cstate = { };
	Node copy;

	if (root)
	{
		copy = root->funcs->copy((struct NodeCopyState*)&cstate, root, flags);

		return copy;
	}


	return 0;
}

Node
vh_nsql_copytree(Node root, uint64_t flags)
{
	/*
	struct CopyTreeStateData cts = { };
	struct CopyTreeMapEntryData ctme_probe, *ctme;
	CopyTreeLogEntry *ctle_head, ctle;
	uint32_t ctle_sz, i;
	Node root_copy;

	cts.mctx = vh_MemoryPoolCreate(vh_mctx_current(),
								   8192,
								   "NSQL Copy Tree Working");
	cts.remaps = vh_SListCreate_ctx(cts.mctx);
	cts.map = pavl_create((pavl_comparison_func*) nsql_copytree_comp,
						  0,
						  vh_MemoryContextAllocAVL_ctx(cts.mctx));
	cts.ncs.map = (copytree_map_func)nsql_copytree_map;
	cts.ncs.log = (copytree_log_func)nsql_copytree_log;

	root_copy = nsql_copytree_recurse(&cts, 0, root, flags);

	ctle_sz = vh_SListIterator(cts.remaps, ctle_head);
	
	for (i = 0; i < ctle_sz; i++)
	{
		ctle = ctle_head[i];

		if (ctle->n_source)
		{
			ctme_probe.old = ctle->n_source;

			ctme = pavl_find(cts.map, &ctme_probe);

			if (ctme)
			{
				*ctle->n_created = ctme->new;
			}
			else
			{
				if ((*ctle->n_created)->funcs->check(*ctle->n_created))
				{
				}
				else
				{
					// throw an error, the check failed
				}
			}
		}
	}

	vh_mctx_destroy(cts.mctx);

	return root_copy;
	*/

	return 0;
}

static Node
nsql_copytree_recurse(CopyTreeState cstate, Node parent,
					  Node root, uint64_t flags)
{
	Node root_copy, root_child, child_copy;

	root_copy = root->funcs->copy(&cstate->ncs, root, flags);
	cstate->ncs.map(&cstate->ncs, root, root_copy);

	if (parent)
		vh_nsql_child_rappend(parent, root_copy);
	

	root_child = root->firstChild;

	while (root_child)
	{
		child_copy = nsql_copytree_recurse(cstate, root_copy,
										   root_child, flags);

		if (child_copy)
		{
		}

		root_child = root_child->nextSibling;
	}

	return root_copy;
}


static bool 
nsql_copytree_map(CopyTreeState cstate, Node n_source,
				  Node n_copy)
{
	CopyTreeMapEntry ctme;

	if (n_source && n_copy)
	{
		ctme = vhmalloc_ctx(cstate->mctx,
							sizeof(struct CopyTreeMapEntryData));
		ctme->old = n_source;
		ctme->new = n_copy;
		//pavl_insert(cstate->map, ctme);

		return true;
	}

	return false;
}

static bool 
nsql_copytree_log(CopyTreeState cstate, Node n_source,
				  Node *n_created)
{
	CopyTreeLogEntry ctle;

	if (n_source && n_created && *n_created)
	{
		ctle = vhmalloc_ctx(cstate->mctx,
							sizeof(struct CopyTreeLogEntryData));
		ctle->n_source = n_source;
		ctle->n_created = n_created;
		vh_SListPush(cstate->remaps, ctle);

		return true;
	}

	return false;
}


static int32_t 
nsql_copytree_comp(const CopyTreeMapEntry a, 
				   const CopyTreeMapEntry b, void *params)
{
	return (a->old < b->old ? -1 : (a->old > b->old));
}


/*
 * Iterates the entire tree recursively and calls the callback function for
 * every node in the tree.
 */
void
vh_nsql_visit_tree(Node root,
				   void (*callback)(Node, void*),
				   void *data)
{
	Node nchild;

	(*callback)(root, data);

	nchild = root->firstChild;

	while (nchild)
	{
		vh_nsql_visit_tree(nchild, callback, data);
		nchild = nchild->nextSibling;
	}
}

void 
vh_nsql_child_rappend(Node parent, Node child)
{
	child->parent = parent;

	if (parent->lastChild)
	{
		parent->lastChild->nextSibling = child;
		parent->lastChild = child;
		return;
	}

	parent->firstChild = child;
	parent->lastChild = child;
}

void 
vh_nsql_child_lappend(Node parent, Node child)
{
	child->nextSibling = parent->firstChild;
	parent->firstChild = child;
}

int32_t
vh_nsql_child_count(Node node)
{
	int c = 0;
	Node sibling;

	sibling = node->firstChild;

	while (sibling)
	{
		c++;
		sibling = sibling->nextSibling;
	}

	return c;
}

bool
vh_nsql_tree_contains(Node root, Node node)
{
	struct TreeContainsState tcs;

	tcs.find = node;
	tcs.found = false;

	vh_nsql_visit_tree(root, nsql_tree_contains_recurse, &tcs);

	return tcs.found;
}

static void
nsql_tree_contains_recurse(Node node, void *data)
{
	struct TreeContainsState *tcs = data;

	if (tcs->found)
		return;

	if (node == tcs->find)
		tcs->found = true;
}

vh_nsql_cmd_cb
vh_nsql_cmd_ft_lkp(const NodeSqlCmdFuncTable *table, NodeTag tag)
{
	const int32_t max_table_len = NT_Last;
	int32_t i = 1;

	if (!table)
		elog(ERROR2,
			 emsg("vh_nsql_cmd_ft_lkp Null |table| passed for command function lookup!"));

	while (table->tag != NT_Invalid)
	{
		if (i > max_table_len)
			return 0;

		if (table->tag == tag)
			return table->func;

		table++;
		i++;
	}

	return 0;
}

int32_t
vh_nsql_cmd(Node root,
			String *output_cmd,
			const NodeSqlCmdFuncTable *override_table,
			vh_nsql_cmd_param_cb param_ph,
			int32_t param_offset,
			BackEnd be,
			void *caller_data,
			TypeVarSlot **param_values,
			bool recurse)
{
	String cmd;
	vh_nsql_cmd_cb cb = 0;
	bool cb_ret = false;
	struct NodeSqlCmdContextData ctx = { };
	struct NodeSqlCmdRecurse recurse_data = { };
	
	if (*output_cmd)
	{
		cmd = *output_cmd;
	}
	else
	{
		cmd = vh_str.Create();
		*output_cmd = cmd;
	}

	ctx.param_placeholder = param_ph;
	ctx.be = be;
	ctx.caller_data = caller_data;
	ctx.override_table = override_table;
	ctx.previous_tag = -1;
	ctx.param_count = param_offset;

	if (recurse)
	{
		recurse_data.cmd = cmd;
		recurse_data.ctx = &ctx;

		vh_nsql_visit_tree(root, nsql_cmd_recurse, &recurse_data);

		return 0;
	}

	if (override_table)
	{
		cb = vh_nsql_cmd_ft_lkp(override_table, root->tag);
		ctx.default_cmd = root->funcs->to_sql_cmd;

		if (!cb)
			cb = root->funcs->to_sql_cmd;
	}
	else
	{
		cb = root->funcs->to_sql_cmd;
		ctx.default_cmd = 0;
	}

	if (!cb)
		elog(ERROR2,
			 emsg("Unable to find function for tag %d to form a SQL command!",
				  root->tag));

	cb_ret = cb(cmd, root, &ctx);

	if (!cb_ret)
		return -1;

	vh_str.Append(cmd, ";");

	if (param_values)
		*param_values = ctx.param_values;

	return ctx.param_count - param_offset;
}

int32_t 
vh_nsql_cmd_impl(Node root, String cmd,	
				 NodeSqlCmdContext ctx, bool recurse)
{
	vh_nsql_cmd_cb cb = 0;
	bool cb_ret = false;
	struct NodeSqlCmdRecurse recurse_data = { };

	if (recurse)
	{
		recurse_data.cmd = cmd;
		recurse_data.ctx = ctx;

		vh_nsql_visit_tree(root, nsql_cmd_recurse, &recurse_data);

		return 0;
	}

	if (ctx->override_table)
	{
		cb = vh_nsql_cmd_ft_lkp(ctx->override_table, root->tag);
		ctx->default_cmd = root->funcs->to_sql_cmd;

		if (!cb && root->funcs)
			cb = root->funcs->to_sql_cmd;
	}
	else if (root->funcs)
	{
		cb = root->funcs->to_sql_cmd;
		ctx->default_cmd = 0;
	}

	if (!cb)
		elog(ERROR2,
			 emsg("Unable to find function for tag %d to form a SQL command!",
				  root->tag));

	cb_ret = cb(cmd, root, ctx);

	if (!cb_ret)
		return -1;

	ctx->previous_tag = root->tag;

	return 0;
}

int32_t 
vh_nsql_cmd_impl_def(Node root, String cmd,	
					 NodeSqlCmdContext ctx, bool recurse)
{
	vh_nsql_cmd_cb cb = 0;
	bool cb_ret = false;
	struct NodeSqlCmdRecurse recurse_data = { };

	if (recurse)
	{
		recurse_data.cmd = cmd;
		recurse_data.ctx = ctx;

		vh_nsql_visit_tree(root, nsql_cmd_recurse, &recurse_data);

		return 0;
	}

	if (root->funcs)
	{
		cb = root->funcs->to_sql_cmd;
	}

	if (!cb)
		elog(ERROR2,
			 emsg("Unable to find function for tag %d to form a SQL command!",
				  root->tag));

	cb_ret = cb(cmd, root, ctx);

	if (!cb_ret)
		return -1;

	ctx->previous_tag = root->tag;

	return 0;
}

static void 
nsql_cmd_recurse(Node root, void *data)
{
	struct NodeSqlCmdRecurse *recurse = data;	
	vh_nsql_cmd_cb cb = 0;
	bool cb_ret;

	if (recurse->ctx->override_table)
	{
		cb = vh_nsql_cmd_ft_lkp(recurse->ctx->override_table, root->tag);;

		if (!cb && root->funcs)
			cb = root->funcs->to_sql_cmd;
	}
	else if (root->funcs)
	{
		cb = root->funcs->to_sql_cmd;
	}

	if (!cb)
	{
		recurse->ctx->previous_tag = root->tag;
		return;
	}

	cb_ret = cb(recurse->cmd, root, recurse->ctx);

	if (!cb_ret)
		elog(ERROR2,
		     emsg("Node to SQL command function failed during recursion for tag %d",
			 	  root->tag));

	recurse->ctx->last_processed_tag = root->tag;
	recurse->ctx->previous_tag = root->tag;	
}

/*
 * vh_nsql_cmd_param_placeholder
 *
 * We want to provide users with the ability to customize how parameter placehodlers
 * are generated.  Many of the back ends may use common SQL dialect but they expect
 * parameter placeholder formats to be different.  Thus when standing up a
 * NodeSqlCmdContext we give the option to provide a function pointer to stand
 * up a customized placeholder routine.
 *
 * It's expected that everywhere a parameter placeholder is required, that we will
 * always call this function to do the placeholder.
 *
 * We also go ahead and push the TypeVarSlot containing the placeholder value
 */
void
vh_nsql_cmd_param_placeholder(String cmd, NodeSqlCmdContext ctx, 
							  TypeVarSlot *tvs)
{
	TypeVarSlot *tvs_param;


	if (!ctx->param_capacity)
	{
		ctx->param_values = vhmalloc(sizeof(TypeVarSlot) * 8);
		ctx->param_capacity = 8;
	} 
	else if (ctx->param_capacity == ctx->param_count)
	{
		ctx->param_capacity *= 2;
		ctx->param_values = vhrealloc(ctx->param_values, 
									  sizeof(TypeVarSlot) * ctx->param_capacity);
	}

	tvs_param = &ctx->param_values[ctx->param_count];
	vh_tvs_init(tvs_param);
	vh_tvs_copy(tvs_param, tvs);

	if (ctx->param_placeholder)
		return ctx->param_placeholder(cmd, ctx, tvs_param);
	
	vh_str.Append(cmd, "?");
	++ctx->param_count;
}

/*
 * vh_nsql_cmd_datatype
 *
 * Write the textual representation of the data type out.  We'll check to see
 * if a BackEnd has been provided.  If it has, we'll call the standard function
 * vh_be_type_getbe to get the data type.
 *
 * Otherwise, we'll just make one up based on what we know.
 */
void
vh_nsql_cmd_datatype(String cmd, NodeSqlCmdContext ctx,
					 Type *tys, int8_t tys_depth)
{
	const char *dt;
	int8_t i;
	bool first = true;

	if (ctx->be)
	{
		dt = vh_be_type_getbe(ctx->be, tys);

		if (dt)
		{
			vh_str.Append(cmd, dt);
			return;
		}
	}
	
	for (i = 0; i < tys_depth; i++)
	{
		if (first)
			first = false;
		else
			vh_str.Append(cmd, "->");

		vh_str.Append(cmd, tys[i]->name);
	}
}

