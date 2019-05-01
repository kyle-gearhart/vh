/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_jobs_directive_H
#define vh_jobs_directive_H

typedef struct DirectiveData *Directive;
typedef struct DirInsData *DirIns;

typedef struct DirCtxData *DirCtx;

struct DirectiveCtxData
{
};

enum DirectiveError
{
};

/*
 * We came here to call jobs_run, which is going to fire in the following order:
 * 	1)	For each unique directive, we'll call jobs_directive_init_func if
 * 		available.
 * 	2)	For each directive instance, we'll call jobs_directive_start_func
 * 		if available.
 * 			a)	jobs_directive_run_func will be called if no HeapTuple function
 * 				(jobs_directive_run_ht_func or jobs_directive_run_htb_func) is
 * 				specified.  We favor the former if no HeapTuple context is
 * 				available.
 * 	3)	At the close of a directive instance, we'll call jobs_directive_end_func.
 * 	4)	When the last instance of a unique directive is executed, we'll call
 * 		jobs_directive_dispose_func.
 */

typedef int32_t (*jobs_dir_deserialize_xml_func)(JobCtx *jctx, xmlNode *xml_node,
		 										 DirIns *di);
typedef int32_t (*jobs_dir_destroy_func)(Directive dir);

typedef int32_t (*jobs_dir_init_func)(JobCtx jctx, Directive dir, DirectiveCtx dirctx);
typedef int32_t (*jobs_dir_terminate_func)(JobCtx jctx, Directive dir, DirectiveCtx dirctx);

typedef int32_t (*jobs_dir_start_func)(JobCtx jctx, DirIns di);
typedef int32_t (*jobs_dir_stop_func)(JobCtx jctx, DirIns di);

/*
 * If a run_func is present for a job, it'll be run in favor of a run_ht_func
 * or a run_htb_func.  The primary purpose of the jobstream is to enrich data,
 * row by row, rather than just pick it up and move it around.  For those
 * actions, a directive should provide a jobs_dir_run_func.
 *
 * |di| is a DirIns superstructure that we pass around as a void* to make casting
 * cleaner in the implementing functions.
 */
typedef int32_t (*jobs_dir_run_func)(JobCtx jctx,  DirIns di);
typedef int32_t (*jobs_dir_run_ht_func)(JobCtx jctx, DirIns di, HeapTuple ht);
typedef int32_t (*jobs_dir_run_htb_func(JobCtx jctx, DirIns di, HeapTuple *hta, 
										uint32_t hta_sz);

/*
 * DirectiveData
 *
 * Primary a function table.  The real driver of branching within a jobstream are the
 * properties set on DirIns structure.
 */
struct DirectiveData
{
	const char *ns;
	const char *name;

	size_t instance_sz;
	
	struct
	{
		jobs_dir_deserialize_xml_func deserialize_xml;
		jobs_dir_destroy_func destroy;

		jobs_dir_init_func init;
		jobs_dir_start_func start;
		jobs_dir_run_func run;
		jobs_dir_run_ht_func run_ht;
		jobs_dir_run_htb_func run_htb;
		jobs_dir_stop_func stop;
		jobs_dir_terminate_func terminate;
	} funcs;
};

Directive jobs_dir_create(JobCtx jctx, size_t size);


/*
 * DirInsData
 * Directive Instance
 *
 * Options telling the directive what to do.  Typically these are deserialized
 * from an XML node.  This is the first member of a superstructure.  We have the
 * ability to form a tree.
 */
struct DirInsData
{
	const struct DirectiveData* dir;

	/*
	 * Instance specific function table.
	 */
	struct
	{
		jobs_dir_destroy_func destroy;

		jobs_dir_start_func start;
		jobs_dir_run_func run;
		jobs_dir_run_ht_func run_ht;
		jobs_dir_run_htb_func run_htb;
		jobs_dir_end_func stop;
	} funcs;

	/*
	 * Instance tree, tree_parent will be null if the instance is a top level
	 * node in the in the tree.
	 */
	DirIns tree_parent;
	DirIns tree_sibling;
	DirIns tree_child;
	DirIns tree_child_tail;
};

typedef int32_t (*jobs_dirins_walk_tree_cb)(DirIns dir, void *cb_data);
int32_t jobs_walk_tree(DirIns root, jobs_dirins_walk_tree_cb cb_func,
					   void *cb_data);

DirIns jobs_dirins_create(JobCtx, Directive directive, size_t sz);




int32_t jobs_check(JobCtx *jctx, DirIns root);
int32_t jobs_run(JobCtx *jctx, DirIns root);

#endif

