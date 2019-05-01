/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "jobs/directive.h"

static int32_t run_recurse(JobCtx *jctx, DirIns root);

int32_t
jobs_walk_tree(DirIns root, jobs_dirins_walk_tree_cb cb_func,
			   void *cb_data)
{
	int32_t ret = cb_func(root, cb_data);

	if (ret)
		return ret;

	if (root->tree_sibling)
		if ((ret = cb_func(root->tree_sibling, cb_data))
			return ret;

	if (root->tree_child)
		if ((ret = cb_func(root->tree_child, cb_data))
			return ret;
}

int32_t
jobs_run(JobCtx *jctx, DirIns root)
{
	return jobs_walk_tree(root, run_recurse, jctx);
}

static int32_t
run_recurse(DirIns di, void *data)
{
	int32_t ret = 0;
	Directive dir = di->dir;
	JobCtx jctx = data;
	jobs_dir_start_func start_func;
	jobs_dir_run_func run_func;
	jobs_dir_run_ht_func run_ht_func;
	jobs_dir_run_htb_func run_htb_func;
	jobs_dir_stop_func run_stop_func;

	start_func = di->funcs.start ? di->funcs.start : dir->funcs.start;
	run_func = di->funcs.run ? di->funcs.run : dir->funcs.run;
	run_ht_func = di->funcs.run_ht ? di->funcs.run_ht : dir->funcs.run_ht;
	run_htb_func = di->funcs.run_htb ? di->funcs.run_htb : dir->funcs.run_htb;
	run_stop_func = di->funcs.run_end_func ? di->funcs.stop : dir->funcs.stop;

	/*
	 * First let's see if we need to call the global initializer.  We'll need to
	 * check the JobCtx to see if this has been done already.
	 */

	if (dir->funcs.init)
	{
		ret = dir->funcs.init(jctx, dir, 0);

		if (ret)
			return ret;
	}

	if (start_func)
	{
		ret = start_func(jctx, di);

		if (ret)
			return ret;
	}

	if (run_func)
	{
		ret = run_func(jctx, di);

		if (ret)
			return ret;
	}

	if (end_func)
	{
		ret = end_func(jctx, di);

		if (ret)
			return ret;
	}
}

