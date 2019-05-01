/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"


typedef struct TableFieldBackEndOpts
{
	BackEnd be;
	struct TypeAMFuncs *funcs;
	String format;	
} TableFieldBackEndOpts;

static void init_beopt(TableFieldBackEndOpts *opts);
static TableFieldBackEndOpts* make_beopt(TableField tf, BackEnd be);
static TableFieldBackEndOpts* get_beopt(TableField tf, BackEnd be);
static void finalize_beopt(TableFieldBackEndOpts *opts);

TableField
vh_tf_create(void)
{
	TableField tf = vhmalloc(sizeof(TableFieldData));

	memset(((HeapField)tf) + 1, 0, sizeof(TableFieldData) - sizeof(HeapFieldData));	
	vh_hf_init(&tf->heap);

	tf->heap.tag |= vh_hf_tag_tablefield;

	return tf;
}

void
vh_tf_finalize(TableField tf)
{
	uint32_t i;

	if (tf->beopts)
	{
		assert(tf->n_be_opts);

		for (i = 0; i < tf->n_be_opts; i++)
			finalize_beopt(&tf->beopts[i]);

		vhfree(tf->beopts);
	}

	if (tf->fname)
	{
		vh_str.Destroy(tf->fname);
		tf->fname = 0;
	}
}

void
vh_tf_beopt_format_clear(TableField tf, BackEnd be)
{
	TableFieldBackEndOpts *opts = get_beopt(tf, be);

	if (opts && opts->format)
	{
		vh_str.Destroy(opts->format);
		opts->format = 0;
	}
}

void
vh_tf_beopt_format_set(TableField tf, BackEnd be, const char *format)
{
	TableFieldBackEndOpts *opts = get_beopt(tf, be);

	if (!opts)
		opts = make_beopt(tf, be);

	if (opts->format)
		vh_str.Assign(opts->format, format);
	else
		opts->format = vh_str.Convert(format);
}

bool
vh_tf_beopt_format(TableField tf, BackEnd be, const char **dest_format)
{
	TableFieldBackEndOpts *opts = get_beopt(tf, be);

	if (!opts)
	{
		*dest_format = 0;
	}
	else if (!opts->format)
	{
		*dest_format = 0;
	}
	else
	{
		*dest_format = vh_str_buffer(opts->format);
		return true;
	}

	return false;
}

struct TypeAMFuncs*
vh_tf_be_has_tam_funcs(TableField tf, BackEnd be)
{
	TableFieldBackEndOpts *beopt;

	beopt = get_beopt(tf, be);

	if (beopt)
	{
		return beopt->funcs;
	}

	return 0;
}

struct TypeAMFuncs*
vh_tf_be_tam_funcs(TableField tf, BackEnd be)
{
	TableFieldBackEndOpts *beopt;

	beopt = get_beopt(tf, be);

	if (beopt)
	{
		if (beopt->funcs)
			return beopt->funcs;

		beopt->funcs = vhmalloc(sizeof(struct TypeAMFuncs));
		memset(beopt->funcs, 0, sizeof(struct TypeAMFuncs));

		return beopt->funcs;
	}

	beopt = make_beopt(tf, be);
	beopt->funcs = vhmalloc(sizeof(struct TypeAMFuncs));
	memset(beopt->funcs, 0, sizeof(struct TypeAMFuncs));

	return beopt->funcs;
}

static void 
init_beopt(TableFieldBackEndOpts *opts)
{
	memset(opts, 0, sizeof(TableFieldBackEndOpts));
}

static TableFieldBackEndOpts* 
make_beopt(TableField tf, BackEnd be)
{
	TableFieldBackEndOpts *opts;

	if (tf->beopts)
	{
		tf->beopts = vhrealloc(tf->beopts, sizeof(TableFieldBackEndOpts) *
							   ++tf->n_be_opts);
	}
	else
	{
		assert(tf->beopts == 0);
		tf->beopts = vhmalloc(sizeof(TableFieldBackEndOpts) *
							 ++tf->n_be_opts);
	}

	opts = &tf->beopts[tf->n_be_opts - 1];
	
	init_beopt(opts);

	opts->be = be;
	
	return opts;
}

static TableFieldBackEndOpts* 
get_beopt(TableField tf, BackEnd be)
{
	TableFieldBackEndOpts *opts;
	uint32_t i;

	for (i = 0; i < tf->n_be_opts; i++)
	{
		opts = &tf->beopts[i];

		if (opts->be == be)
			return opts;
	}

	return 0;
}

static void
finalize_beopt(TableFieldBackEndOpts *opt)
{
	if (opt->format)
		vh_str.Destroy(opt->format);
}

