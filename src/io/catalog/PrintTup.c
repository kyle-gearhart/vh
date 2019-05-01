/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/catalog/PrintTup.h"
#include "io/catalog/Type.h"
#include "io/catalog/tam.h"

struct PrintTupFieldData
{
	HeapField hf;
	vh_tam_cstr_get *funcs;
	void **formatters;
	bool print;
};


/*
 * The HeapTupleDef passed to vh_ptup_creates determines how long the fields
 * array will be.
 */
struct PrintTupCtxData
{
	HeapTupleDef htd;
	int32_t n_fields;
	struct PrintTupFieldData fields[1];		/* Variable length */
};



PrintTupCtx
vh_ptup_create(HeapTupleDef htd)
{
	PrintTupCtx ptup;
	int32_t n_fields;
	size_t sz_ctx;

	assert(htd);

	n_fields = htd->nfields;

	sz_ctx = sizeof(struct PrintTupCtxData) +
		sizeof(struct PrintTupFieldData) * (n_fields - 1);
	ptup = vhmalloc(sz_ctx);
	memset(ptup, 0, sz_ctx);

	ptup->htd = htd;
	ptup->n_fields = n_fields;

	return ptup;
}

void
vh_ptup_finalize(PrintTupCtx ptup)
{
	int32_t i = 0, j = 0;
	struct PrintTupFieldData *ptf;
	vh_tam_cstr_fmt_destroy fd;

	for (i = 0; i < ptup->n_fields; i++)
	{
		ptf = &ptup->fields[i];

		if (ptf->funcs)
			vhfree(ptf->funcs);

		if (!ptf->formatters)
			continue;

		for (j = 0; j < ptf->hf->type_depth; j++)
		{
			fd = ptf->hf->types[j]->tam.cstr_fmt_destroy;

			if (fd && ptf->formatters[j])
			{
				fd(ptf->hf->types[j], ptf->formatters[j]);
			}
		}

		vhfree(ptf->formatters);
		ptf->formatters = 0;
	}
}

void
vh_ptup_add_field(PrintTupCtx ptup, HeapField hf, const char *format)
{
	struct PrintTupFieldData *ptf;
	int32_t i = 0;
	vh_tam_cstr_fmt_destroy fd;

	ptf = &ptup->fields[hf->heapord];
	ptf->hf = hf;	
	ptf->print = true;

	if (!ptf->funcs)
		ptf->funcs = vhmalloc(sizeof(vh_tam_cstr_get) * hf->type_depth);

	if (ptf->formatters)
	{
		for (i = 0; i < hf->type_depth; i++)
		{
			fd = hf->types[i]->tam.cstr_fmt_destroy;

			if (fd && ptf->formatters[i])
			{
				fd(hf->types[i], ptf->formatters[i]);
				ptf->formatters[i] = 0;
			}
		}
	}
	else
	{
		ptf->formatters = vhmalloc(sizeof(void**) * hf->type_depth);
	}

	/*
	 * Set all of the getter functions, thru the entire stack.
	 */
	for (i = 0; i < hf->type_depth; i++)
		ptf->funcs[i] = hf->types[i]->tam.cstr_get;

	/*
	 * For now, only set the bottom most formatter string.
	 */
	if (format)
	{
		ptf->formatters[hf->type_depth - 1] = vh_tam_cstr_format(hf->types[hf->type_depth - 1], format, 0, 0);
	}
}

char*
vh_ptup_field(PrintTupCtx ptup, 
			  HeapTuplePtr htp, HeapField hf, 
			  size_t *out_length)
{
	static const struct CStrAMOptionsData opts = { .malloc = true }; 
	struct PrintTupFieldData *ptf;
	char *fval;
	size_t length = 0, cursor = 0;
	HeapTuple ht;

	ptf = &ptup->fields[hf->heapord];
	ht = vh_htp(htp);

	assert(ptf->hf == hf);
	assert(ht);
	assert(ht->htd == ptup->htd);

	if (vh_htf_isnull(ht, hf))
		return 0;

	if (!ptf->funcs[0] && ptf->print)
		elog(WARNING,
			 emsg("No C string get TAM has been defined for the type %s, "
				  "unable to get the value as a C string.",
				  ptf->hf->types[0]->name));

	fval = vh_tam_firea_cstr_get(&ptf->hf->types[0],				/* Type stack */
								 ptf->funcs,						/* Functions */
								 &opts,								/* CharOpts */
								 vh_ht_field(ht, hf),				/* Source */
								 0,									/* Target */
								 &length,							/* Length */
								 &cursor,							/* Cursor */
								 ptf->formatters);

	if (out_length)
		*out_length = length;

	return fval;
	
}

size_t
vh_ptup_field_buffer(PrintTupCtx ptup,
					 HeapTuplePtr htp, HeapField hf,
					 char *buffer, size_t buffer_capacity)
{
	static const struct CStrAMOptionsData opts = { .malloc = false }; 
	struct PrintTupFieldData *ptf;
	size_t length = buffer_capacity, cursor = 0;
	HeapTuple ht;

	ptf = &ptup->fields[hf->heapord];
	ht = vh_htp(htp);

	assert(ptf->hf == hf);
	assert(ht);
	assert(ht->htd == ptup->htd);

	if (vh_htf_isnull(ht, hf))
	{
		if (buffer)
			buffer[0] = '\0';

		return 0;
	}
	
	if (!ptf->funcs[0] && ptf->print)
		elog(WARNING,
			 emsg("No C string get TAM has been defined for the type %s, "
				  "unable to get the value as a C string.",
				  ptf->hf->types[0]->name));

	vh_tam_firea_cstr_get(&ptf->hf->types[0],				/* Type stack */
								 ptf->funcs,						/* Functions */
								 &opts,								/* CharOpts */
								 vh_ht_field(ht, hf),				/* Source */
								 buffer,							/* Target */
								 &length,							/* Length */
								 &cursor,							/* Cursor */
								 ptf->formatters);

	return length;
}

