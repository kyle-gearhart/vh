/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/catalog/tam.h"
#include "io/utils/SList.h"


#define tam_field_mask		0xF

typedef struct CStrFormatPattern
{
	union
	{
		const char *pattern;
		const char **patterns;
	};

	int32_t n_patterns;
} CStrFormatPattern;

static void set_union_type(TypeAM tam, Type ty, 
						   TamUnion *u, CStrFormatPattern *fmt, 
						   bool get);
static void set_union_tf(TypeAM tam, TableField tf, 
						 TamUnion *u, CStrFormatPattern *fmt, 
						 bool get);
static void set_union_be_type(TypeAM tam, Type ty, BackEnd be, 
							  TamUnion *u, CStrFormatPattern *fmt, 
							  bool get);
static void set_union_be_tf(TypeAM tam, TableField tf, BackEnd be, 
							TamUnion *u, CStrFormatPattern *fmt, 
							bool get);

static void set_union_typeamfuncs(TypeAM tam, const struct TypeAMFuncs *src, 
								  TamUnion *u, CStrFormatPattern *fmt,
								  bool get);

bool
vh_tam_fill_stack(int32_t fields, TypeAM tam,
				  Type *tys, int8_t tys_depth,
				  BackEnd be, TableField tf,
				  TamUnion *funcs, void **fmts,
				  bool get)
{
	int32_t field_i = fields, field = 0;
	int8_t i;
	bool found_tam_inner = false, found_tam_outter = true;
	CStrFormatPattern fmt_pattern = { }, last_fmt_pattern = { }, *fmt_touse;

	if (tam == TAM_CStr)
		assert(fmts);

	for (i = 0; i < tys_depth; i++)
	{
		found_tam_inner = false;
		funcs[i].get.bin = 0;
		field_i = fields;
		field = fields & tam_field_mask;

		while (field)
		{
			fmt_pattern.pattern = 0;
			fmt_pattern.n_patterns = 0;

			switch (field)
			{
				case vh_tam_loc_type:
					
					if (tys[i])
						set_union_type(tam, tys[i], &funcs[i], &fmt_pattern, get);
					else
						elog(ERROR1,
							 emsg("Invalid Type passed to vh_tam_fill_stack in slot %d",
								  i));

				   break;


				case vh_tam_loc_tf:

				   if (vh_hf_is_tablefield(tf))
				   {
					   if (tf)
						   set_union_tf(tam, tf, &funcs[i], &fmt_pattern, get);
					   else
						   elog(ERROR1,
								emsg("Invalid TableField passed to vh_tam_fill_stack in slot %d",
									 i));
				   }

					break;


				case vh_tam_loc_betype:

					if (be)
					{
						if (tys[i])
							set_union_be_type(tam, tys[i], be, &funcs[i], &fmt_pattern, get);
						else
							elog(ERROR1,
								 emsg("Invalid Type passed to vh_tam_fill_stack in slot %d", i));
					}

					break;


				case vh_tam_loc_tfbe:

					if (tf && vh_hf_is_tablefield(tf) && be)
					{
						set_union_be_tf(tam, tf, be, &funcs[i], &fmt_pattern, get);
					}

					break;
			}

			/*
			 * If we've been set then break out.  Otherwse advance to the next
			 * field.
			 *
			 * We should do a quick check for a CStr formatter, is one exists
			 * then generate it.
			 */
			if (funcs[i].get.bin)
			{
				if (tam == TAM_CStr && fmts)
				{
					fmt_touse = 0;

					if (fmt_pattern.pattern)
					{
						fmt_touse = &fmt_pattern;
					}
					else if (last_fmt_pattern.pattern)
					{
						fmt_touse = &last_fmt_pattern;
					}

					if (fmt_touse)
						fmts[i] = vh_tam_cstr_format(tys[i],
													 fmt_touse->n_patterns ? 0 : fmt_touse->pattern,
													 fmt_touse->n_patterns ? fmt_touse->patterns : 0,
													 fmt_touse->n_patterns);
				}

				found_tam_inner = true;

				break;
			}

			if (fmt_pattern.pattern)
				last_fmt_pattern = fmt_pattern;

			field_i = field_i >> 4;
			field = 0 | (field_i & tam_field_mask);
		}

		found_tam_outter = found_tam_outter && found_tam_inner;
	}

	return found_tam_outter;
}


TamUnion** 
vh_tam_htd_create(int32_t fields, TypeAM tam,
				  HeapTupleDef htd, BackEnd be,
				  void ****cstr_format,
				  bool get)
{
	HeapField *hf_head, hf;
	uint32_t hf_sz, i;
	TamUnion **tam_head, *tam_field;
	void ***cstr_head = 0, **cstr_field;
	size_t sz;

	sz = vh_htd_tam_calcsize(htd);

	hf_sz = vh_SListIterator(htd->fields, hf_head);
	tam_head = vhmalloc(sz);
	tam_field = (TamUnion*)(tam_head + hf_sz);

	if (cstr_format)
	{
		cstr_head = vhmalloc(sz);
		cstr_field = (void**)(cstr_head + hf_sz);
		memset(cstr_head, 0, sz);
	}

	memset(tam_head, 0, sz);

	for (i = 0; i < hf_sz; i++)
	{
		tam_head[i] = tam_field;
		hf = hf_head[i];

		if (cstr_head)
			cstr_head[i] = cstr_field;

		if (get)
		{
			vh_tam_be_field_fill_get(tam, be, hf, tam_field, cstr_field);
		}
		else
		{
			vh_tam_be_field_fill_set(tam, be, hf, tam_field, cstr_field);
		}

		tam_field += hf->type_depth + 1;

		if (cstr_head)
			cstr_field += hf->type_depth + 1;
	}

	return tam_head;
}

static void 
set_union_type(TypeAM tam, Type ty, 
			   TamUnion *u, CStrFormatPattern *fmt, 
			   bool get)
{
	set_union_typeamfuncs(tam, &ty->tam, u, fmt, get);
}

static void 
set_union_tf(TypeAM tam, TableField tf, 
			 TamUnion *u, CStrFormatPattern *fmt, 
			 bool get)
{
	struct TypeAMFuncs *tffuncs = 0;

	tffuncs = vh_tf_has_tam_funcs(tf);

	if (tffuncs)
		set_union_typeamfuncs(tam, tf->tam, u, fmt, get);
}

static void 
set_union_be_type(TypeAM tam, Type ty, BackEnd be, 
				  TamUnion *u, CStrFormatPattern *fmt, 
				  bool get)
{
	struct TypeAMFuncs *befuncs;

	befuncs = vh_be_type_getam(be, ty);

	if (befuncs)
		set_union_typeamfuncs(tam, befuncs, u, fmt, get);
}


/*
 * set_union_be_tf
 *
 * For TableFieldBackEndOpts, we're allowed to specify more than one
 * C string format pattern.  We'll first allow set_union_typeamfuncs
 * to find a formatter there.  After returning we'll check the back
 * end options for the TableField to see if more than one pattern has
 * been defined.  If it has, then we'll set CStrFormatPattern accordingly.
 */	
static void 
set_union_be_tf(TypeAM tam, TableField tf, BackEnd be, 
				TamUnion *u, CStrFormatPattern *fmt, 
				bool get)
{
	struct TypeAMFuncs *tfbefuncs = 0;

	tfbefuncs = vh_tf_be_has_tam_funcs(tf, be);

	if (tfbefuncs)
		set_union_typeamfuncs(tam, tfbefuncs, u, fmt, get);	
}

static void 
set_union_typeamfuncs(TypeAM tam, const struct TypeAMFuncs *src, 
					  TamUnion *u, CStrFormatPattern *fmt,
					  bool get)
{
	switch (tam)
	{
		case TAM_Binary:
			
			if (get)
				u->get.bin = src->bin_get;
			else
				u->set.bin = src->bin_set;

			break;

		case TAM_CStr:

			/*
			 * We want the pattern, even if there isn't a specific
			 * function defined.  The standard Type functions will
			 * often accept a pattern.
			 */
			if (get)
			{
				u->get.cstr = src->cstr_get;
			}
			else
			{
				u->set.cstr = src->cstr_set;
			}

			fmt->pattern = src->cstr_format;
			fmt->n_patterns = 0;

			break;

		case TAM_MemSet:

			if (get)
				u->get.memset = src->memset_get;
			else
				u->set.memset = src->memset_set;

			break;
	}
}

