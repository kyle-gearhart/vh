/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"
#include "io/catalog/types/int.h"
#include "io/utils/art.h"

typedef struct TypeData TypeData;

static int32_t tom_comp_stub(struct TomCompStack *tomstack,
							 const void *lhs, const void *rhs);
static void tom_construct_stub(struct TomConstructStack *tomstack,
							   void *target, HeapBufferNo hbno);
static void tom_destruct_stub(struct TomDestructStack *tomstack,
							  void *target);


/*
 * We use an ART tree internally to store the operator functions.
 */ 

static art_tree *oper_table = 0;

static int32_t oper_fill_key(Type lhs, Type rhs, const char *oper,
							 char *key, size_t key_sz);


/*
 * CStr Formatters
 */

static void* cstr_format_one(Type ty, const char *pattern);
static void* cstr_format_array(Type ty, const char **patterns, int32_t n_patterns);

/*
 * Initializes the type stack by setting the first member to zero.  As more
 * types are pushed onto the stack, a null terminating value will indicate the
 * bottom of the stack.
 */
void
vh_type_stack_init(Type *ts)
{
	if (ts)
		ts[0] = 0;
}

/*
 * Pushes a type onto the type stack.  Assumes the stack is VH_TAMS_MAX_DEPTH
 * in length.  Returns the index of the newly added item or -1 if it could not
 * be added.
 */
int8_t
vh_type_stack_push(Type *ts, Type ty)
{
	int8_t i = 0;
	Type typ, tyi;

	typ = 0;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		tyi = ts[i];

		if (!tyi)
			break;
	}

	if (i < VH_TAMS_MAX_DEPTH - 1)
	{
		if (i > 0)
			typ = ts[i - 1];

		if (typ)
		{
			if (typ->allow_inner)
			{
				ts[i] = ty;
				ts[i + 1] = 0;

				return i;
			}
			else
			{
				elog(WARNING,
					 emsg("The type %s does not accept inner types!",
						  typ->name));
			}
		}
		else
		{
			ts[i] = ty;
			ts[i + 1] = 0;

			return i;
		}
	}
	else
	{
		elog(WARNING,
			 emsg("Maximum type stack depth will be exceeded, the type %s "
				  "cannot be pushed on the type stack!",
				  ty->name));
	}

	return -1;
}

int8_t
vh_type_stack_copy(Type *dest, Type *src)
{
	int32_t i;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		dest[i] = src[i];

		if (!dest[i])
			break;
	}

	return i;
}

int8_t
vh_type_stack_2tags(TypeTag *tags, Type *tys)
{
	int8_t i;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		if (!tys[i])
			break;

		tags[i] = tys[i]->id;
	}

	if (i < VH_TAMS_MAX_DEPTH)
		tags[i] = 0;

	return i;
}

int8_t
vh_type_stack_fromtags(Type *tys, TypeTag *tags)
{
	int8_t i;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		if (!tags[i])
			break;

		tys[i] = vh_type_tag(tags[i]);

		if (!tys[i])
			break;
	}

	if (i < VH_TAMS_MAX_DEPTH)
		tys[i] = 0;

	return i;
}

int8_t
vh_type_stack_key(Type *tys, TypeStackKey *tsk)
{
	int32_t i;
	int8_t counter = 0;
	uint16_t *cursor = (uint16_t*)tsk;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++, cursor++)
	{
		if (tys[i])
		{
			*cursor = tys[i]->id;
			counter++;
		}
		else
		{
			*cursor = 0;
		}

		cursor++;
	}

	return counter;
}

bool
vh_type_stack_match(Type *lhs, Type *rhs)
{
	int32_t i;
	bool match = true;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		if (lhs[i] != rhs[i])
		{
			match = false;
			break;
		}

		if (!lhs[i])
			break;
	}

	return match;
}

/*
 * vh_type_stack_data_maxalign
 *
 * Gets the maximum alignment value of all nested member types.  This could
 * really be a whole lot smarter about how this works, but we'll see.
 */
size_t
vh_type_stack_data_maxalign(Type *stack)
{
	size_t max_align;
	Type type;
	uint32_t i;

	max_align = 0;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		type = stack[i];

		if (type)
		{
			if (type->alignment > max_align)
				max_align = type->alignment;

			if (!type->include_inner_for_size)
				break;
		}
		else
		{
			break;
		}
	}

	return max_align;
}

/*
 * vh_type_stack_data_width
 *
 * Calculates the width of a data member.
 */
size_t
vh_type_stack_data_width(Type *stack)
{
	size_t sz;
	Type type;
	uint32_t i;
	uint8_t size_mult;

	sz = 0;
	size_mult = 1;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		type = stack[i];

		if (type)
		{
			sz += type->size * size_mult;

			if (type->varlen &&
				!type->varlen_has_header)
				sz += sizeof(struct vhvarlenm);

			if (!type->include_inner_for_size)
				break;

			size_mult = type->inner_for_size_multiplier;
			size_mult = size_mult ? size_mult : 1;
		}
		else
		{
			break;
		}
	}

	return sz;
}

bool
vh_type_stack_has_varlen(Type *stack)
{
	Type type;
	uint32_t i;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		type = stack[i];

		if (!type)
			break;

		if (type->varlen)
			return true;
	}

	return false;
}

void
vh_type_stack_properties(Type *tys, int8_t scan_limit,
						 int8_t *depth, 
						 size_t *width, size_t *max_align,
						 bool *constructor)
{
	size_t sz, max_a;
	Type type;
	int8_t i, max;
	uint8_t size_mult;
	bool construct = false;

	max = (scan_limit == -1 ? VH_TAMS_MAX_DEPTH : scan_limit);

	sz = 0;
	max_a = 0;
	size_mult = 1;

	for (i = 0; i < max; i++)
	{
		type = tys[i];

		if (type)
		{
			sz += type->size * size_mult;

			if (type->alignment > max_a)
				max_a = type->alignment;

			if (type->construct_forhtd)
				construct = true;

			if (type->varlen &&
				!type->varlen_has_header)
				sz += sizeof(struct vhvarlenm);

			if (!type->include_inner_for_size)
				continue;

			size_mult = type->inner_for_size_multiplier;
			size_mult = size_mult ? size_mult : 1;
		}
		else
		{
			break;
		}
	}

	if (depth)
		*depth = i;

	if (width)
		*width = sz;

	if (max_align)
		*max_align = max_a;

	if (constructor)
		*constructor = construct;
}

/*
 * vh_type_stack_fillaccum
 *
 * Fills a TypeStack with the accumulator.
 */
int8_t
vh_type_stack_fillaccum(Type *tys_accum, Type *tys)
{
	int8_t i;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		if (!tys[i])
		{
			tys_accum[i] = 0;
			break;
		}

		if (tys[i]->accumulator)
			tys_accum[i] = tys[i]->accumulator;
		else
			tys_accum[i] = tys[i];
	}

	return i;
}

bool
vh_tams_fill_get_funcs(Type *stack, TamGetUnion *funcs, TypeAM tam)
{
	bool filled = false;
	Type ty;
	uint32_t i;

	i = 0;
	ty = stack[0];

	while (ty)
	{
		if (!ty)
		{
			funcs[i].bin = 0;
			i++;

			ty = stack[i];

			continue;
		}

		filled = true;

		switch (tam)
		{
		case TAM_Binary:
			funcs[i].bin = ty->tam.bin_get;

			break;

		case TAM_CStr:
			funcs[i].cstr = ty->tam.cstr_get;
			break;

		case TAM_MemSet:
			funcs[i].memset = ty->tam.memset_get;

			break;
		}

		i++;
		ty = stack[i];
	}

	return filled;
}

bool
vh_tams_fill_set_funcs(Type *stack, TamSetUnion *funcs, TypeAM tam)
{
	bool filled = false;
	Type ty;
	uint32_t i;

	i = 0;
	ty = stack[0];

	while (ty)
	{
		if (!ty)
		{
			funcs[i].bin = 0;

			i++;
			ty = stack[i];

			continue;
		}

		filled = true;

		switch (tam)
		{
		case TAM_Binary:
			funcs[i].bin = ty->tam.bin_set;

			break;

		case TAM_CStr:
			funcs[i].cstr = ty->tam.cstr_set;
			break;

		case TAM_MemSet:
			funcs[i].memset = ty->tam.memset_set;

			break;
		}

		i++;
		ty = stack[i];
	}

	return filled;
}

size_t
vh_tams_length_from_get(Type *typestack, TamGetUnion *funcs, 
						TypeAM tam, const void *source)
{
	struct BinaryAMOptionData bopts = { .malloc = false };
	size_t i_len, o_len;

	i_len = 0;
	o_len = 0;

	switch (tam)
	{
	case TAM_Binary:
		vh_tam_fireu_bin_get(typestack, 
							 funcs, 
							 &bopts, 
							 source, 
							 0, 
							 &i_len, 
							 0);
		o_len = i_len;

		break;
	default:
		elog(ERROR1,
			 emsg("Get operation requested does not support fetching the "
				  "length only!"));
		break;
	}

	return o_len;
}

bool
vh_toms_fill_comp_funcs(Type *stack, vh_tom_comp *comp)
{
	bool filled = false;
	Type ty_i = stack[0];
	uint32_t i = 0;

	while (ty_i)
	{
		filled = true;
		comp[i] = ty_i->tom.comp;

		i++;
		ty_i = stack[i];
	}

	comp[i] = 0;

	return filled;
}

bool
vh_toms_fill_construct_funcs(Type *stack, vh_tom_construct *construct)
{
	bool filled = false;
	Type ty_i = stack[0];
	uint32_t i = 0;

	while (ty_i)
	{
		filled = true;
		construct[i] = ty_i->tom.construct;

		i++;
		ty_i = stack[i];
	}

	construct[i] = 0;

	return filled;
}

bool
vh_toms_fill_destruct_funcs(Type *stack, vh_tom_destruct *destruct)
{
	bool filled = false;
	Type ty_i = stack[0];
	uint32_t i = 0;

	while (ty_i)
	{
		filled = true;
		destruct[i] = ty_i->tom.destruct;

		i++;
		ty_i = stack[i];
	}

	destruct[i] = 0;

	return filled;
}

struct TypeData*
vh_type_create(const char* name)
{
	MemoryContext t;
	TypeData* ty;

	t = vh_mctx_top();
	
	ty = (TypeData*)
		vhmalloc_ctx(t, sizeof(TypeData));
	
	vh_type_init(ty);

	ty->name = name;

	return ty;
}

void
vh_type_destroy(struct TypeData* ty)
{

}

void
vh_type_init(struct TypeData* ty)
{
	memset(ty, sizeof(TypeData), 0);
}

static int32_t 
tom_comp_stub(struct TomCompStack *tomstack,
			  const void *lhs, const void *rhs)
{
	return vh_tom_firen_comp(tomstack, lhs, rhs);
}

static void 
tom_construct_stub(struct TomConstructStack *tomstack,
				   void *target, HeapBufferNo hbno)
{
	return vh_tom_firen_construct(tomstack, target, hbno);
}

static void 
tom_destruct_stub(struct TomDestructStack *tomstack,
				  void *target)
{
	return vh_tom_firen_destruct(tomstack, target);
}

void
vh_type_oper_register(Type ty_lhs, const char *oper, Type ty_rhs,
					  vh_tom_oper func, bool assign_lhs)
{
	char key[50];
	int32_t key_len;
	void *is_new;

	if (!oper_table)
	{
		oper_table = vhmalloc(sizeof(art_tree));
		art_tree_init(oper_table);
	}

	if (!(key_len = oper_fill_key(ty_lhs, ty_rhs, oper, &key[0], 50)))
	{
	}

	is_new = art_insert(oper_table, (unsigned char*)(&key[0]), key_len, func);

	if (is_new)
		elog(WARNING, emsg("Existing operator [%s] replaced for type %s and type %s!",
						   oper, ty_lhs->name, ty_rhs->name));

}

vh_tom_oper
vh_type_oper(Type ty_lhs, const char *oper, Type ty_rhs, int32_t *flags)
{
	vh_tom_oper of = 0;
	char key[50];
	int32_t key_len;

	if (!oper_table)
	{
		elog(FATAL, emsg("Startup error, operator lookup table has not been "
						 "initialized!"));
		return 0;
	}

	if (!(key_len = oper_fill_key(ty_lhs, ty_rhs, oper, &key[0], 50)))
	{
		elog(ERROR1, emsg("Operator was too long! "
						  "Unable to perform operator lookup for [%s]",
						  oper));
		return 0;
	}

	of = art_search(oper_table, (unsigned char*)(&key[0]), key_len);

	if (!of)
	{
		/*
		elog(ERROR1, emsg("Operator [%s] for %s and %s could not be found!",
						  oper, ty_lhs->name, ty_rhs->name));
		*/
		return 0;
	}

	return of;	
}

/*
 * oper_fill_key
 *
 * Fill |key| with the pointer values.
 */

static int32_t 
oper_fill_key(Type lhs, Type rhs, const char *oper,
			  char *key, size_t key_sz)
{
	const size_t min_key_sz = (sizeof(uintptr_t) * 2) + sizeof(char);
	size_t oper_len = 0;
	char *cursor = key;

	if (key_sz < min_key_sz)
		return 0;

	oper_len = strlen(oper);

	if (key_sz - min_key_sz < oper_len - 1)
		return 0;

	memcpy(cursor, lhs, sizeof(uintptr_t));
	cursor += sizeof(uintptr_t);
	strcpy(cursor, oper);
	cursor += oper_len;

	if (rhs)
	{
		memcpy(cursor, rhs, sizeof(uintptr_t));
		return ((sizeof(uintptr_t) * 2) + oper_len);
	}

	return sizeof(uintptr_t) + oper_len;
}

void* 
vh_tam_cstr_format(Type ty, const char *pattern, 
   				   const char **patterns, int32_t n_patterns)
{
	if (!ty || !ty->tam.cstr_fmt)
		return 0;

	if (n_patterns)
	{
		return ty->tam.cstr_fmt(ty, patterns, n_patterns);
	}
	
	return ty->tam.cstr_fmt(ty, &pattern, 1); 
}

void
vh_tam_cstr_formats_destroy(Type *tys, void **formatters)
{
	int8_t i;

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		if (!tys[i])
			break;

		if (formatters[i])
			vh_tam_cstr_format_destroy(tys[i], formatters[i]);
	}
}

void
vh_tam_cstr_format_destroy(Type ty, void *formatter)
{
	vh_tam_cstr_fmt_destroy dty = 0;

	if (ty && (dty = ty->tam.cstr_fmt_destroy))
		dty(ty, formatter);
}


