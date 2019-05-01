/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdarg.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/utils/SList.h"


#define TYPEVAR_MAX_ARRAY_WALKS		10
#define TYPEARRAY_HEAD_MAGIC		0x24defc

typedef struct TypeVarArray
{
	int32_t magic;
	size_t size;
	size_t capacity;
	size_t alignment;

	void **toc;
	struct TypeVarArray **array;
	
	int16_t alignment_tag_factor;
	int16_t type_depth;
	bool has_constructor;
	
	size_t tys_size;
	size_t tys_span;

	Type tys[VH_TAMS_MAX_DEPTH];
	TypeTag tags[1];
} *TypeVarArray;

static int32_t fill_tags_rev(TypeTag *src, TypeTag *tgt);
static int32_t fill_tys_from_tags_rev(Type *tys, TypeTag *tags);
static int32_t fill_tys_from_tags_fwd(Type *tys, TypeTag *tags);

static size_t fill_tys_from_variadic(Type *tys, TypeTag *tags, int32_t nargs, ...);

static size_t typevar_align_factor(Type ty, int8_t depth);

static void typevar_construct_impl(Type *tys, void *data);
static void typevar_free_impl(Type *tys, int32_t depth, void *data);
static void typevar_finalize_impl(Type *tys, void *data);

static void typearray_expand_impl(TypeVarArray tva, size_t min_capacity, bool build_toc);


/*
 * This is going to be our heavy lifter...the Operator Flag parser
 * should generate atleast one of these, potentially more if either
 * the LHS or RHS contained an array.  When we have a special situation
 * when working with HTP, because we want to keep locking to a minimum,
 * so we get in and get out quickly by acquiring the lock in the loop
 * iterating our struct below.
 *
 * The parser should also give us some more love...the operator function
 * to fire, subsequent calls to fill more of these structures after the
 * first block has been exhausted.
 *
 * This guy is pretty thick, measuring 36 bytes (40 with padding) on its 
 * own so we don't just want to try to allocate the LHS and RHS cartesian 
 * product.  We'll do it in blocks if we have a cartesian product that 
 * produces more than one.
 *
 */

typedef struct OpEntrySideData *OpEntrySide;
typedef struct OpExecCombinerData OpExecCombinerData, *OpExecCombiner;
typedef struct OpExecSideData OpExecSideData, *OpExecSide;
typedef struct TypeVarOpExecData TypeVarOpExecData, *TypeVarOpExec;
typedef struct TypeVarOpEntryData TypeVarOpEntryData, *TypeVarOpEntry;

struct OpEntrySideData
{
	union
	{
		HeapTuple ht;
		HeapTuplePtr htp;
		String str;
		size_t strlen;
	};

	union
	{
		void *data;
		int16_t i16;
		int32_t i32;
		int64_t i64;
		double dbl;
		float flt;
	};

	bool null;
};

struct TypeVarOpEntryData
{
	struct OpEntrySideData ret;
	struct OpEntrySideData lhs;
	struct OpEntrySideData rhs;
};

#define OP_MAX				(4)
#define OP_RET 				(0x01u)
#define OP_LHS 				(0x02u)
#define OP_RHS 				(0x04u)

typedef void (*opexecside_func)(TypeVarOpExec, TypeVarOpEntry, uint8_t sides);

struct OpExecCombinerData
{
	OpEntrySide data;
	OpExecSide exec; 
};

static int8_t opes_get_combiner(TypeVarOpExec exec,
								TypeVarOpEntry data,
								uint8_t sides,
								OpExecCombiner combine);

/*
 * OpExecSideData
 *
 * Stores stateful configuration from the parsing of the flags, but not the
 * data values themselves.  The member ordering is setup to minimize padding
 * in the structure.
 */
struct OpExecSideData
{
	SList list;
	size_t list_size;
	size_t entry_cursor;

	Type tys[VH_TAMS_MAX_DEPTH];
	opexecside_func begin;
	opexecside_func end;

	HeapTupleDef htd;
	
	void *formatters[VH_TAMS_MAX_DEPTH];

	int32_t dt_flags;
	
	uint16_t ty_depth;
	uint16_t ht_null;		/* Store the Null Flag Index */
	uint16_t ht_offset;		/* Store the HeapTuple offset for a field */
	
	bool modify;			/* Good chance we'll modify */
	bool by_val;			/* OpEntrySide->data is not a pointer, but a value */
	bool has_formatters;
};

/*
 * Begin/End Functions for:
 * 	HeapTuplePtr
 * 	HeapTuplePtr (read-only)
 * 	String
 */

static void opes_string_begin(TypeVarOpExec, TypeVarOpEntry, uint8_t);
static void opes_ht_begin(TypeVarOpExec, TypeVarOpEntry, uint8_t);

static void opes_htp_begin(TypeVarOpExec, TypeVarOpEntry, uint8_t);
static void opes_htp_end(TypeVarOpExec, TypeVarOpEntry, uint8_t);

static void opes_htpr_begin(TypeVarOpExec, TypeVarOpEntry, uint8_t);
static void opes_htpr_end(TypeVarOpExec, TypeVarOpEntry, uint8_t);

static void opes_var_begin(TypeVarOpExec, TypeVarOpEntry, uint8_t);


/*
 * Pin Strategy for HeapTuplePtr references.  This give us a simple multiple
 * option path for pinning HeapTuplePtr / HeapTuple.
 */
#define OP_PIN_RET			0x01
#define OP_PIN_LHS			0x02
#define OP_PIN_RHS			0x04

/*
 * Function Call to Use
 */
#define OP_FUNC_COMP		0x01
#define OP_FUNC_OPER		0x02
#define OP_FUNC_TAM_GEN		0x03
#define OP_FUNC_CSTR_SET	0x04
#define OP_FUNC_CSTR_GET	0x05
#define OP_FUNC_STR_GET 	0x06
#define OP_FUNC_STR_SET 	0x07

struct TypeVarOpExecData
{
	struct OpExecSideData ret;
	struct OpExecSideData lhs;
	
	/*
	 * Function to execute as once TypeVarOpEntry has its lhs_data
	 * and rhs_data members populated properly.
	 */
	union
	{
		vh_tom_comp comp;
		vh_tom_oper oper;
		vh_tam_generic tam_gen;
		vh_tam_cstr_get tam_cstr_get;
		vh_tam_cstr_set tam_cstr_set;
	} lr_func[VH_TAMS_MAX_DEPTH];

	int8_t pin_strategy;
	bool lhs_rhs_pin_same;				/* Pin LHS and RHS in the same call */
	bool lhs_rhs_ht_origin;

	struct OpExecSideData rhs;

	struct TypeVarOpEntryData ope;		/* RET, LHS, RHS Pinned Data */

	union
	{
		vh_tom_oper oper;
		vh_tam_generic tam_gen;
	} ret_func[VH_TAMS_MAX_DEPTH];

	const char *op;
	int16_t opi;

	int16_t ret_func_meth;				/* OP_FUNC_* family of defines: LHS->RET */
	int16_t lr_func_meth;				/* OP_FUNC_* family of defines: LHS->RHS */

	bool ret_lhs_pin_same;

	bool ret_lhs_tys_match;
	bool lhs_rhs_tys_match;
	bool lhs_is_ret;
};


static bool process_flags(TypeVarOpExec exec,
						  int32_t flags,
				   		  va_list argp);
static bool process_flags_fp(TypeVarOpExec exec,
							 va_list argp);


static bool process_dt_flag(TypeVarOpExec exec,
							OpExecSide side,
							OpExecSide side2,
							OpEntrySide entry,
							OpEntrySide entry2,
							int32_t flag,
							va_list argp);
static bool process_dt_flag_fp(TypeVarOpExec exec,
							   OpExecSide side,
		   					   OpExecSide side2,
	   						   OpEntrySide entry,
	   						   OpEntrySide entry2,
	   						   int32_t flag,
	   						   va_list argp);


static bool process_id_flag(TypeVarOpExec exec,
							OpExecSide side,
							OpExecSide side2,
							int32_t flag,
							va_list argp);

static void process_format_patterns(TypeVarOpExec exec);
static void cleanup_formatters(TypeVarOpExec exec);
static void cleanup_formatter(TypeVarOpExec exec, OpExecSide fmts);


#define OP_DT_RET_MASK		(0xff000000u)
#define OP_DT_LHS_MASK		(0x00ff0000u)
#define OP_ID_LHS_MASK		(0x0000f000u)
#define OP_DT_RHS_MASK		(0x00000ff0u)
#define OP_ID_RHS_MASK		(0x0000000fu)

#define OP_DT_RET_SHIFT 	24
#define OP_DT_LHS_SHIFT		16
#define OP_ID_LHS_SHIFT		12
#define OP_DT_RHS_SHIFT		4
#define OP_ID_RHS_SHIFT		0

/*
 * We make some fast path operators that fit into a two byte integer
 * so that we don't have to read both characters separately.  Just a
 * basic SWITCH to see if we're on a fast path operator lookup.
 */

#define VH_COMP_LT 			0x3c00u
#define VH_COMP_GT			0x3e00u
#define VH_COMP_LTEQ		0x3c3du
#define VH_COMP_GTEQ		0x3e3du
#define VH_COMP_EQ			0x3d3du
#define VH_COMP_NEQ			0x21edu

#define VH_OP_PL			0x2b00u
#define VH_OP_PLEQ			0x2b3du
#define VH_OP_PLPL			0x2b2bu
#define VH_OP_SU			0x2d00u
#define VH_OP_SUEQ			0x2d3du
#define VH_OP_SUSU			0x2d2du

#define VH_OP_MULT			0x2a00u
#define VH_OP_MULTEQ		0x2a3du
#define VH_OP_DIV 			0x2f00u
#define VH_OP_DIVEQ			0x2f3du
#define VH_OP_MOD			0x2500u

#define VH_OP_EQ 			0x3d00u


/*
 * We should be able to detect when an Operator Data Type is a HeapTuple.
 * Doing this allows us to set a memset routine to copy the varlen data if both
 * sides are from a HeapTuple origin.  We include HTP, HTR, and HT.
 */
#define DT_IS_HEAPTUPLE(dt)		(((dt) == VH_OP_DT_HTP) 		||				\
								 ((dt) == VH_OP_DT_HTR)			||				\
								 ((dt) == VH_OP_DT_HTM)			||				\
								 ((dt) == VH_OP_DT_HTI))

static bool comp_lookup_funcs(TypeVarOpExec ed);
static bool comp_exec(TypeVarOpExec tvope, int32_t *res, bool fp);

static bool op_lookup_funcs(TypeVarOpExec ed);
static int32_t op_exec(TypeVarOpExec tvope, bool fp);


void*
vh_typevar_make(int32_t nargs, ...)
{
	va_list ap;
	char *tname;
	Type ty;
	int32_t i, tag_count = 0;
	Type tys[VH_TAMS_MAX_DEPTH];

	va_start(ap, nargs);

	for (i = 0; i < VH_TAMS_MAX_DEPTH && i < nargs; i++)
	{
		tname = va_arg(ap, char*);

		if (!tname)
			break;

		/*
		 * Lookup the type by internal type name
		 */

		ty = vh_type_ctype(tname);

		if (ty)
		{
			tys[i] = ty;
			tag_count++;	
		}
		else
		{
			elog(ERROR1,
					emsg("Type %s specified at index %d could not be found "
					     "in the catalog.  Unable to initialize the "
					     "TypeVarSlot* as requested.",
					     tname,
					     i));
		}
	}

	if (i < VH_TAMS_MAX_DEPTH)
	{
		tys[i] = 0;
	}

	va_end(ap);

	return vh_typevar_make_tys(&tys[0]);
}

void*
vh_typevar_make_tys(Type *tys)
{
	Type ty_align;
	int32_t i, j, align_sz, align_factor;
	int8_t tag_count = 0;
	size_t sz = 0, alloc_sz, align_diff;
	TypeTag tags[VH_TAMS_MAX_DEPTH];
	TypeTag *tvm_tags;
	void *tvm_data;
	bool requires_construct = false;
		
	sz = vh_type_stack_data_width(tys);

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		if (!tys[i])
		{
			break;
		}

		tags[i] = tys[i]->id;

		if (tys[i]->construct_forhtd)
			requires_construct = true;
	}

	if (i < VH_TAMS_MAX_DEPTH)
		tag_count = i;
	else
		tag_count = VH_TAMS_MAX_DEPTH;

	/*
	 * We need to check some alignment issues, so that these can have a
	 * structure overlaid on top of them.  We check the outter most type
	 * in tys[tag_count - 1] for it's alignment and then do the difference 
	 * between where we're at right now sizeof(TypeTag) * tag_count and
	 * where we've got to be at ty_align->alignment.
	 *
	 * We then compute a factor to offset tvm_tags by before we drop into
	 * the loop assigning out the tags.
	 */

	ty_align = tys[0];
	align_sz = sizeof(TypeTag) * tag_count;

	if ((align_diff = (align_sz % ty_align->alignment)))
		align_factor = (ty_align->alignment - align_diff) / sizeof(TypeTag);
	else
		align_factor = 0;

	alloc_sz = sz + (sizeof(TypeTag) * (tag_count + align_factor));
	tvm_tags = vhmalloc(alloc_sz);
	tvm_tags += align_factor;
	tvm_data = tvm_tags + tag_count;

	assert(((uintptr_t)tvm_data) % ty_align->alignment == 0);

	/*
	 * Iterate the thru the tags, swapping positions and set the END_FLAG
	 * on the last tag.
	 */
	for (i = tag_count - 1, j = 0; i >= 0; i--, j++)
		tvm_tags[i] = tags[j] | VH_TYPETAG_MAGIC;

	tvm_tags[0] |= VH_TYPETAG_END_FLAG;

	/*
	 * We're going to call the constructor with an invalid HeapBuffer so that
	 * the underlying type allocates any out of line memory in the current 
	 * context.
	 */
	if (requires_construct)
	{
		tys[tag_count] = 0;
		vh_tom_fire_construct(tys, tvm_data, 0);
	}
	else
	{
		/*
		 * Zero us out instead
		 */
		memset(tvm_data, 0, sz);
	}

	return tvm_data;
}

/*
 * vh_typevar_make_tvs
 *
 * Uses the Type Stack from a TypeVarSlot to create a TypeVar.  More of a
 * convenience function for callers.
 */
void*
vh_typevar_make_tvs(TypeVarSlot *tvs)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	int8_t ty_depth;

	ty_depth = vh_tvs_fill_tys(tvs, tys);

	if (ty_depth)
	{
		return vh_typevar_make_tys(tys);
	}

	return 0;
}

/*
 * vh_typevar_tys_size
 * 
 * Estimates the size of the TypeVar for a given TypeStack.  Includes dead
 * alignment padding for the TypeTag to ensure the actual data point is
 * on the appropriate boundary.  Assumes we're going to be initializing on
 * an (uintptr_t) aligned boundary.
 */
size_t
vh_typevar_tys_size(Type *tys, size_t *var_offset)
{
	size_t alloc_sz, sz, max_align, align_factor;
	int8_t depth;

	vh_type_stack_properties(tys, -1, &depth, &sz, &max_align, 0);

	assert(depth);

	align_factor = typevar_align_factor(tys[0], depth);
	
	if (var_offset)
		*var_offset = sizeof(TypeTag) * (align_factor + depth);

	alloc_sz = sz + (sizeof(TypeTag) * (align_factor + depth));

	return alloc_sz;
}

static size_t
typevar_align_factor(Type ty, int8_t depth)
{
	size_t align_sz, align_diff, align_factor;

	align_sz = (sizeof(TypeTag) * (depth));

	if ((align_diff = (align_sz % ty->alignment)))
		align_factor = (ty->alignment - align_diff) / sizeof(TypeTag);
	else
		align_factor = 0;

	return align_factor;
}

/*
 * vh_typevar_init
 *
 * Initializes a TypeVar in memory that's already been allocated @at.  We don't
 * assume the caller has allocted enough space for us.
 */
void*
vh_typevar_init(void *at, size_t sz, Type *tys)
{
	char *cursor = at, *tvm_data;
	TypeTag *tvm_tags;	
	size_t alloc_sz, max_align, align_factor;
	int8_t i, j, depth;
	bool construct;
	
	/*
	 * Make sure we're on a (uintptr_t) boundary, otherwise this won't work
	 * very well.
	 */

	vh_type_stack_properties(tys, -1, &depth, &alloc_sz, &max_align, &construct);
	align_factor = typevar_align_factor(tys[0], depth);
	alloc_sz += (sizeof(TypeTag) * (align_factor + depth));
	
	assert(((uintptr_t)at) % max_align == 0);
	assert(sz >= alloc_sz);

	memset(at, 0, alloc_sz);

	tvm_tags = (TypeTag*)cursor;
	tvm_tags += align_factor;
	tvm_data = (char*)(tvm_tags + depth);

	for (i = depth - 1, j = 0; i >= 0; i--, j++)
		tvm_tags[i] = tys[j]->id | VH_TYPETAG_MAGIC;

	tvm_tags[0] |= VH_TYPETAG_END_FLAG;

	if (construct)
	{
		vh_tom_fire_construct(tys, tvm_data, 0);
	}

	return tvm_data;
}

/*
 * vh_typevar_create
 *
 * See comments in TypeVar.h
 *
 */
void*
vh_typevar_create(Type *tys, int32_t tag_count, int32_t *tag_align_factor,
				  size_t header_sz,
				  size_t footer_sz, size_t footer_align,
				  void **data_at, void **footer_at)
{
	size_t sz = header_sz + footer_sz, alloc_sz, align_diff;
	Type ty_align;
	int32_t i, j, align_sz, align_factor;
	TypeTag tags[VH_TAMS_MAX_DEPTH];
	TypeTag *tvm_tags;
	unsigned char *tvm_data, *tvm_head, *tvm_foot;
	bool requires_construct = false;

	sz = vh_type_stack_data_width(tys);

	for (i = 0; i < tag_count; i++)
	{
		tags[i] = tys[i]->id;

		if (tys[i]->construct_forhtd)
			requires_construct = true;
	}

	/*
	 * We need to check some alignment issues, so that these can have a
	 * structure overlaid on top of them.  We check the outter most type
	 * in tys[tag_count - 1] for it's alignment and then do the difference 
	 * between where we're at right now sizeof(TypeTag) * tag_count and
	 * where we've got to be at ty_align->alignment.
	 *
	 * We then compute a factor to offset tvm_tags by before we drop into
	 * the loop assigning out the tags.
	 */

	ty_align = tys[0];

	if (header_sz % 2)
		header_sz++;

	align_sz = header_sz + (sizeof(TypeTag) * (tag_count));

	if ((align_diff = (align_sz % ty_align->alignment)))
		align_factor = (ty_align->alignment - align_diff) / sizeof(TypeTag);
	else
		align_factor = 0;

	if (tag_align_factor)
		*tag_align_factor = align_factor;

	/*
	 * There's going to be some more alignment issues here, if header_sz isn't
	 * on a two byte boundary then we're in trouble.
	 *
	 * For the actual underlying data set itself, we have to take into account
	 * the header_sz plus the tags plus their alignment factor to make sure the
	 * data is going on the appropriate boundary.
	 *
	 * The footer is the same way, we need to compute the entire boundary of all
	 * the preceeding elements to ensure the footer is on the right boundary.
	 */
	alloc_sz = sz + header_sz + (sizeof(TypeTag) * (tag_count + align_factor));
	tvm_head = vhmalloc(alloc_sz);
	tvm_tags = ((TypeTag*)(tvm_head + header_sz));
	tvm_tags += align_factor;
	tvm_data = (unsigned char*)(tvm_tags + tag_count);
	tvm_foot = ((unsigned char*)tvm_data) + footer_sz;

	assert(((uintptr_t)tvm_data) % ty_align->alignment == 0);

	/*
	 * Zero this entire block out.
	 */
	memset(tvm_head, 0, alloc_sz);


	/*
	 * Iterate the thru the tags, swapping positions and set the END_FLAG
	 * on the last tag.
	 */
	for (i = tag_count - 1, j = 0; i >= 0; i--, j++)
		tvm_tags[i] = tags[j] | VH_TYPETAG_MAGIC;

	tvm_tags[0] |= VH_TYPETAG_END_FLAG;

	/*
	 * We're going to call the constructor with an invalid HeapBuffer so that
	 * the underlying type allocates any out of line memory in the current 
	 * context.
	 */
	if (requires_construct)
	{
		tys[tag_count] = 0;
		vh_tom_fire_construct(tys, tvm_data, 0);
	}

	if (data_at)
		*data_at = tvm_data;

	if (footer_at)
		*footer_at = tvm_foot;

	return tvm_head;
}

/*
 * vh_typevar_construct
 *
 * Fires the constructor on a typevar.
 */
void
vh_typevar_construct(void *typevar)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	TypeTag *tags = typevar;

	tags -= 1;

	/*
	 * Make sure we've got something sane here, a valid pointer and the
	 * TYPETAG_MAGIC flag set.
	 */
	if (typevar && (tags[0] & VH_TYPETAG_MAGIC))
	{
		fill_tys_from_tags_rev(&tys[0], &tags[0]);
		typevar_construct_impl(&tys[0], typevar);
	}
}

/*
 * vh_typevar_destroy
 *
 * Fires the finalize and the free on a typevar.  The free is a little tricky
 * because we've got to walk back the typevar.  See vh_typevar_make on how where
 * we allocate and the pointer that actually gets returned to the user are 
 * different.
 *
 * |typevar| becomes an invalid pointer and the user should discard it.
 */
void
vh_typevar_destroy(void *typevar)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	TypeTag *tags = typevar;
	int32_t stack_depth;

	tags -= 1;

	if (typevar)
	{
		stack_depth = fill_tys_from_tags_rev(&tys[0], tags);
		typevar_finalize_impl(&tys[0], typevar);
		
		/*
		 * Make sure we're not an array, if so the user should be smart
		 * enough to call the vh_typearr_destroy functions.
		 */

		if (!(tags[0] & VH_TYPETAG_ARRAY_FLAG))
			typevar_free_impl(&tys[0], stack_depth, typevar);
	}
}

/*
 * vh_typevar_finalize
 *
 * Fires the finalize on a typevar.
 *
 * |typevar| remains a valid pointer and the construct method should be called
 * on it to use the underlying type's storage.
 */
void
vh_typevar_finalize(void *typevar)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	TypeTag *tags = typevar;

	tags -= 1;

	if (typevar)
	{
		fill_tys_from_tags_rev(&tys[0], tags);
		typevar_finalize_impl(&tys[0], typevar);
	}
}

/*
 * vh_typevar_free
 *
 * Returns the space allocated by the typevar to the allocator.  Arrays
 * will be ignored and should use the vh_typearr_free infrastructure.
 */
void
vh_typevar_free(void *typevar)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	TypeTag *tags = typevar;
	int32_t stack_depth;

	tags -= 1;

	if (typevar)
	{
		if (tags[0] & VH_TYPETAG_ARRAY_FLAG)
		{
			elog(WARNING,
					emsg("vh_typevar_free was called with an array member.  "
						"Use vh_typearr_free to free the array and all of it's members."));
		}
		else
		{
			stack_depth = fill_tys_from_tags_rev(&tys[0], tags);
			typevar_free_impl(&tys[0], stack_depth, typevar);
		}
	}
}


/*
 * vh_typevar_reset
 *
 * Reset the typevar calling a finalize by a construct.
 */
void
vh_typevar_reset(void *typevar)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	TypeTag *tags = typevar;

	tags -= 1;

	if (typevar)
	{
		fill_tys_from_tags_rev(&tys[0], tags);
		typevar_finalize_impl(&tys[0], typevar);
		typevar_construct_impl(&tys[0], typevar);
	}
}

int8_t
vh_typevar_fill_stack(void *typevar, Type *tys)
{
	TypeTag *tags = typevar;
	int32_t depth;

	tags -= 1;

	if (typevar)
	{
		depth = fill_tys_from_tags_rev(tys, tags);

		return depth;
	}

	return 0;
}

int8_t
vh_typevar_fill_tags(void *typevar, TypeTag *tags)
{
	TypeTag *ttags = typevar;
	Type tys[VH_TAMS_MAX_DEPTH];
	int32_t depth, i;

	ttags -= 1;

	if (typevar)
	{
		depth = fill_tys_from_tags_rev(tys, ttags);

		for (i = 0; i < depth; i++)
		{
			tags[i] = tys[i]->id;
		}

		return depth;
	}

	return 0;
}

bool
vh_typevar_isa(void *typevar, Type ty)
{
	TypeTag tags[VH_TAMS_MAX_DEPTH], *ttags = typevar;
	int8_t depth;

	assert(typevar);
	assert(ty);

	ttags--;

	depth = fill_tags_rev(ttags, tags);

	if (depth == 1)
	{
		return (ty->id == tags[0]) ? true : false;
	}

	return false;
}

bool
vh_typevar_isatys(void *typevar, Type *tys)
{
	TypeTag tags[VH_TAMS_MAX_DEPTH], *ttags = typevar;
	int8_t i, depth;
	bool match = true;

	assert(typevar);

	ttags--;

	depth = fill_tags_rev(ttags, tags);

	for (i = 0; i < depth && match; i++)
	{
		if (tys[i] && tags[i])
		{
			match = (tys[i]->id == tags[i]) ? true : false;
		}
		else if (tys[i] == 0 && tags[i] == 0)
		{
			match = true;
			break;
		}
		else
		{
			match = false;
		}
	}

	if (match && i < VH_TAMS_MAX_DEPTH)
	{
		/*
		 * It's possible the typevar doesn't extend the full depth of the
		 * type stack.  If that's the case, then we don't have a match.
		 */
		match = (tys[i] == 0) ? true : false;
	}

	return match;
}

/*
 * vh_typevar_makecopy
 *
 * Makes a copy of the TypeVar, allocates in the current MemoryContext.  This
 * will call the memcpy TAM on the stack.
 */
void*
vh_typevar_makecopy(void *src)
{
	TypeTag *tags = src;
	Type tys[VH_TAMS_MAX_DEPTH];
	void *data;
	size_t sz = 0, align_diff, align_sz;
	int32_t type_depth, i, tag_factor, alignment;
	bool has_constructor = false;

	tags -= 1;

	type_depth = fill_tys_from_tags_rev(&tys[0], tags);
	
	if (!type_depth)
		elog(ERROR1, 
				emsg("Corrupt type stack, unable to copy the TypeVar"));

	alignment = tys[0]->alignment;
	align_sz = sizeof(TypeTag) * type_depth;

	if ((align_diff = (align_sz % alignment)))
		tag_factor = (alignment - align_diff) / sizeof(TypeTag);
	else
		tag_factor = 0;

	sz = vh_type_stack_data_width(tys);

	for (i = 0; i < type_depth; i++)
	{	

		if (tys[i]->construct_forhtd)
			has_constructor = true;
	}

	tags = vhmalloc((sizeof(TypeTag) * (tag_factor + type_depth)) + sz);
	tags += tag_factor;

	for (i = 0; i < type_depth; i++)
	{
		*tags = tys[i]->id | VH_TYPETAG_MAGIC;

		if (i == 0)
			*tags |= VH_TYPETAG_END_FLAG;

		tags += 1;
	}

	data = tags;

	if (has_constructor)
		vh_tom_fire_construct(&tys[0], data, 0);

	/*
	 * Do a deep copy of the values on the source to the target.
	 */
	vh_tam_fire_memset_set(&tys[0], src, data, false);

	return data;
}

/*
 * vh_typevar_copy
 *
 * Assumes both TypeVar have the same TypeStack.  Eventually we'll each this
 * thing to commute the types if we get handed different type stacks.
 *
 * Returns true if the copy to target was succesful.
 */
bool
vh_typevar_copy(void *tv_src, void *tv_tgt)
{
	TypeTag *tags_src = tv_src, *tags_tgt = tv_tgt;
	Type tys_src[VH_TAMS_MAX_DEPTH], tys_tgt[VH_TAMS_MAX_DEPTH];
	int32_t tyd_src, tyd_tgt, i;
	bool same_tags = true, has_constructor = false;

	tyd_src = fill_tys_from_tags_rev(&tys_src[0], tags_src);
	tyd_tgt = fill_tys_from_tags_rev(&tys_tgt[0], tags_tgt);

	if (tyd_src == tyd_tgt)
	{
		for (i = 0; i < tyd_src; i++)
		{
			if (tys_src[i]->id == tys_tgt[i]->id)
			{
				same_tags = false;
				break;
			}

			if (tys_src[i]->construct_forhtd)
				has_constructor = true;
		}

		if (same_tags)
		{
			if (has_constructor)
			{
				vh_tom_fire_destruct(&tys_tgt[0], tv_tgt);
				vh_tom_fire_construct(&tys_tgt[0], tv_tgt, 0);
			}

			vh_tam_fire_memset_set(&tys_src[0], tv_src, tv_tgt, false);

			return true;
		}
	}

	return false;
}

/*
 * vh_typevar_move
 *
 * Moves the value from source to target and then desconstructs source
 * and re-constructs it to accept values.
 */

bool
vh_typevar_move(void *tv_src, void *tv_tgt)
{
	TypeTag *tags_src = tv_src, *tags_tgt = tv_tgt;
	Type tys_src[VH_TAMS_MAX_DEPTH], tys_tgt[VH_TAMS_MAX_DEPTH];
	size_t sz = 0;
	int32_t tyd_src, tyd_tgt, i;
	bool same_tags = true, has_constructor = false;

	tyd_src = fill_tys_from_tags_rev(&tys_src[0], tags_src);
	tyd_tgt = fill_tys_from_tags_rev(&tys_tgt[0], tags_tgt);

	if (tyd_src == tyd_tgt)
	{
		for (i = 0; i < tyd_src; i++)
		{
			if (tys_src[i]->id == tys_tgt[i]->id)
			{
				same_tags = false;
				break;
			}

			sz += tys_src[i]->size;

			if (tys_src[i]->construct_forhtd)
				has_constructor = true;
		}

		if (same_tags)
		{
			if (has_constructor)
			{
				vh_tom_fire_destruct(&tys_tgt[0], tv_tgt);
				vh_tom_fire_construct(&tys_tgt[0], tv_tgt, 0);
			}

			vh_tam_fire_memset_set(&tys_src[0], tv_src, tv_tgt, false);

			if (has_constructor)
			{
				vh_tom_fire_destruct(&tys_src[0], tv_src);
				vh_tom_fire_construct(&tys_src[0], tv_src, 0);
			}
			else
			{
				memset(tv_src, 0, sz);
			}

			return true;
		}
	}

	return false;
}

/*
 * fill_tys_from_variadic
 *
 * Fills a type stack and type tags from a variadic function.  Returns the
 * size of types.
 */
static size_t 
fill_tys_from_variadic(Type *tys, TypeTag *tags, int32_t nargs, ...)
{
	va_list ap;
	const char *tname;
	Type ty;
	int32_t i, tag_count = 0;
	size_t sz = 0;

	va_start(ap, nargs);

	for (i = 0; i < VH_TAMS_MAX_DEPTH && i < nargs; i++)
	{
		tname = va_arg(ap, const char*);

		if (!tname)
			break;

		/*
		 * Lookup the type by internal type name
		 */

		ty = vh_type_ctype(tname);

		if (ty)
		{
			tags[i] = ty->id;
			tys[i] = ty;
			sz += ty->size;

			tag_count++;	
		}
		else
		{
			elog(ERROR1,
					emsg("Type %s specified at index %d could not be found "
					     "in the catalog.  Unable to initialize the "
					     "TypeVarSlot* as requested.",
					     tname,
					     i));
		}
	}

	va_end(ap);

	return sz;
}

/*
 * Fills a TypeTags working right to left on the TypeTag array until
 * a ZERO type tag is presented.  Inner most tag is at tags[0] while the
 * outtermost tag is at tags[0 - x] where x is the number of tags.
 */
static int32_t 
fill_tags_rev(TypeTag *tags, TypeTag *tgt)
{
	TypeTag *tagptr, tag, ltags[VH_TAMS_MAX_DEPTH];
	TypeVarArray *tva;
	int32_t i = 0, j = 0, tag_count = 0;

	if (tags[0] & VH_TYPETAG_ARRAY_FLAG)
	{
		tagptr = tags;

		while (tagptr[0] & VH_TYPETAG_ARRAY_FLAG &&
		       i < TYPEVAR_MAX_ARRAY_WALKS)
		{
			tagptr -= ((tagptr[0] & (~VH_TYPETAG_ARRAY_FLAG)) / sizeof(TypeTag));

			if (tagptr[0] & VH_TYPETAG_END_FLAG &&
					!(tagptr[0] & VH_TYPETAG_ARRAY_FLAG))
			{
				/*
				 * We're at the very first tag, so now all we have
				 * to do is find out how many tags to walk backwards for
				 * our TypeVarArray pointer.
				 */
				tva = (TypeVarArray*)(tagptr - (tagptr[0] & (~VH_TYPETAG_ID_MASK)));

				assert((*tva)->magic = TYPEARRAY_HEAD_MAGIC);

				for (i = 0; i <= (*tva)->type_depth; i++)
					tgt[i] = (*tva)->tys[i]->id;

				return (*tva)->type_depth;	
			}
			else
			{
				assert(tagptr[0] & VH_TYPETAG_ARRAY_FLAG);
			}

			i++;
		}

		if (!(tagptr[0] & VH_TYPETAG_MAGIC))
		{
			elog(ERROR1, emsg("Corrupt TypeTag after walking the Array"));
		}
	}
	else if (!(tags[0] & VH_TYPETAG_ARRAY_FLAG) &&
		 !(tags[0] & VH_TYPETAG_MAGIC) &&
		 (tags[0] & VH_TYPETAG_END_FLAG))
	{
		tva = (TypeVarArray*)(tags - (tags[0] & (~VH_TYPETAG_ID_MASK)));

		assert((*tva)->magic = TYPEARRAY_HEAD_MAGIC);

		for (i = 0; i <= (*tva)->type_depth; i++)
			tgt[i] = (*tva)->tys[i]->id;

		return (*tva)->type_depth;
	}
	else
	{
		tagptr = tags;
	}

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		tag = tags[0 - i];

		assert(tag & VH_TYPETAG_MAGIC);
		assert(!(tag & VH_TYPETAG_ARRAY_FLAG));

		ltags[i] = (tag & (~VH_TYPETAG_ID_MASK));

		if (ltags[i])
			tag_count++;

		if (tag & VH_TYPETAG_END_FLAG)
			break;
	}

	assert(tag_count <= VH_TAMS_MAX_DEPTH);

	/*
	 * We fill ltys from inner most tag to outter most (inner most is at
	 * ltys[0] but we need to flip that around.  The typestack always expects
	 * the outter most type in tys[0].
	 */
	for (i = tag_count - 1, j = 0; j < tag_count; i--, j++)
		tgt[j] = ltags[i];

	if (tag_count < VH_TAMS_MAX_DEPTH)
		tgt[tag_count] = 0;

	return tag_count;
}

/*
 * Fills a TypeStack working right to left on the TypeTag array until
 * a ZERO type tag is presented.  Inner most tag is at tags[0] while the
 * outtermost tag is at tags[0 - x] where x is the number of tags.
 */
static int32_t 
fill_tys_from_tags_rev(Type *tys, TypeTag *tags)
{
	TypeTag *tagptr, tag;
	Type ty, ltys[VH_TAMS_MAX_DEPTH];
	TypeVarArray *tva;
	int32_t i = 0, j = 0, tag_count = 0;

	if (tags[0] & VH_TYPETAG_ARRAY_FLAG)
	{
		tagptr = tags;

		while (tagptr[0] & VH_TYPETAG_ARRAY_FLAG &&
		       i < TYPEVAR_MAX_ARRAY_WALKS)
		{
			tagptr -= ((tagptr[0] & (~VH_TYPETAG_ARRAY_FLAG)) / sizeof(TypeTag));

			if (tagptr[0] & VH_TYPETAG_END_FLAG &&
					!(tagptr[0] & VH_TYPETAG_ARRAY_FLAG))
			{
				/*
				 * We're at the very first tag, so now all we have
				 * to do is find out how many tags to walk backwards for
				 * our TypeVarArray pointer.
				 */
				tva = (TypeVarArray*)(tagptr - (tagptr[0] & (~VH_TYPETAG_ID_MASK)));

				assert((*tva)->magic = TYPEARRAY_HEAD_MAGIC);

				for (i = 0; i <= (*tva)->type_depth; i++)
					tys[i] = (*tva)->tys[i];

				return (*tva)->type_depth;	
			}
			else
			{
				assert(tagptr[0] & VH_TYPETAG_ARRAY_FLAG);
			}

			i++;
		}

		if (!(tagptr[0] & VH_TYPETAG_MAGIC))
		{
			elog(ERROR1, emsg("Corrupt TypeTag after walking the Array"));
		}
	}
	else if (!(tags[0] & VH_TYPETAG_ARRAY_FLAG) &&
		 !(tags[0] & VH_TYPETAG_MAGIC) &&
		 (tags[0] & VH_TYPETAG_END_FLAG))
	{
		tva = (TypeVarArray*)(tags - (tags[0] & (~VH_TYPETAG_ID_MASK)));

		assert((*tva)->magic = TYPEARRAY_HEAD_MAGIC);

		for (i = 0; i <= (*tva)->type_depth; i++)
			tys[i] = (*tva)->tys[i];

		return (*tva)->type_depth;
	}
	else
	{
		tagptr = tags;
	}

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		tag = tags[0 - i];

		assert(tag & VH_TYPETAG_MAGIC);
		assert(!(tag & VH_TYPETAG_ARRAY_FLAG));

		ty = vh_type_tag(tag & (~VH_TYPETAG_ID_MASK));

		if (ty)
		{
			ltys[i] = ty;
			tag_count++;
		}
		else
		{
			elog(ERROR1,
					emsg("Unable to resolve TypeTag %d",
						tag & (~VH_TYPETAG_ID_MASK)));
		}

		if (tag & VH_TYPETAG_END_FLAG)
			break;
	}

	assert(tag_count <= VH_TAMS_MAX_DEPTH);

	/*
	 * We fill ltys from inner most tag to outter most (inner most is at
	 * ltys[0] but we need to flip that around.  The typestack always expects
	 * the outter most type in tys[0].
	 */
	for (i = tag_count - 1, j = 0; j < tag_count; i--, j++)
		tys[j] = ltys[i];

	if (tag_count < VH_TAMS_MAX_DEPTH)
		tys[tag_count] = 0;

	return tag_count;
}


/*
 * fill_tys_from_tags_fwd
 *
 * Tags have the outter most tag at tags[0] and the inner most tag at tags[x]
 * where X is the number of tags as indicated by the VH_TYPETAG_END_FLAG.
 *
 * >> tags[0] >>
 */
static int32_t 
fill_tys_from_tags_fwd(Type *tys, TypeTag *tags)
{
	TypeTag *tagptr, tag;
	Type ty;
	int32_t i = 0, tag_count = 0;

	if (tags[0] & VH_TYPETAG_ARRAY_FLAG)
	{
		tagptr = &tags[0];

		while (tagptr[0] & VH_TYPETAG_ARRAY_FLAG &&
		       i < TYPEVAR_MAX_ARRAY_WALKS)
		{
			tagptr -= ((tagptr[0] & (~VH_TYPETAG_ARRAY_FLAG)) + sizeof(TypeTag));
			i++;
		}
	}
	else
	{
		tagptr = tags;
	}

	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		tag = tags[i];

		assert(tag & VH_TYPETAG_MAGIC);
		assert(!(tag & VH_TYPETAG_ARRAY_FLAG));

		ty = vh_type_tag(tag & (~VH_TYPETAG_ID_MASK));

		if (ty)
		{
			tys[i] = ty;
			tag_count++;
		}
		else
		{
			elog(ERROR1,
					emsg("Unable to resolve TypeTag %d",
						tag));
		}

		if (tag & VH_TYPETAG_END_FLAG)
		{
			tys[i + 1] = 0;
			break;
		}
	}

	return tag_count;
}

/*
 * typevar_construct_impl
 *
 * We always work in HeapBuffer 0 on these, so that the underlying type uses
 * the current MemoryContext to do out of line allocations.
 */
static void 
typevar_construct_impl(Type *tys, void *data)
{
	vh_tom_fire_construct(&tys[0], data, 0);
}

/*
 * typevar_free_impl
 *
 * This can get a little tricky because we've got to deal with some alignment
 * issues to get to the original pointer that was allocated by vh_typevar_make.
 */
static void 
typevar_free_impl(Type *tys, int32_t type_depth, void *data)
{
	TypeTag *tags = data;
	Type ty_align = tys[0];
	size_t align_sz, align_diff;
	int32_t align_factor;

	align_sz = sizeof(TypeTag) * type_depth;

	if ((align_diff = (align_sz % ty_align->alignment)))
		align_factor = (ty_align->alignment - align_diff) / sizeof(TypeTag);
	else
		align_factor = 0;

	tags -= (type_depth + align_factor);

	vhfree(tags);
}

static void 
typevar_finalize_impl(Type *tys, void *data)
{
	vh_tom_fire_destruct(&tys[0], data);
}



/*
 * ===========================================================================
 * TypeVarArray
 * ===========================================================================
 */

void*
vh_typearray_make(int32_t entries, int32_t nargs, ...)
{
	va_list ap;
	const char *tname;
	Type ty;
	int32_t i, j, tag_count = 0;
	size_t sz = 0, align_diff;
	TypeTag tags[VH_TAMS_MAX_DEPTH];
	Type tys[VH_TAMS_MAX_DEPTH], ty_align;
	TypeVarArray tva = 0;
	bool requires_construct = false;

	va_start(ap, nargs);

	for (i = 0; i < VH_TAMS_MAX_DEPTH && i < nargs; i++)
	{
		tname = va_arg(ap, const char*);

		if (!tname)
			break;

		/*
		 * Lookup the type by internal type name
		 */

		ty = vh_type_ctype(tname);

		if (ty)
		{
			tags[i] = ty->id;
			tys[i] = ty;

			if (ty->construct_forhtd)
				requires_construct = true;

			tag_count++;	
		}
		else
		{
			elog(ERROR1,
					emsg("Type %s specified at index %d could not be found "
					     "in the catalog.  Unable to create the array of "
					     "TypeVar as requested.",
					     tname,
					     i));
		}
	}

	va_end(ap);

	if (tag_count)
	{
		sz = vh_type_stack_data_width(tys);

		ty_align = tys[0];

		tva = vhmalloc(sizeof(struct TypeVarArray) +
				(sizeof(TypeTag) * (tag_count - 1)));
		tva->magic = TYPEARRAY_HEAD_MAGIC;
		tva->size = 0;
		tva->capacity = 0;
		
		tva->toc = 0;
		tva->array = 0;

		tva->alignment = ty_align->alignment;

		if ((align_diff = ((sizeof(TypeTag) * tag_count) % tva->alignment)))
			tva->alignment_tag_factor = (tva->alignment - align_diff) / sizeof(TypeTag);
		else
			tva->alignment_tag_factor = 0;

		tva->type_depth = tag_count;

		tva->tys_size = sz;
		tva->tys_span = sz + (sizeof(TypeTag) * (1 + tva->alignment_tag_factor));

		for (i = 0, j = tag_count - 1; j >= 0; j--, i++)
		{
			tva->tys[i] = tys[j];
			tva->tags[i] = tags[j] | VH_TYPETAG_MAGIC;
		}

		tva->tags[0] |= VH_TYPETAG_END_FLAG;
		tva->tys[tag_count] = 0;

		typearray_expand_impl(tva, entries, false);

		tva->size = entries;
	}
	else
	{
		elog(ERROR1,
				emsg("No type names could be resolved when attempting "
				     "to form an array of TypeVar.  Unable to proceeed."));
	}

	return tva;
}

/*
 * vh_typearray_toc
 *
 * Returns a table of contents so that TypeVars may be accessed via indexes
 */

void**
vh_typearray_toc(void *typearray)
{
	TypeVarArray tva = typearray;
	unsigned char *data;
	int32_t i;
	void **toc;

	if (tva && tva->magic == TYPEARRAY_HEAD_MAGIC)
	{
		if (tva->toc)
			return tva->toc;

		toc = vhmalloc(sizeof(void**) * tva->size);

		if (!toc)
		{
			return 0;
		}

		data = (unsigned char*)(tva->array + 1);
		data += (sizeof(TypeTag) * (1 + tva->alignment_tag_factor));

		for (i = 0; i < tva->size; i++)
		{
			toc[i] = data;
			data += tva->tys_span;
		}

		tva->toc = toc;

		return toc;
	}

	elog(WARNING,
			emsg("Unable to build a table of contents, the value at [%p] "
				 "is not a TypeVarArray or may be corrupt.",
				 typearray));

	return 0;
}

int32_t
vh_typearray_size(void *typearray)
{
	TypeVarArray tva = typearray;

	if (tva && tva->magic == TYPEARRAY_HEAD_MAGIC)
	{
		return tva->size;
	}

	return 0;
}

void*
vh_typearray_at(void *typearray, int32_t idx)
{
	TypeVarArray tva = typearray;
	unsigned char *array;

	if (tva && tva->magic == TYPEARRAY_HEAD_MAGIC)
	{
		if (idx >= 0 &&
		    idx < tva->size)
		{
			/*
			 * Adjust for the root pointer to the TypeVarArray plus
			 * the span for the index and then the TypeTags with full
			 * alignment.
			 */
			array = (unsigned char*)(tva->array + 1);
			array += (tva->tys_span * idx);
			array += sizeof(TypeTag) * (1 + tva->alignment_tag_factor);

			return array;
		}
		else
		{
			elog(ERROR1,
					emsg("Array index %d out of bounds, array only contains %d"
						"entries",
						idx,
						tva->size));
		}

	}

	return 0;
}

/*
 * vh_typearray_push
 *
 * We push a TypeVar to the end of the array using a copy or move semantic.
 *
 * If the array needs to be expanded then we'll go ahead and do that too.
 */
void
vh_typearray_push(void *typearray, void *typevar, bool move)
{
	TypeVarArray tva = typearray;
	TypeTag *tag_src = typevar;
	Type tys_src[VH_TAMS_MAX_DEPTH];
	int32_t src_depth, i;
	unsigned char *target;
	bool same_type = true;

	tag_src -= 1;

	if (tva->size + 1 > tva->capacity)
	{
		typearray_expand_impl(tva, tva->capacity + 10, false);
	}

	/*
	 * Check to make sure these are of the same type.  We cheat and 
	 * use the TypeVarArray type stack.
	 */

	src_depth = fill_tys_from_tags_rev(&tys_src[0], tag_src);

	if (src_depth == tva->type_depth)
	{
		for (i = 0; i < src_depth; i++)
		{
			if (tva->tys[i] != tys_src[i])
			{
				same_type = false;
				break;
			}
		}

		if (same_type)
		{
			target = (unsigned char*)(tva->array + 1);
			target += ((tva->tys_span * tva->size) + (sizeof(TypeTag) * (1 + tva->alignment_tag_factor)));


			if (move)
			{
				elog(ERROR1,
						emsg("Move semantics not implemented"));
			}
			else
			{
				vh_tam_fire_memset_set(&tys_src[0], typevar, target, false);
			}

			tva->size++;
		}
		else
		{
			elog(ERROR1,
					emsg("Detination array and source type do not have the "
						"same type stack.  Unable to push the TypeVar "
						"into the desired array."));
		}
	}
}

/*
 * vh_typearray_pop
 *
 * Pop the last TypeVar off the array and destruct it if necessary.  Shrink
 * the size of the array by one so that we don't try to double destruct.
 */
void
vh_typearray_pop(void *typearray)
{
	TypeVarArray tva = typearray;
	unsigned char *target;

	if (tva && tva->magic == TYPEARRAY_HEAD_MAGIC)
	{
		if (tva->has_constructor)
		{
			target = (unsigned char*)(tva->array + 1);
			target += ((tva->tys_span * (tva->size - 1)) + (sizeof(TypeTag) * (1 + tva->alignment_tag_factor)));
			
			vh_tom_fire_destruct(&tva->tys[0], target); 
		}

		tva->size--;
	}
}

/*
 * vh_typearray_iterate
 *
 * Iterates the array, calling the call back until the array has been fully
 * iterated or the callback returns false.
 */
bool
vh_typearray_iterate(void *typearray, vh_typearray_iterate_cb cb, void *cb_data)
{
	TypeVarArray tva = typearray;
	unsigned char *data;
	int32_t i;
	bool cb_ret;

	if (tva && tva->magic == TYPEARRAY_HEAD_MAGIC)
	{
		data = (unsigned char*)(tva->array + 1);
		data += (sizeof(TypeTag) * (tva->alignment_tag_factor + 1));

		for (i = 0; i < tva->size; i++)
		{
			cb_ret = cb(typearray,			/* typearray */
				    &tva->tys[0],		/* Type Stack */
				    i,				/* Index */
				    data,			/* TypeVar */
				    cb_data);

			if (!cb_ret)
				return false;

			data += tva->tys_span;
		}

		return true;
	}

	return false;
}

/*
 * vh_typearray_finalize
 *
 * Releases all memory contained by the types and by the array.  Does not free
 * the TypeVarArray structure passed to the user.
 */
void
vh_typearray_finalize(void *ta)
{
	TypeVarArray tva = ta;
	unsigned char *array_root, *array_idx;
	int32_t i = 0;
	vh_tom_destruct funcs[VH_TAMS_MAX_DEPTH];

	if (tva && tva->magic == TYPEARRAY_HEAD_MAGIC)
	{
		if (tva->has_constructor)
		{
			/*
			 * Fetch our destructor so we can fire the array instead of having to
			 * do the lookup each time.
			 */
			vh_toms_fill_destruct_funcs(&tva->tys[0], &funcs[0]);

			/*
			 * Find the root of the array and then go ahead and jump over the
			 * tags (including the alignment factor).  This way we can just stride
			 * the tva->tys_span each time in the loop without any extra math.
			 */
			array_root = (unsigned char*)(tva->array + 1);
			array_idx = array_root + (sizeof(TypeTag) * (1 + tva->alignment_tag_factor));

			for (i = 0; i < tva->size; i++)
			{
				vh_tom_firea_destruct(&tva->tys[0], &funcs[0], array_idx);
				array_idx += tva->tys_span;
			}
		}

		if (tva->array)
			vhfree(tva->array);

		if (tva->toc)
			vhfree(tva->toc);

		tva->array = 0;
		tva->toc = 0;

		tva->capacity = 0;
		tva->size = 0;
	}
	else
	{
		elog(ERROR2,
			emsg("Invalid TypeArray pointer passed to vh_typearray_finalize"));
	}
}

void
vh_typearray_free(void *ta)
{
	TypeVarArray tva = ta;

	if (tva && tva->magic == TYPEARRAY_HEAD_MAGIC)
	{
		if (tva->array ||
		    tva->toc)
		{
			elog(WARNING,
					emsg("vh_typearray_free called at %p but the TypeArray has not been "
						"finalized.  Free will not release memory acquired by the "
						"the TypeVarArray.",
						ta));
		}

		vhfree(ta);
	}
}

void
vh_typearray_destroy(void *ta)
{
	TypeVarArray tva = ta;

	if (tva && tva->magic == TYPEARRAY_HEAD_MAGIC)
	{
		vh_typearray_finalize(ta);
		vh_typearray_free(ta);
	}
}

/*
 * typearray_expand_impl
 *
 * Expands an array to a minimum capacity.
 */
static void
typearray_expand_impl(TypeVarArray tva, size_t min_capacity, bool build_toc)
{
	unsigned char **array, *index;
	TypeTag *tag;
	unsigned char *data, *adata, *ldata;
	int32_t i;
	int16_t walkbacks = 0;
	vh_tom_construct funcs[VH_TAMS_MAX_DEPTH];


	/*
	 * We'd rather fire an array than lookup the constructor for each
	 * and every instance of the type in the array, so fetch that those
	 * constructor functions now.
	 */
	if (tva->has_constructor)
		vh_toms_fill_construct_funcs(&tva->tys[0], &funcs[0]);	

	if (tva->array)
	{
		assert(tva->capacity);
		assert(tva->capacity < min_capacity);

		array = vhrealloc(tva->array, min_capacity * tva->tys_span);
		index = (unsigned char*)(array + 1);
		index += (tva->tys_span * tva->capacity);

		memset(index, 0, (min_capacity - tva->capacity) * tva->tys_span);

		/*
		 * We should really calculate the last walk back index used in the
		 * original array to minimize the number of walkbacks.  For now,
		 * we'll just use the last original index as the walkback.
		 */

		adata = index - tva->tys_span;
		adata += sizeof(TypeTag) * (1 + tva->alignment_tag_factor);
		assert((uintptr_t)adata % tva->alignment == 0);

		for (i = min_capacity - tva->capacity; i < min_capacity; i++)
		{
			tag = (TypeTag*)(index + (sizeof(TypeTag) * tva->alignment_tag_factor));
			data = (unsigned char*)(tag + 1);

			assert((uintptr_t)data % tva->alignment == 0);
			
			if (((uint16_t)(data - adata)) >= (~VH_TYPETAG_ARRAY_FLAG))
			{
				adata = ldata;
				walkbacks++;
			}

			*tag = ((TypeTag)(data - adata)) | VH_TYPETAG_ARRAY_FLAG;

			if (tva->has_constructor)
				vh_tom_firea_construct(&tva->tys[0], &funcs[0], data, 0);

			index += tva->tys_span;
			ldata = data;
		}	
	}
	else
	{
		assert(!tva->capacity);

		array = vhmalloc((min_capacity * tva->tys_span) + sizeof(uintptr_t));
		*array = (unsigned char*)tva;

		index = (unsigned char*)(array + 1);
		data = index;

		memset(index, 0, min_capacity * tva->tys_span);
		
		for (i = 0; i < min_capacity; i++)
		{
			tag = (TypeTag*)(index + (sizeof(TypeTag) * tva->alignment_tag_factor));
			data = (unsigned char*)(tag + 1);

			assert((uintptr_t)data % tva->alignment == 0);

			if (i == 0)
			{
				adata = data;

				/*
				 * For the first tag in an array, we set the end flag and indicate
				 * the number of tags to walk backwards by to get a pointer to
				 * the TypeVarArray structure.
				 */
				*tag = (sizeof(uintptr_t) / sizeof(TypeTag)) + tva->alignment_tag_factor;
				*tag |= VH_TYPETAG_END_FLAG;
			}
			else
			{
				/*
				 * Check to see if our span is too great to walk back, if so we
				 * need to replace adata (the last anchor rollback) with ldata.
				 * ldata now becomes the anchor walkback link.
				 */
				if ((uint16_t)(data - adata) >= (uint16_t)~VH_TYPETAG_ARRAY_FLAG)
				{
					adata = ldata;
					walkbacks++;
				}

				*tag = ((TypeTag)(data - adata)) | VH_TYPETAG_ARRAY_FLAG;
			}

			if (tva->has_constructor)
				vh_tom_firea_construct(&tva->tys[0], &funcs[0], data, 0);

			index += tva->tys_span;
			ldata = data;
		}
		
		tva->capacity = min_capacity;
		tva->array = (struct TypeVarArray**)array;
	}
}



/*
 * ===========================================================================
 * TypeVar Operators
 * ===========================================================================
 */

bool
vh_typevar_comp(const char *op, int32_t flags, ...)
{
	va_list ap;
	struct TypeVarOpExecData ed = { };
	uint16_t opi = (uint16_t)((op[0] << 8) | op[1]);

	va_start(ap, flags);
	process_flags(&ed, flags, ap);
	va_end(ap);

	ed.opi = opi;
	ed.op = op;

	if (comp_lookup_funcs(&ed))
		return comp_exec(&ed, 0, false);
	else
		elog(ERROR1, emsg("Unable to resolve functions to complete the comparison "
						  "operator requested."));

	return false;
}

int32_t
vh_typevar_comp_impl(int32_t flags, ...)
{
	va_list ap;
	struct TypeVarOpExecData ed = { };
	int32_t res;

	va_start(ap, flags);
	process_flags(&ed, flags, ap);
	va_end(ap);

	ed.opi = 0;
	ed.op = 0;

	if (comp_lookup_funcs(&ed))
	{
		comp_exec(&ed, &res, false);

		return res;
	}
	else
	{
		elog(ERROR2,
				emsg("Unable to resolve functions to complete the comparison "
					 "operator requested"));
	}

	return 0;
}

void*
vh_typevar_op(const char *op, int32_t flags, ...)
{	
	va_list ap;
	struct TypeVarOpExecData ed = { };
	uint16_t opi = (uint16_t)((op[0] << 8) | op[1]);
	void *ret;
	int32_t op_ret;
	union { uintptr_t ptr; void *data; } *ret_by_val;

	va_start(ap, flags);
	process_flags(&ed, flags, ap);
	va_end(ap);

	ed.opi = opi;
	ed.op = op;

	if (op_lookup_funcs(&ed))
		op_exec(&ed, false);
	else
		elog(ERROR1, emsg("Unable to resolve functions to complete the "
						  "operator requested."));

	/*
	 * Cleaup any formatters we may have used.
	 */
	cleanup_formatters(&ed);

	if (ed.ret_func_meth)
	{
		/*
		 * We need to stand up our actual return type.  The type represented
		 * by ed.ret should match the LHS.
		 */

		ret = 0;

		switch (ed.ret_func_meth)
		{
			case OP_FUNC_OPER:
				
				op_ret = vh_tom_firea_oper(&ed.ret.tys[0],			/* LHS Stack */
										   &ed.ret_func[0].oper,	/* Funcs */
										   &ed.ret.tys[0],			/* RHS Stack */
										   &ret,					/* LHS Data */
										   ed.ope.ret.data,			/* RHS Data */
										   0);						/* RET Data */

				if (op_ret)
				{
				}

				vh_typevar_destroy(ed.ope.ret.data);

				return ret;
		}
	}

	if (ed.ret.by_val)
	{
		ret_by_val = ed.ope.ret.data;
		return ret_by_val->data;
	}

	return ed.ope.ret.data;
}

TypeVarOpExec
vh_typevar_comp_init(const char *op, int32_t flags, ...)
{
	va_list ap;
	TypeVarOpExec ed;
	uint16_t opi = (uint16_t)((op[0] << 8) | op[1]);

	ed = vhmalloc(sizeof(struct TypeVarOpExecData));
	memset(ed, 0, sizeof(struct TypeVarOpExecData));
	
	ed->op = op;
	ed->opi = opi;

	/*
	 * Capture our type stack by calling process_flags.  This is the regular route
	 * which captures the stack.  The fast path route will be invoked vhen
	 * vh_typevar_comp_fp is called.
	 */
	va_start(ap, flags);
	process_flags(ed, flags, ap);
	va_end(ap);

	if (comp_lookup_funcs(ed))
		return ed;
	else
		elog(ERROR1, emsg("Unable to find valid comparison functions for the requested "
						  "types."));

	return 0;
}

TypeVarOpExec
vh_typevar_comp_init_tys(const char *op, 
						 Type *tys_lhs, Type *tys_rhs)
{
	TypeVarOpExec ed;
	uint16_t opi = (uint16_t)((op[0] << 8) | op[1]);

	ed = vhmalloc(sizeof(struct TypeVarOpExecData));
	memset(ed, 0, sizeof(struct TypeVarOpExecData));

	ed->op = op;
	ed->opi = opi;

	if (tys_lhs)
	{
		ed->lhs.ty_depth = vh_type_stack_copy(&ed->lhs.tys[0], tys_lhs);
		ed->lhs.dt_flags = VH_OP_DT_VAR;
	}

	if (tys_rhs)
	{
		ed->rhs.ty_depth = vh_type_stack_copy(&ed->rhs.tys[0], tys_rhs);
		ed->rhs.dt_flags = VH_OP_DT_VAR;
	}

	if (tys_lhs && tys_rhs)
	{
		ed->lhs_rhs_tys_match = vh_type_stack_match(&ed->lhs.tys[0], &ed->rhs.tys[0]);
	}


	if (comp_lookup_funcs(ed))
	{
		return ed;
	}
	else
	{
		elog(ERROR1,
				emsg("Unable to determine comparison operator functions "
					 "for the requested types"));
	}

	return 0;
}

TypeVarOpExec
vh_typevar_op_init(const char *op, int32_t flags, ...)
{
	va_list ap;
	TypeVarOpExec ed;
	uint16_t opi = (uint16_t)((op[0] << 8) | op[1]);

	ed = vhmalloc(sizeof(struct TypeVarOpExecData));
	memset(ed, 0, sizeof(struct TypeVarOpExecData));
	
	ed->op = op;
	ed->opi = opi;

	/*
	 * Capture our stack by calling process_flags.  This is the regular route
	 * which captures the stack.  The fast path route will be invoked when
	 * vh_typevar_op_fp is called.
	 */
	va_start(ap, flags);
	process_flags(ed, flags, ap);
	va_end(ap);

	if (op_lookup_funcs(ed))
		return ed;
	else
		elog(ERROR1,
				emsg("Unable to find valid operator functions for the requested types."));

	return 0;
}

TypeVarOpExec
vh_typevar_op_init_tys(const char *op, Type *tys_lhs, 
					   Type *tys_rhs, Type *tys_ret)
{
	TypeVarOpExec ed;
	uint16_t opi = (uint16_t)((op[0] << 8) | op[1]);

	ed = vhmalloc(sizeof(struct TypeVarOpExecData));
	memset(ed, 0, sizeof(struct TypeVarOpExecData));

	ed->op = op;
	ed->opi = opi;

	if (tys_lhs)
	{
		ed->lhs.ty_depth = vh_type_stack_copy(&ed->lhs.tys[0], tys_lhs);
		ed->lhs.dt_flags = VH_OP_DT_VAR;
	}

	if (tys_rhs)
	{
		ed->rhs.ty_depth = vh_type_stack_copy(&ed->rhs.tys[0], tys_rhs);
		ed->rhs.dt_flags = VH_OP_DT_VAR;
	}

	if (tys_ret)
	{
		ed->ret.ty_depth = vh_type_stack_copy(&ed->ret.tys[0], tys_ret);
		ed->ret.dt_flags = VH_OP_DT_VAR;

		ed->ope.ret.data = vh_typevar_make_tys(&ed->ret.tys[0]);
	}

	if (tys_lhs && tys_ret)
	{
		ed->ret_lhs_tys_match = vh_type_stack_match(&ed->ret.tys[0], &ed->lhs.tys[0]);
	}

	if (tys_lhs && tys_rhs)
	{
		ed->lhs_rhs_tys_match = vh_type_stack_match(&ed->lhs.tys[0], &ed->rhs.tys[0]);
	}

	if (op_lookup_funcs(ed))
		return ed;
	else
		elog(ERROR1,
				emsg("Unable to determine operator functions for the requested types"));

	return 0;
}

void
vh_typevar_comp_swap_op(TypeVarOpExec tvope, const char *op)
{
	uint16_t opi = (uint16_t)((op[0] << 8) | op[1]);

	tvope->op = op;
	tvope->opi = opi;

	if (!comp_lookup_funcs(tvope))
		elog(ERROR1, emsg("Unable to find valid comparison functions for the requested "
						  "types.  Operator not swapped as requested."));

}


void
vh_typevar_comp_destroy(TypeVarOpExec tvope)
{
	if (tvope)
	{
		vhfree(tvope);
	}
}

void
vh_typevar_op_destroy(TypeVarOpExec tvope)
{
	cleanup_formatters(tvope);
	vhfree(tvope);
}

bool
vh_typevar_comp_fp(TypeVarOpExec tvope, ...)
{
	va_list ap;

	if (tvope)
	{
		va_start(ap, tvope);
		process_flags_fp(tvope, ap);
		va_end(ap);

		return comp_exec(tvope, 0, true);
	}

	elog(ERROR1,
			emsg("Invalid TypeVarOpExec pointer [%p] passed to "
				 "vh_typevar_comp_fp",
				 tvope));

	return false;
}

void*
vh_typevar_op_fp(TypeVarOpExec tvope, ...)
{
	va_list ap;
	void *ret;
	union { uintptr_t ptr; void *data; } *ret_by_val;
	int32_t op_ret;

	if (tvope)
	{
		va_start(ap, tvope);
		process_flags_fp(tvope, ap);
		va_end(ap);

		op_ret = op_exec(tvope, true);

		if (tvope->ret_func_meth)
		{
			ret = 0;

			switch (tvope->ret_func_meth)
			{
				case OP_FUNC_OPER:
					op_ret = vh_tom_firea_oper(&tvope->ret.tys[0],		/* LHS Stack */
											   &tvope->ret_func[0].oper,
											   &tvope->lhs.tys[0],
											   &ret,
											   tvope->ope.ret.data,
											   0);

					if (op_ret)
					{
					}

					return ret;	
			}
		}

		if (tvope->ret.by_val)
		{
			ret_by_val = tvope->ope.ret.data;
			return ret_by_val->data;
		}

		return tvope->ope.ret.data;
	}

	return 0;
}

/*
 * process_flags
 *
 * When this is done we should have TypeStacks on both sides.  It's up the caller
 * to resolve the functions to run.
 */
static bool 
process_flags(TypeVarOpExec ed, int32_t flags, va_list argp)
{
	int32_t ret_dt, lhs_dt, rhs_dt, 
		lhs_id, rhs_id, 
		arg_count = 0, 
		i = 0, 
		idx[5], idx_flags[5];
	bool lhs_is_ht_origin = false;

	ret_dt = (flags & OP_DT_RET_MASK) >> OP_DT_RET_SHIFT;
	lhs_dt = (flags & OP_DT_LHS_MASK) >> OP_DT_LHS_SHIFT;
	rhs_dt = (flags & OP_DT_RHS_MASK) >> OP_DT_RHS_SHIFT;
	lhs_id = (flags & OP_ID_LHS_MASK) >> OP_ID_LHS_SHIFT;
	rhs_id = (flags & OP_ID_RHS_MASK) >> OP_ID_RHS_SHIFT;

	/*
	 * Calculate the number of arguments we have and set up some slots
	 * so that we're in the VAARGS loop we know what we're dealing with.
	 */
	if (ret_dt)
	{
		/*
		 * We want to make sure the RET value is passed in as an argument, if 
		 * it's not then there won't be anything to pull.
		 */
		if (ret_dt & VH_OP_DT_BOTH)
		{
			arg_count++;
			idx[i] = OP_DT_RET_MASK;
			idx_flags[i] = ret_dt;
			i++;
		}
	}

	if (lhs_dt)
	{
		arg_count++;
		idx[i] = OP_DT_LHS_MASK;
		idx_flags[i] = lhs_dt;
		i++;
	}

	if (lhs_id)
	{
		arg_count++;
		idx[i] = OP_ID_LHS_MASK;
		idx_flags[i] = lhs_id;
		i++;
	}

	if (rhs_dt)
	{
		arg_count++;
		idx[i] = OP_DT_RHS_MASK;
		idx_flags[i] = rhs_dt;
		i++;
	}

	if (rhs_id)
	{
		arg_count++;
		idx[i] = OP_ID_RHS_MASK;
		idx_flags[i] = rhs_id;
		i++;
	}

	for (i = 0; i < arg_count; i++)
	{
		switch (idx[i])
		{
		case OP_DT_RET_MASK:
			process_dt_flag(ed, &ed->ret, 0,
							&ed->ope.ret, 0,
							idx_flags[i], argp);
			ed->ret.dt_flags = idx_flags[i];

			break;

		case OP_DT_LHS_MASK:

			if (idx_flags[i] & VH_OP_DT_BOTH)
			{
				process_dt_flag(ed, 
						&ed->lhs, &ed->rhs,
					        &ed->ope.lhs, &ed->ope.rhs,	
						idx_flags[i], argp);

				ed->lhs.dt_flags = idx_flags[i];
				ed->rhs.dt_flags = idx_flags[i];

				/*
				 * Check if we're a HeapTuple-ish data type.  If we are, we may
				 * need to call any memset operation we try a little differently
				 * to ensure we don't copy of varlen data from a non-HeapTuple
				 * source.  Non-HeapTuple sources don't have a HeapBuffer assigned
				 * and allocate into the current memory context.
				 */
				if (DT_IS_HEAPTUPLE(idx_flags[i]))
					ed->lhs_rhs_ht_origin = true;
			}
			else
			{
				process_dt_flag(ed, 
						&ed->lhs, 0, 
						&ed->ope.lhs, 0,
						idx_flags[i], argp);

				ed->lhs.dt_flags = idx_flags[i];

				/*
				 * Set the local flag lhs_is_ht_origin so that when we process the
				 * right hand side we can easily make a decision about setting
				 * ed->lhs_rhs_is_ht_orign.
				 */
				if (DT_IS_HEAPTUPLE(idx_flags[i]))
					lhs_is_ht_origin = true;
			}

			break;

		case OP_ID_LHS_MASK:

			if (idx_flags[i] & VH_OP_ID_BOTH)
			{
				process_id_flag(ed, 
						&ed->lhs, &ed->rhs, 
						idx_flags[i], argp);
			}
			else
			{
				process_id_flag(ed, 
						&ed->lhs, 0, 
						idx_flags[i], argp);
			}

			break;

		case OP_DT_RHS_MASK:

			process_dt_flag(ed, 
					&ed->rhs, 0, 
					&ed->ope.rhs, 0,
					idx_flags[i], argp);

			ed->rhs.dt_flags = idx_flags[i];

			/*
			 * If the LHS was from a HeapTuple origin and the now the RHS is a 
			 * HeapTupleo origin, set the flag.
			 */
			if (lhs_is_ht_origin &&
				DT_IS_HEAPTUPLE(idx_flags[i]))
				ed->lhs_rhs_ht_origin = true;

			break;

		case OP_ID_RHS_MASK:

			process_id_flag(ed, 
					&ed->rhs, 0, 
					idx_flags[i], argp);
			break;
		}	
	}

	/*
	 * Do our type comparisons now so we don't have to do them later.
	 */

	if (ret_dt)
	{
		if (!(ret_dt & VH_OP_DT_BOTH))
		{
			switch (ret_dt)
			{
				case VH_OP_DT_VAR:
				case VH_OP_DT_TYSVAR:
					ed->ret.ty_depth = vh_type_stack_copy(&ed->ret.tys[0], &ed->lhs.tys[0]);
					ed->ret.begin = opes_var_begin;
					ed->ret_lhs_tys_match = true;

					break;

				case VH_OP_DT_CHR:
					break;

				case VH_OP_DT_BOO:

					vh_type_stack_push(&ed->ret.tys[0], &vh_type_bool);

					ed->ret.by_val = true;
					ed->ret.ty_depth = 1;
					ed->ope.ret.data = vh_typevar_make_tys(ed->ret.tys);
					break;

				case VH_OP_DT_I16:
				case VH_OP_DT_U16:

					vh_type_stack_push(&ed->ret.tys[0], &vh_type_int16);
					
					ed->ret.by_val = true;
					ed->ret.ty_depth = 1;
					ed->ope.ret.data = vh_typevar_make_tys(ed->ret.tys);
					break;

				case VH_OP_DT_I32:
				case VH_OP_DT_U32:

					vh_type_stack_push(&ed->ret.tys[0], &vh_type_int32);
					
					ed->ret.by_val = true;
					ed->ret.ty_depth = 1;
					ed->ope.ret.data = vh_typevar_make_tys(ed->ret.tys);
					break;

				case VH_OP_DT_I64:
				case VH_OP_DT_U64:

					vh_type_stack_push(&ed->ret.tys[0], &vh_type_int64);
					
					ed->ret.by_val = true;
					ed->ret.ty_depth = 1;
					ed->ope.ret.data = vh_typevar_make_tys(ed->ret.tys);
					break;
			}
		}

		/*
		 * Compares the Return Type Stack to the LHS Type Stack
		 */
		ed->ret_lhs_tys_match = vh_type_stack_match(&ed->ret.tys[0],
													&ed->lhs.tys[0]);		
	}

	if (lhs_dt && rhs_dt)
	{
		/*
		 * Compares the LHS Type Stack to the RHS Type Stack
		 */
		ed->lhs_rhs_tys_match = vh_type_stack_match(&ed->lhs.tys[0],
													&ed->rhs.tys[0]);
	}

	return false;
}

/*
 * process_flags_fp
 *
 * This is the fast path route for process flags.  We simply swap out the
 * data pointer on the LHS and RHS values.  We come thru the variadic function
 * so we can get the right sizing.
 */
static bool 
process_flags_fp(TypeVarOpExec ed, va_list argp)
{
	int32_t ret_dt, lhs_dt, rhs_dt, 
		arg_count = 0, 
		i = 0, 
		idx[5], idx_flags[5];

	ret_dt = ed->ret.dt_flags;
	lhs_dt = ed->lhs.dt_flags;
	rhs_dt = ed->rhs.dt_flags;

	/*
	 * Calculate the number of arguments we have and set up some slots
	 * so that we're in the VAARGS loop we know what we're dealing with.
	 */
	if (ret_dt)
	{
		if (ret_dt & VH_OP_DT_BOTH)
		{
			arg_count++;
			idx[i] = OP_DT_RET_MASK;
			idx_flags[i] = ret_dt;
			i++;
		}
	}

	if (lhs_dt)
	{
		arg_count++;
		idx[i] = OP_DT_LHS_MASK;
		idx_flags[i] = lhs_dt;
		i++;
	}

	if (rhs_dt)
	{
		arg_count++;
		idx[i] = OP_DT_RHS_MASK;
		idx_flags[i] = rhs_dt;
		i++;
	}

	/*
	 * We with hold the return type and process it last, we need the context,
	 * specifically the LHS type stack, before we produce a return, if one
	 * is needed.
	 */
	for (i = 0; i < arg_count; i++)
	{
		switch (idx[i])
		{
		case OP_DT_RET_MASK:

			process_dt_flag_fp(ed, &ed->ret, 0,
							   &ed->ope.ret, 0,
							   idx_flags[i], argp);

			break;

		case OP_DT_LHS_MASK:

			if (idx_flags[i] & VH_OP_DT_BOTH)
			{
				process_dt_flag_fp(ed,
								   &ed->lhs, &ed->rhs,
		   						   &ed->ope.lhs, &ed->ope.rhs,	
			   					   idx_flags[i], argp);
			}
			else
			{
				process_dt_flag_fp(ed,
								   &ed->lhs, 0, 
								   &ed->ope.lhs, 0,
								   idx_flags[i], argp);
			}

			break;

		case OP_DT_RHS_MASK:

			process_dt_flag_fp(ed, 
							   &ed->rhs, 0, 
			   				   &ed->ope.rhs, 0,
		   					   idx_flags[i], argp);

			break;
		}	
	}

	/*
	 * We need to check to see if either side picked a formatter.  If so, then
	 * we should setup our formatters.
	 */

	if (ed->lhs.formatters[0] || ed->rhs.formatters[0]) 
	{
		process_format_patterns(ed);
	}

	return true;
}


static bool 
process_dt_flag(TypeVarOpExec exec,
				OpExecSide side,
				OpExecSide side2,
				OpEntrySide entry,
				OpEntrySide entry2,
				int32_t flag,
				va_list argp)
{
	int32_t flag_underlying = flag & ~VH_OP_DT_MASK, list_size, i;
	void *lf = 0;
	HeapTuple ht;
	HeapTuplePtr htp;
	HeapTupleDef htd;
	TypeTag *tags;
	TypeVarSlot *tvs;
	Type *tys;
	char *cstr;
	String str;
	
	union
	{
		bool boo;
		int16_t i16;
		int32_t i32;
		int64_t i64;
		double dbl;
		float flt;
	} byval;

	/*
	 * If we're an SList go ahead and extract the first entry in the list
	 * as |lf| (list first).  In our switch below on the data types we
	 * check lf for a non-null value to process with or we extract from
	 * va_list.
	 */
	if (flag & VH_OP_DT_SLIST)
	{
		side->list = va_arg(argp, SList);
		side->list_size = list_size = vh_SListSize(side->list);
		side->entry_cursor = 0;
	
		if (list_size)
		{
			lf = vh_SListFirst(side->list);
		}

		if (side2)
		{
			side2->list = side->list;
			side2->list_size = side->list_size;
			side2->entry_cursor = 0;
		}
	}

	switch (flag_underlying)
	{
	case VH_OP_DT_HTP:
	case VH_OP_DT_HTR:

		/*
		 * Go ahead and pull out the HeapTupleDef from the pointer so
		 * we can be ready for an ID flag.  check_flags should have already
		 * made sure that we've got the appropriate number of ID flags.
		 *
		 * We cannot set the type stack yet.  We have to wait on the identifier
		 * to do that properly.
		 *
		 * We should go ahead and set the locker functions.
		 */

		if (lf)
			htp = *((HeapTuplePtr*)lf);
		else
		{
			htp = va_arg(argp, HeapTuplePtr);
		}

		if (htp)
		{
			ht = vh_htp_immutable(htp);

			if (ht)
			{
				htd = ht->htd;
				side->htd = htd;
				side->by_val = false;
				side->ht_offset = htd->tupoffset;
				entry->htp = htp;	
			}

			if (flag_underlying & VH_OP_DT_HTP)
			{
				side->begin = opes_htp_begin;
				side->end = opes_htp_end;
			}
			else if (flag_underlying & VH_OP_DT_HTR)
			{
				side->begin = opes_htpr_begin;
				side->end = opes_htpr_end;
			}

			if (side2)
			{
				side2->begin = side->begin;
				side2->end = side->end;
				side2->htd = side->htd;
				side2->by_val = side->by_val;

				entry->htp = htp;
			}
		}

		break;

	case VH_OP_DT_HTM:

		if (lf)
			ht = (HeapTuple)lf;
		else
		{
			ht = va_arg(argp, HeapTuple);
		}

		if (ht)
		{
			side->htd = ht->htd;
			side->by_val = false;
			side->ht_offset = ht->htd->tupoffset;
			entry->ht = ht;

			side->begin = opes_ht_begin;
			
			if (side2)
			{
				side2->begin = side->begin;
				side2->by_val = side->by_val;
				side2->ht_offset = side->ht_offset;
				entry->ht = ht;

				side2->begin = side->begin;
			}
		}

		break;

	case VH_OP_DT_VAR:

		if (lf)
			tags = lf;
		else
		{	
			tags = va_arg(argp, TypeTag*);
		}

		if (tags)
		{
			entry->data = tags;
			entry->null = false;

			if (!side->ty_depth)
			{
				tags -= 1;
				side->ty_depth = fill_tys_from_tags_rev(&side->tys[0], tags);
				side->by_val = false;

				if (side2)
				{
					for (i = 0; i <= side->ty_depth; i++)
						side2->tys[i] = side->tys[i];

					side2->ty_depth = side->ty_depth;
					side2->by_val = side->by_val;

					entry2->data = entry->data;
					entry2->null = entry->null;
				}	
			}
		}

		break;

	case VH_OP_DT_TVS:
		
		if (lf)
			tvs = lf;
		else
			tvs = va_arg(argp, TypeVarSlot*);

		if (tvs)
		{
			entry->data = vh_tvs_value(tvs);
			entry->null = vh_tvs_isnull(tvs);

			if (!side->ty_depth)
			{
				side->ty_depth = vh_type_stack_fromtags(side->tys, tvs->tags);
				side->by_val = false;

				if (side2)
				{
					side2->ty_depth = vh_type_stack_copy(side2->tys, side->tys);
					side2->by_val = side->by_val;

					entry2->data = entry->data;
					entry2->null = entry->null;
				}
			}
		}

		break;

	case VH_OP_DT_CHR:

		if (lf)
		{
			cstr = lf;
		}
		else
		{
			cstr = va_arg(argp, char*);
		}

		if (cstr)
		{
			entry->data = cstr;
			entry->strlen = strlen(cstr);

			/*
			 * It's impossible for us to have a side 2 here.  Go ahead and
			 * assert it.
			 */

			assert(!side2);
		}

		break;

	case VH_OP_DT_STR:

		/*
		 * We should create a begin function but not an end function to
		 * set the proper data point.
		 */

		if (lf)
		{
			str = lf;
		}
		else
		{
			str = va_arg(argp, String);
		}

		if (str)
		{
			side->tys[0] = &vh_type_String;
			side->tys[1] = 0;
			side->ty_depth = 1;
			side->by_val = false;
			side->begin = opes_string_begin;

			entry->str = str;

			if (side2)
			{
				side2->tys[0] = side->tys[0];
				side2->tys[1] = side->tys[1];
				side2->ty_depth = side->ty_depth;
				side2->by_val = side->by_val;
				side2->begin = side->begin;

				entry2->str = str;
			}
		}

		break;

	case VH_OP_DT_BOO:

		if (lf)
			byval.boo = (bool)((uintptr_t)lf);
		else
			/* Keep the compiler quite, minimum VA_ARGS is 4 bytes */
			byval.i32 = va_arg(argp, int32_t);

		side->tys[0] = &vh_type_bool;
		side->tys[1] = 0;
		side->ty_depth = 1;
		side->by_val = true;

		entry->data = (void*)((uintptr_t)byval.boo);
		entry->null = false;

		break;

	case VH_OP_DT_I16:
	case VH_OP_DT_U16:

		if (lf)
			byval.i16 = (int16_t)((uintptr_t)lf);
		else
			byval.i32 = va_arg(argp, int32_t);

		side->tys[0] = &vh_type_int16;
		side->tys[1] = 0;
		side->ty_depth = 1;
		side->by_val = true;

		entry->i16 = byval.i16;
		entry->null = false;

		break;

	case VH_OP_DT_I32:
	case VH_OP_DT_U32:

		if (lf)
			byval.i32 = (int32_t)((uintptr_t)lf);
		else
			byval.i32 = va_arg(argp, int32_t);

		side->tys[0] = &vh_type_int32;
		side->tys[1] = 0;
		side->ty_depth = 1;
		side->by_val = true;

		entry->i32 = byval.i32;
		entry->null = false;

		break;

	case VH_OP_DT_I64:
	case VH_OP_DT_U64:

		if (lf)
#if VHB_SIZEOF_VOID == 8
			byval.i64 = (int64_t)lf;
#else
			byval.i64 = *((int64_t*)lf);
#endif
		else
			byval.i64 = va_arg(argp, int64_t);

		side->tys[0] = &vh_type_int64;
		side->tys[1] = 0;
		side->ty_depth = 1;
#if VHB_SIZEOF_VOID == 8		
		entry->i64 = byval.i64;
		side->by_val = true;
#else
		entry->i64 = byval.i64;
		side->by_val = false;
#endif
		entry->null = false;

		break;

	case VH_OP_DT_TYSVAR:

		tys = va_arg(argp, Type*);

		side->ty_depth = vh_type_stack_copy(side->tys, tys);
		side->dt_flags = VH_OP_DT_VAR;

		if (flag & VH_OP_DT_BOTH)
			side->dt_flags |= VH_OP_DT_BOTH;

		break;
	}

	return false;
}


/* 
 * process_dt_flag_fp
 *
 * Fast path to set the data pointer.  Unlike with process_dt_flag we've
 * already got all the offsets we need to access a HeapTuple.  So we'll take
 * the lock on it and go ahead and set the side->data and side2->data pointers
 * where applicable.
 *
 * The mainline function will only have to call the end function pointer to
 * release the HeapTuple lock.
 */
static bool 
process_dt_flag_fp(TypeVarOpExec exec,
				   OpExecSide side,
	   			   OpExecSide side2,
	   			   OpEntrySide entry,
	   			   OpEntrySide entry2,
	   			   int32_t flag,
	   			   va_list argp)
{
	int32_t flag_underlying = flag & ~VH_OP_DT_MASK;
	void *lf = 0;
	HeapTuple ht;
	HeapTuplePtr htp;
	TypeTag *tags;
	TypeVarSlot *tvs;
	char *cstr;
	String str;
	
	union
	{
		bool boo;
		int16_t i16;
		int32_t i32;
		int64_t i64;
		double dbl;
		float flt;
	} byval;

	if (flag & VH_OP_DT_SLIST)
	{
		/*
		 * Pull the |lf| off the cursor.
		 */
	}

	switch (flag_underlying)
	{
	case VH_OP_DT_HTP:
	case VH_OP_DT_HTR:

		/*
		 * Go ahead and pull out the HeapTupleDef from the pointer so
		 * we can be ready for an ID flag.  check_flags should have already
		 * made sure that we've got the appropriate number of ID flags.
		 *
		 * We cannot set the type stack yet.  We have to wait on the identifier
		 * to do that properly.
		 *
		 * We should go ahead and set the locker functions.
		 */

		if (lf)
			htp = *((HeapTuplePtr*)lf);
		else
		{
			htp = va_arg(argp, HeapTuplePtr);
		}

		if (htp)
		{
			if (flag_underlying == VH_OP_DT_HTR)
				ht = vh_htp_immutable(htp);
			else
				ht = vh_htp(htp);

			if (ht)
			{
				entry->data = ((unsigned char*)ht) + side->ht_offset;
				entry->null = (ht->flags[side->ht_null + 1]) & VH_HTF_FLAG_NULL;
				entry->htp = htp;	

				if (side2 && entry2)
				{
					entry2->data = ((unsigned char*)ht) + side->ht_offset;
					entry2->null = (ht->flags[side2->ht_null + 1]) & VH_HTF_FLAG_NULL;
				}
			}
		}

		break;

	case VH_OP_DT_HTM:

		if (lf)
			ht = (HeapTuple)lf;
		else
		{
			ht = va_arg(argp, HeapTuple);
		}

		if (ht)
		{
			entry->data = ((unsigned char*)ht) + side->ht_offset;
			entry->null = (ht->flags[side->ht_null + 1]) & VH_HTF_FLAG_NULL;
			entry->ht = ht;

			if (side2 && entry2)
			{
				entry2->data = entry->data;
				entry2->null = entry->null;
			}
		}

		break;

	case VH_OP_DT_VAR:
	case VH_OP_DT_TYSVAR:

		if (lf)
			tags = lf;
		else
		{	
			tags = va_arg(argp, TypeTag*);
		}

		if (tags)
		{
			entry->data = tags;
			entry->null = false;

			if (side2)
			{
				entry2->data = entry->data;
				entry2->null = entry->null;
			}
		}

		break;

	case VH_OP_DT_TVS:

		if (lf)
			tvs = lf;
		else
			tvs = va_arg(argp, TypeVarSlot*);

		if (tvs)
		{
			entry->data = vh_tvs_value(tvs);
			entry->null = vh_tvs_isnull(tvs);

			if (side2)
			{
				entry2->data = entry->data;
				entry2->null = entry->null;
			}
		}

		break;

	case VH_OP_DT_CHR:

		if (lf)
			cstr = lf;
		else
			cstr = va_arg(argp, char*);

		if (cstr)
		{
			entry->data = cstr;
			entry->strlen = strlen(cstr);
			entry->null = false;

			/*
			 * Impossible for us to have a side 2 here, so assert that.
			 */

			assert(!side2);
		}
		else
		{
			entry->null = true;
		}

		break;

	case VH_OP_DT_STR:

		/*
		 * We should create a begin function but not an end function to
		 * set the proper data point.
		 */

		if (lf)
			str = lf;
		else
			str = va_arg(argp, String);

		if (str)
		{
			entry->str = str;
			entry->null = false;

			if (side2)
			{
				entry2->str = str;
				entry2->null = false;
			}
		}

		break;
	
	case VH_OP_DT_BOO:

		if (lf)
			byval.boo = (bool)((uintptr_t)lf);
		else
			byval.i32 = va_arg(argp, int32_t);

		/*
		side->tys[0] = &vh_type_bool;
		side->tys[1] = 0;
		side->ty_depth = 1;
		side->by_val = true;
		*/

		entry->data = (void*)((uintptr_t)byval.boo);
		entry->null = false;

		break;

	case VH_OP_DT_I16:
	case VH_OP_DT_U16:

		if (lf)
			byval.i16 = (int16_t)((uintptr_t)lf);
		else
			byval.i32 = va_arg(argp, int32_t);

		/*
		side->tys[0] = &vh_type_int16;
		side->tys[1] = 0;
		side->ty_depth = 1;
		side->by_val = true;
		*/

		entry->i16 = byval.i16;
		entry->null = false;

		break;

	case VH_OP_DT_I32:
	case VH_OP_DT_U32:

		if (lf)
			byval.i32 = (int32_t)((uintptr_t)lf);
		else
			byval.i32 = va_arg(argp, int32_t);

		/*
		side->tys[0] = &vh_type_int32;
		side->tys[1] = 0;
		side->ty_depth = 1;
		side->by_val = true;
		*/

		entry->i32 = byval.i32;
		entry->null = false;

		break;

	case VH_OP_DT_I64:
	case VH_OP_DT_U64:

		if (lf)
#if VHB_SIZEOF_VOID == 8
			byval.i64 = (int64_t)lf;
#else
			byval.i64 = *((int64_t*)lf);
#endif
		else
			byval.i64 = va_arg(argp, int64_t);

		/*
		side->tys[0] = &vh_type_int64;
		side->tys[1] = 0;
		side->ty_depth = 1;
		side->by_val = true;
		*/
#if VHB_SIZEOF_VOID == 8
		entry->i64 = byval.i64;
#else
		entry->i64 = byval.i64;
#endif

		entry->null = false;

		break;
	}

	return true;
}

/*
 * Work our ID flags.  We're either going to get a field name (const char*), HeapField
 * or a HeapField index.
 */
static bool 
process_id_flag(TypeVarOpExec exec,
		OpExecSide side,
		OpExecSide side2,
		int32_t flag,
		va_list argp)
{
	int32_t flag_underlying = flag & ~VH_OP_DT_MASK;
	const char *fname;
	TableDefVer tdv;
	TableField tf;
	HeapField hf;
	char *pattern;
	uint32_t fi;
	
	switch (flag_underlying)
	{
	case VH_OP_ID_NM:

		fname = va_arg(argp, const char*);

		if (fname)
		{
			tdv = (TableDefVer)side->htd;

			if (!tdv)
				return false;

			tf = vh_tdv_tf_name(tdv, fname);

			if (tf)
			{
				side->ht_offset += tf->heap.offset;
				side->ht_null = tf->heap.dord;
				side->ty_depth = vh_type_stack_copy(&side->tys[0], &tf->heap.types[0]);

				if (side2)
				{
					side2->ht_offset = side->ht_offset;
					side2->ht_null = side->ht_null;
					side2->ty_depth = side->ty_depth;
				
					side->ty_depth = vh_type_stack_copy(&side2->tys[0], &tf->heap.types[0]);
				}

				return true;
			}
		}

		break;

	case VH_OP_ID_HF:

		hf = va_arg(argp, HeapField);

		if (hf)
		{
			side->ht_offset += hf->offset;
			side->ht_null = hf->dord;
			side->ty_depth = vh_type_stack_copy(&side->tys[0], &hf->types[0]);

			if (side2)
			{
				side2->ht_offset = side->ht_offset;
				side2->ht_null = side->ht_null;
				side2->ty_depth = vh_type_stack_copy(&side->tys[0], &hf->types[0]);
			}

			return true;
		}
		
		break;

	case VH_OP_ID_FI:

		fi = va_arg(argp, uint32_t);
		hf = vh_htd_field_by_idx(side->htd, (uint16_t)fi);

		if (hf)
		{
			side->ht_offset += hf->offset;
			side->ht_null = hf->dord;
			side->ty_depth = vh_type_stack_copy(&side->tys[0], &hf->types[0]);

			if (side2)
			{
				side2->ht_offset = side->ht_offset;
				side2->ht_null = side->ht_null;
				side2->ty_depth = vh_type_stack_copy(&side->tys[0], &hf->types[0]);
			}

			return true;
		}

		break;

	case VH_OP_ID_FMTSTR:

		/*
		 * This is a special case for c string operations to allow a formatter
		 * to be passed in to the TypeVar.  It's intended for this to be most
		 * used when assigning a CStr to a value, or extracting a value in
		 * a specific format.
		 *
		 * What we do here is just extract the pattern.  Since we need both a
		 * LHS and RHS context, we have to wait.
		 */

		pattern = va_arg(argp, char*);
		side->formatters[0] = pattern;

		return true;

		break;
		
	}

	return false;
}

/*
 * process_format_patterns
 *
 * Somewhere along the way we managed to get a format pattern that we need
 * to grab real formatters for.  We get to invert the sides, so we work
 * the type stack that doesn't have a formatter.  If both sides have a
 * pattern, we're in trouble.
 *
 * NOTE: This works in the event we're assigning to a value from a CStr.
 * But what if we're going from a value and assigning to a CStr (i.e.
 * vh_tam_cstr_get)?  Should work.
 */

static void
process_format_patterns(TypeVarOpExec ed)
{
	OpExecSide side_tys, side_fmts;
	const char *pattern;
	int8_t i, patterncount = 0, d;

	if (ed->rhs.formatters[0])
	{
		side_fmts = &ed->rhs;
		side_tys = &ed->lhs;
	}
	else if (ed->lhs.formatters[0])
	{
		side_fmts = &ed->lhs;
		side_tys = &ed->rhs;
	}

	for (patterncount = 0; ; patterncount++)
	{
		if (!side_fmts->formatters[patterncount])
			break;
	}

	if (side_fmts->ty_depth > patterncount)
	{
		/*
		 * We only got one pattern but here was more than one Type on the other
		 * side.  We'll push the pattern to the lowest type, only on the side with
		 * the types.
		 *
		 * The side known as side_fmts really has patterns in it's formatters slots.
		 * 
		 * We should clear these out so that our cleanup routines don't try to
		 * shove that down into the formatter destructor.
		 */

		d = side_tys->ty_depth - 1;
		pattern = side_fmts->formatters[0];
		side_fmts->formatters[0] = 0;

		side_tys->formatters[d] = vh_tam_cstr_format(side_tys->tys[d],
													 pattern,
													 0,
													 0);
	}
	else
	{
		assert(patterncount == side_tys->ty_depth);

		for (i = 0; i < side_tys->ty_depth; i++)
		{
			side_tys->formatters[i] = vh_tam_cstr_format(side_tys->tys[i],
				   										 side_fmts->formatters[0],
													 	 0,
														 0);

		  	/*
			 * Same as above, clean out the pattern that we store in a formatter
			 * slot so that the format destructor doesn't get triggered with
			 * a pattern.  Could be rude, especially if the formatter stores
			 * its own structure.
			 */	
			side_fmts->formatters[i] = 0;
		}

	}

	side_tys->has_formatters = true;
}

/*
 * cleanup_formatters
 *
 * Check to see if we have any formatters that need to be cleaned up and then
 * do it.
 */
static void 
cleanup_formatters(TypeVarOpExec ed)
{
	if (ed->ret.has_formatters)
		cleanup_formatter(ed, &ed->ret);

	if (ed->lhs.has_formatters)
		cleanup_formatter(ed, &ed->lhs);

	if (ed->rhs.has_formatters)
		cleanup_formatter(ed, &ed->rhs);
}

/*
 * cleaup_formatter
 *
 * Traverse the entire VH_TAMS_MAX_DEPTH of formatters and call the destuctors.
 */
static void 
cleanup_formatter(TypeVarOpExec exec, OpExecSide fmts)
{
	int8_t i;

	for (i = 0; i < fmts->ty_depth; i++)
	{
		if (fmts->formatters[i])
			vh_tam_cstr_format_destroy(fmts->tys[i], fmts->formatters[i]);
	}
}

/*
 * opes_get_combiner
 *
 * Parses a sides flags and fills a combiner up, from RET -> LHS -> RHS.
 *
 * This needs to be a lot smarted about handling the both scenario, where
 * source is only going to sit on one side (combine.data->ht/htp) but the
 * data needs to be distributed to both sides.  We can do this with the
 * sides bits we have available now.  This was the actual opes_ functions
 * don't have to be very smart at all, they can just iterate the array.
 */
static int8_t 
opes_get_combiner(TypeVarOpExec exec,
				  TypeVarOpEntry data,
				  uint8_t sides,
				  OpExecCombiner combine)
{
	int8_t count = 0;

	if (sides & OP_RET)
	{
		combine[count].data = &data->ret;
		combine[count].exec = &exec->ret;
		count++;
	}

	if (sides & OP_LHS)
	{
		combine[count].data = &data->lhs;
		combine[count].exec = &exec->lhs;
		count++;
	}

	if (sides & OP_RHS)
	{
		combine[count].data = &data->rhs;
		combine[count].exec = &exec->rhs;
		count++;
	}

	return count;
}

static void 
opes_string_begin(TypeVarOpExec exec, TypeVarOpEntry data, uint8_t sides)
{
	OpExecCombinerData combiner[OP_MAX];
	int8_t count, i;

	count = opes_get_combiner(exec, data, sides, combiner);

	for (i = 0; i < count; i++)
	{
		combiner[i].data->data = vh_str_buffer(combiner[i].data->str);
	}
}


static void 
opes_ht_begin(TypeVarOpExec exec, TypeVarOpEntry data, uint8_t sides)
{

	OpExecCombinerData combiner[OP_MAX];
	char *cursor;;
	int8_t count, i;

	count = opes_get_combiner(exec, data, sides, combiner);

	for (i = 0; i < count; i++)
	{
		cursor = (char*)combiner[i].data->ht;
		combiner[i].data->data = cursor + combiner[i].exec->ht_offset;
	}
}

static void 
opes_htp_begin(TypeVarOpExec exec, TypeVarOpEntry data, uint8_t sides)
{
	OpExecCombinerData combiner[OP_MAX];
	HeapTuple ht;
	unsigned char *cursor;
	int8_t count, i;

	count = opes_get_combiner(exec, data, sides, combiner);

	for (i = 0; i < count; i++)
	{
		ht = vh_htp(combiner[i].data->htp);
		cursor = (unsigned char*)ht;
		cursor += combiner[i].exec->ht_offset;

		combiner[i].data->data = cursor;	
	}
}

/*
 * We use this to create new var for a Return Value.
 */
static void 
opes_var_begin(TypeVarOpExec exec, TypeVarOpEntry data, uint8_t sides)
{
	OpExecCombinerData combiner[OP_MAX];
	int8_t count, i;

	count = opes_get_combiner(exec, data, sides, combiner);

	for (i = 0; i < count; i++)
	{
		combiner[i].data->data = vh_typevar_make_tys(combiner[i].exec->tys);
	}
}

static void 
opes_htp_end(TypeVarOpExec exec, TypeVarOpEntry data, uint8_t sides)
{
}

static void 
opes_htpr_begin(TypeVarOpExec exec, TypeVarOpEntry data, uint8_t sides)
{
}

static void 
opes_htpr_end(TypeVarOpExec exec, TypeVarOpEntry data, uint8_t sides)
{
}

static bool
comp_lookup_funcs(TypeVarOpExec ed)
{
	int32_t i;
	bool funcs_set = false;

	switch (ed->opi)
	{
	case VH_COMP_LT:
	case VH_COMP_LTEQ:
	case VH_COMP_EQ:
	case VH_COMP_NEQ:
	case VH_COMP_GT:
	case VH_COMP_GTEQ:

		/*
		 * Run thru our type stacks for comparison functions that we
		 * can use.  Generally we sould only attempt a type commute at
		 * the last place in the stack.
		 */
		if (ed->lhs.ty_depth == ed->rhs.ty_depth)
		{
			for (i = 0; i < ed->lhs.ty_depth; i++)
			{
				if (ed->lhs.tys[i] ==
				    ed->rhs.tys[i])
				{
					ed->lr_func_meth = OP_FUNC_COMP;
					ed->lr_func[i].comp = ed->lhs.tys[i]->tom.comp;
				}
				else
				{
					ed->lr_func_meth = OP_FUNC_OPER;
					ed->lr_func[i].oper = vh_type_oper(ed->lhs.tys[i],
													ed->op,
													ed->rhs.tys[i],
													0);
				}
			}

			funcs_set = true;
		}
		
		break;

	default:

		if (ed->op == 0)
		{
			if (ed->lhs.ty_depth == ed->rhs.ty_depth)
			{
				funcs_set = true;
				for (i = 0; i < ed->lhs.ty_depth; i++)
				{
					if (ed->lhs.tys[i] ==
						ed->rhs.tys[i])
					{
						ed->lr_func_meth = OP_FUNC_COMP;
						ed->lr_func[i].comp = ed->lhs.tys[i]->tom.comp;
					}
					else
					{
						funcs_set = false;
						break;
					}
				}
			}
		}
		else
		{
			/*
			 * We've got a complicated comparison operator we need to lookup using
			 * the operator function table, rather than just a basic comparison.
			 */
		}

		break;
	}

	return funcs_set;
}

/*
 * op_lookup_funcs
 *
 * This is a whole lot more complex than our regular comparison operators,
 * because of the return type.  We set a flag called unary if the action is
 * expected to update the LHS data point (i.e. ++, -- ) and no RHS data point
 * exists.
 *
 * The binary_self flag will be set if the LHS is to be updated as the return.
 */
static bool
op_lookup_funcs(TypeVarOpExec ed)
{
	vh_tom_oper foper;
	const char *binary_self_op;
	int32_t i;
	bool unary = false, binary_self = false, assignment = false,
		 func_stack_set = true;

	switch (ed->opi)
	{
		case VH_OP_PLPL:
		case VH_OP_SUSU:

			unary = true;
			break;

		case VH_OP_PLEQ:
			binary_self = true;
			binary_self_op = "+";
			break;

		case VH_OP_SUEQ:
			binary_self = true;
			binary_self_op = "-";
			break;

		case VH_OP_MULTEQ:
			binary_self = true;
			binary_self_op = "*";
			break;

		case VH_OP_DIVEQ:
			binary_self = true;
			binary_self_op = "/";
			break;

		case VH_OP_EQ:

			assignment = true;
			break;	
	}

	if (unary)
	{
		/*
		 * Check if there's a custom unary operator, otherwise we'll just
		 * make a TypeVar for the RHS and assign the value to 1.
		 */

		ed->lr_func_meth = OP_FUNC_OPER;

		assert(ed->lhs.ty_depth);

		for (i = 0; i < ed->lhs.ty_depth; i++)
		{
			foper = vh_type_oper(ed->lhs.tys[i], ed->op, ed->lhs.tys[i], 0);

			if (foper)
			{
				ed->lr_func[i].oper = foper;
			}
			else
			{
				func_stack_set = false;
				break;
			}
		}

		if (!func_stack_set)
		{
			func_stack_set = true;

			for (i = 0; i < ed->lhs.ty_depth; i++)
			{
				foper = vh_type_oper(ed->lhs.tys[i],
									 ed->opi == VH_OP_PLPL ? "+" : "-",
			 						 ed->lhs.tys[i], 0);

				if (foper)
				{
					ed->lr_func[i].oper = foper;
				}
				else
				{
					func_stack_set = false;
					break;
				}
			}

			if (func_stack_set)
			{
				ed->ope.rhs.i16 = 1;
				ed->rhs.by_val = true;

				return true;
			}
		}
	}

	if (assignment)
	{
		/*
		 * If our RHS and LHS type stacks match, then we can just do
		 * the 'ole memcpy TAM intead of an operator lookup.
		 */

		if (ed->lhs_rhs_tys_match)
		{
			ed->lr_func_meth = OP_FUNC_TAM_GEN;

			assert(ed->lhs.ty_depth);

			for (i = 0; i < ed->lhs.ty_depth; i++)
			{
				ed->lr_func[i].tam_gen = ed->lhs.tys[i]->tam.memset_set;

				if (!ed->lr_func[i].tam_gen)
					func_stack_set = false;
			}

			return func_stack_set;
		}
		else if (ed->rhs.dt_flags == VH_OP_DT_CHR ||
				 ed->rhs.dt_flags == VH_OP_DT_STR)
		{
			/*
			 * We're assigning a CStr (or a String) to the LHS.  There's
			 * no special care required when reading from a String which
			 * is what we're doing here.
			 *
			 * Use vh_tam_cstr_set
			 */
			ed->lr_func_meth = OP_FUNC_CSTR_SET;

			for (i = 0; i < ed->lhs.ty_depth; i++)
			{
				ed->lr_func[i].tam_cstr_set = ed->lhs.tys[i]->tam.cstr_set;

				if (!ed->lr_func[i].tam_cstr_set)
					func_stack_set = false;
			}

			return func_stack_set;
		}
		else if (ed->lhs.dt_flags == VH_OP_DT_STR)
		{
			/*
			 * We're assigning the value to a String.
			 *
			 * Use vh_tam_cstr_get, but since we're going to a string target
			 * we won't call it with a malloc, but instead let the string handle
			 * it's own expansion routines.  To do this, we just put a slightly
			 * different stub in op_exec to handle the String and it's buffering
			 * techniques.
			 */

			ed->lr_func_meth = OP_FUNC_STR_GET;

			assert(ed->rhs.ty_depth);

			for (i = 0; i < ed->rhs.ty_depth; i++)
			{
				ed->lr_func[i].tam_cstr_get = ed->rhs.tys[i]->tam.cstr_get;

				if (!ed->lr_func[i].tam_cstr_get)
					func_stack_set = false;
			}

			/*
			 * Clear the beginner function, we don't need it.
			 */
			ed->lhs.begin = 0;

			return func_stack_set;
		}
		else
		{
			/*
			 * If we still can't get this to work, we can setup the RHS to
			 * emit a string using vh_tam_cstr_get and then have the LHS
			 * consume the string using vh_tam_cstr_set.  That's nasty but
			 * it may be better than nothing at all.
			 *
			 * If the user wants faster performance with less cycles, they
			 * can define their own operator to assign the two types.
			 */
			ed->lr_func_meth = OP_FUNC_OPER;

			assert(ed->lhs.ty_depth);

			if (ed->lhs.ty_depth == ed->rhs.ty_depth)
			{
				for (i = 0; i < ed->lhs.ty_depth; i++)
				{
					ed->lr_func[i].oper = vh_type_oper(ed->lhs.tys[i],
													   ed->op,
													   ed->rhs.tys[i], 0);

					if (!ed->lr_func[i].oper)
					{
						func_stack_set = false;
						break;
					}
				}

				return func_stack_set;
			}
		}
	}

	/*
	 * Do the left hand and right hand action
	 */
	if (ed->lhs.ty_depth == ed->rhs.ty_depth)
	{
		ed->lr_func_meth = OP_FUNC_OPER;

		assert(ed->lhs.ty_depth);

		for (i = 0; i < ed->lhs.ty_depth; i++)
		{
			ed->lr_func[i].oper = vh_type_oper(ed->lhs.tys[i],
											ed->op,
											ed->rhs.tys[i], 0);

			if (!ed->lr_func[i].oper)
				func_stack_set = false;	
		}
	}

	/*
	 * binary_self
	 *
	 * This is useful for the +=, -= *= /= scenarios we may come across.
	 */

	if (!func_stack_set && binary_self)
	{
		ed->lr_func_meth = OP_FUNC_OPER;
		ed->lhs_is_ret = true;

		if (ed->lhs_rhs_tys_match)
		{
			func_stack_set = true;

			assert(ed->lhs.ty_depth);

			for (i = 0; i < ed->lhs.ty_depth; i++)
			{
				ed->lr_func[i].oper = vh_type_oper(ed->lhs.tys[i],
												   binary_self_op,
												   ed->rhs.tys[i], 0);

				if (!ed->lr_func[i].oper)
				{
					func_stack_set = false;
					break;
				}
			}

			return func_stack_set;
		}

		/*
		 * Check to see if we're appending to a string or if we should
		 * try to parse the string on the RHS and then do the action
		 * on the LHS.  These are new function categories for op_exec
		 * but we should implement them.
		 */
	}

	if (ed->lhs.ty_depth && ed->rhs.ty_depth == 0)
	{
		ed->lr_func_meth = OP_FUNC_OPER;

		for (i = 0; i < ed->lhs.ty_depth; i++)
		{
			foper = vh_type_oper(ed->lhs.tys[i], ed->op, 0, 0);

			if (foper)
				ed->lr_func[i].oper = foper;
			else
				ed->lr_func[i].oper = 0;
		}

		func_stack_set = true;
		return true;
	}

	/*
	 * Figure out how to commute the return type.  If the RET and LHS
	 * are the same, there's nothing special to do.
	 */

	if (func_stack_set && ed->ret.tys[0])
	{
		if (ed->ret_lhs_tys_match)
		{
			ed->ret_func_meth = 0;

			return true;
		}

		ed->ret_func_meth = OP_FUNC_OPER;

		for (i = 0; i < ed->lhs.ty_depth; i++)
		{
			foper = vh_type_oper(ed->ret.tys[0], "=", ed->lhs.tys[i], 0);

			if (foper)
				ed->ret_func[i].oper = foper;
			else
				func_stack_set = false;
		}

		return func_stack_set;
	}

	return false;
}

static bool
comp_exec(TypeVarOpExec ed, int32_t *out_res, bool fp)
{
	int32_t comp = 0, oper = 0;
	bool res = false, comp_error = false;
	void *lhs, *rhs;

		if (ed->lhs.begin)
			ed->lhs.begin(ed, &ed->ope, OP_LHS); 

		if (ed->rhs.begin && !ed->lhs_rhs_pin_same)
			ed->rhs.begin(ed, &ed->ope, OP_RHS);

	if (ed->ope.lhs.null || ed->ope.rhs.null)
	{
		/*
		 * Check for nulls, we should ideally find a way to return a null.
		 *
		 * Postgres returns null when either side of a comparison is null.
		 */

		comp = !(ed->ope.lhs.null && ed->ope.rhs.null);
		comp_error = true;
	}
	else
	{
	
		/*
		 * Check our by_val settings for each side.
		 */
		if (ed->lhs.by_val)
			lhs = &ed->ope.lhs.data;
		else
			lhs = ed->ope.lhs.data;

		if (ed->rhs.by_val)
			rhs = &ed->ope.rhs.data;
		else
			rhs = ed->ope.rhs.data;

		VH_TRY();
		{
			/*
			 * Check to see if we should fire the basic comparison operator
			 * (comp_lookup_funcs) will set this.
			 */
			switch (ed->lr_func_meth)
			{
				case OP_FUNC_COMP:
					comp = vh_tom_firea_comp(&ed->lhs.tys[0], 
											 &ed->lr_func[0].comp,
											 lhs, rhs);

					if (out_res)
						*out_res = comp;

					break;

				case OP_FUNC_OPER:

					oper = vh_tom_firea_oper(&ed->lhs.tys[0],
											 &ed->lr_func[0].oper,
											 &ed->rhs.tys[0],
											 lhs, rhs, 0);

					if (oper)
					{
					}

					break;
			}

		}
		VH_CATCH();
		{
			comp_error = true;
		}
		VH_ENDTRY();
	}


	if (ed->rhs.end && !ed->lhs_rhs_pin_same)
		ed->rhs.end(ed, &ed->ope, OP_RHS);

	if (ed->lhs.end)
		ed->lhs.end(ed, &ed->ope, OP_LHS);


	if (comp_error)
	{
	}
	else
	{
		switch (ed->opi)
		{
		case VH_COMP_LT:

			if (comp < 0)
				res = true;

			break;

		case VH_COMP_LTEQ:

			if (comp <= 0)
				res = true;

			break;

		case VH_COMP_EQ:

			if (comp == 0)
				res = true;

			break;

		case VH_COMP_NEQ:

			if (comp != 0)
				res = true;

			break;

		case VH_COMP_GT:

			if (comp > 0)
				res = true;

			break;

		case VH_COMP_GTEQ:

			if (comp >= 0)
				res = true;

			break;

		default:

			res = comp;

			break;
		}
	}

	return res;
}

static int32_t
op_exec(TypeVarOpExec ed, bool fp)
{
	static const struct CStrAMOptionsData cstr_opts = { .malloc = false };

	void *lhs, *rhs, *ret;
	int32_t op_ret, try_count;
	bool op_error = false, null_vals = true;
	String str;
	size_t slen, len, cur, scap, bcur;


	if (ed->ret.begin)
		ed->ret.begin(ed, &ed->ope, OP_RET);

	if (ed->lhs.begin && !ed->ret_lhs_pin_same)
		ed->lhs.begin(ed, &ed->ope, OP_LHS);

	if (ed->rhs.begin && !ed->lhs_rhs_pin_same)
		ed->rhs.begin(ed, &ed->ope, OP_RHS);


	if (ed->ope.lhs.null || (ed->ope.rhs.null && !ed->rhs.tys[0]))
	{
		op_error = true;
		null_vals = true;
	}
	else
	{
		/*
		 * Handle binary assignment (+=, -=, /=, *=) cases here so
		 * that our function call is simple without branching.
		 */
		if (ed->lhs_is_ret)
		{
			if (ed->lhs.by_val)
				ret = &ed->ope.lhs.data;
			else
				ret = ed->ope.lhs.data;
		}
		else
		{
			ret = ed->ope.ret.data;
		}

		if (ed->lhs.by_val)
			lhs = &ed->ope.lhs.data;
		else
			lhs = ed->ope.lhs.data;

		if (ed->rhs.by_val)
			rhs = &ed->ope.rhs.data;
		else
			rhs = ed->ope.rhs.data;

		VH_TRY();
		{
			switch (ed->lr_func_meth)
			{
				case OP_FUNC_OPER:

					op_ret = vh_tom_firea_oper(&ed->lhs.tys[0],			/* LHS Type */
											   &ed->lr_func[0].oper,	/* Functions */
											   &ed->rhs.tys[0],			/* RHS Type */
											   lhs,
											   rhs,
											   ret);

					if (op_ret)
					{
					}

					break;

				case OP_FUNC_TAM_GEN:

					/*
					 * We have to be careful here about copying over the hbno on
					 * varlen data types, especially when the LHS and RHS are not
					 * both from a HeapTuple origin.  process_flags handles determining
					 * when this is true, so all we have to do is follow it here.
					 *
					 * NOTE: We're only using this to assign the RHS to the LHS.
					 */
					vh_tam_firea_memset_set(&ed->lhs.tys[0],			/* Type */
											&ed->lr_func[0].tam_gen,	/* Functions */
											rhs,
											lhs,
											ed->lhs_rhs_ht_origin);
					break;

				case OP_FUNC_CSTR_SET:

					/*
					 * Big mess here to get this to fire.
					 *
					 * NOTE: We're only using this to assign the RHS to the LHS.
					 */
					vh_tam_firea_cstr_set(&ed->lhs.tys[0],				/* Type */
										  &ed->lr_func[0].tam_cstr_set,	/* Functions */
										  &cstr_opts,					/* Options */
										  rhs,							/* Source */
										  lhs,							/* Target */
										  ed->ope.rhs.strlen,			/* Length */
										  0,							/* Cursor */
										  &ed->rhs.formatters[0]);		/* Formatter */

					break;

				case OP_FUNC_STR_GET:
					/*
					 * We're getting the value off a real type on the RHS and setting
					 * it to a String.  The only real issue with this is that the
					 * target String's buffer may not be wide enough to do that.
					 *
					 * So we call the TAM and let it tell us how much space we need
					 * and if we failed to put it all there.  Then we can resize the
					 * target String and try it again.
					 *
					 * We should be careful if we're in an append operation, to change
					 * the starting point of the buffer we push into the TAM.
					 *
					 * We use bcur to handle our append operations, by setting the value
					 * to the length of the existing string value at LHS.  For regular
					 * assigments, we start it at zero.
					 */

					try_count = 0;
					str = ed->ope.lhs.str;
					cur = 0;
					bcur = 0;

					while (try_count < 3)
					{
						slen = vh_strlen(str);
						scap = vh_str_capacity(str);

						len = scap - slen;

						vh_tam_firea_cstr_get(ed->rhs.tys,					/* Type */
											  &ed->lr_func[0].tam_cstr_get,	/* Functions */
											  &cstr_opts,					/* Options */
											  rhs,							/* Source */
											  vh_str_buffer(str) + bcur,	/* Target */
											  &len,							/* Length */
											  &cur,							/* Cursor */
											  &ed->rhs.formatters[0]);

						/*
						 * Move our string size along by the cursor and advance bcur.
						 * There's a chance we wrote some data to our buffer, so we have
						 * to be careful to not reset cur back to zero.  As once we're
						 * in this loop we live with what we got back from the TAM.
						 */
						str->varlen.size = (slen + cur) |
										   (VH_STR_IS_OOL(str) ? VH_STR_FLAG_OOL : 0);
						bcur += cur;

						if (len == cur)
						{
							/*
							 * We're good, get out of this loop.
							 */
							break;
						}

						/*
						 * Resize
						 */

						vh_str.Resize(str, slen + len + 1);
						try_count++;
					}


					break;
			}
		}
		VH_CATCH();
		{
			op_error = false;
		}
		VH_ENDTRY();

	}

	if (ed->rhs.end && !ed->lhs_rhs_pin_same)
		ed->rhs.end(ed, &ed->ope, OP_RHS);

	if (ed->lhs.end && !ed->ret_lhs_pin_same)
		ed->lhs.end(ed, &ed->ope, OP_LHS);

	if (ed->ret.end)
		ed->ret.end(ed, &ed->ope, OP_RET);

	if (op_error)
	{
	}

	if (null_vals)
	{
	}

	return 0;
}

