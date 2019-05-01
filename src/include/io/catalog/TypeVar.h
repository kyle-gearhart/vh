/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_catalog_TypeVar_H
#define vh_io_catalog_TypeVar_H

#include "io/catalog/Type.h"

/*
 * TypeVar
 *
 * TypeVar's allow automatic Type deduction.  We accomplish this by including a
 * 2 byte field (the "Type Tag") which identifies the internal type used by 
 * VH.
 *
 * TypeVar has the ability to allocate a single instance of a complete type or
 * an array of a complete type.
 *
 * A function called vh_typevar_make takes vardic arugments to indicate the 
 * nesting level of the type.  There are three flags on the identifier, two 
 * change the access of the type.  The third, VH_TYPEVAR_MAGIC is nothing more
 * than a magic bit to help make sure someone gave us a valid TypeVar.
 *
 * VH_TYPETAG_ARRAY_FLAG		L'\X80\X00'		First Highest Bit Set
 * VH_TYPETAG_END_FLAG			L'\X40\X00'		Second Highest Bit Set
 * VH_TYPETAG_MAGIC				L'\X20\X00'		Third Highest Bit Set
 *
 * Valid combinations for TypeTag:
 * 		VH_TYPETAG_ARRAY_FLAG && !VH_TYPETAG_END && !VH_TYPETAG_MAGIC
 *		!VH_TYPETAG_ARRAY_FLAG && VH_TYPETAG_END && VH_TYPETAG_MAGIC
 * 		!VH_TYPETAG_ARRAY_FLAG && !VH_TYPETAG_END && VH_TYPETAG_MAGIC
 *
 * This leaves us with 2 ^ 13 or 8,192 - 1 unique individual types.  Zero is
 * considered an invalid type.
 *
 * When the VH_TYPEVAR_ARRAY_FLAG is set, we know the var is in an array and
 * it's type identifying flags are at the start of the array.  The following 
 * 15 bits indicate the number of bytes to walk backward from the address of 
 * the var itself.  We may chain back up to a maximum of 10 steps.  This allows
 * for a maximum allocation of 327,680 bytes of allocation for the array,
 * including its type tags.  The advantage of using an array over a individual
 * makevar is we only generate one type stack for the entire array.
 *
 * Without a VH_TYPEVAR_ARRAY_FLAG, the Type Tag's lower 14 bits are the Type
 * Identifier.  If the VH_TYPEVAR_END_FLAG is set, then we know we have reached
 * the end of the type nesting chain.  The right most Type Identifier is the 
 * inner most type.  As we work the chain going left, we work from inner most to
 * outter most nested type.
 *
 * vh_typevar_make is conscious of alignment.  We must take into account the
 * standard MemoryChunkHeader when called from vhmalloc.  Generally we'll put the
 * padding to get the type itself on the proper boundary between the end of the
 * MemoryChunkHeader and the first Type Tag.
 */


#define VH_TYPETAG_ARRAY_FLAG		0x8000u
#define VH_TYPETAG_END_FLAG		0x4000u
#define VH_TYPETAG_MAGIC 		0x2000u
#define VH_TYPETAG_ID_MASK		0xe000u
 
#define vh_typetag_isarray(tag)		((tag) & (TypeTag)VH_TYPETAG_ARRAY_FLAG)
#define vh_typetag_isend(tag)		((tag) & (TypeTag)VH_TYPETAG_END_FLAG)
 
#define vh_getarg1(n, ...)		(n)
#define vh_getarg2(_1, n, ...)		(n)

#define CppAsString(ident)		#ident


typedef void *TypeVar;


/*
 * TypeVar macros
 */

#define vh_makevar(...)																\
	vh_typevar_make(PP_NARG(__VA_ARGS__), __VA_ARGS__)
#define vh_makevar1(_1)																\
	vh_typevar_make(1, #_1)
#define vh_makevar2(_1, _2)															\
	vh_typevar_make(2, #_1, #_2)
#define vh_makevar3(_1, _2, _3)														\
	vh_typevar_make(3, #_1, #_2, #_3)
#define vh_makevar4(_1, _2, _3, _4)													\
	vh_typevar_make(4, #_1, #_2, #_3, #_4)
#define vh_makevar5(_1, _2, _3, _4, _5)												\
	vh_typevar_make(5, #_1, #_2, #_3, #_4, #_5)
#define vh_makevar6(_1, _2, _3, _4, _5, _6)											\
	vh_typevar_make(6, #_1, #_2, #_3, #_4, #_5, #_6)
#define vh_makevar7(_1, _2, _3, _4, _5, _6, _7)										\
	vh_typevar_make(7, #_1, #_2, #_3, #_4, #_5, #_6, #_7)
#define vh_makevar8(_1, _2, _3, _4, _5, _6, _7, _8)									\
	vh_typevar_make(8, #_1, #_2, #_3, #_4, #_5, #_6, #_7, #_8)

#define vh_makearray(count, ...)													\
		vh_typearray_make((count), PP_NARG(__VA_ARGS__), __VA_ARGS__)
#define vh_makearray1(count, _1)													\
		vh_typearray_make((count), 1, #_1)			
#define vh_makearray2(count, _1, _2)												\
		vh_typearray_make((count), 2, #_1, #_2)
#define vh_makearray3(count, _1, _2, _3)											\
		vh_typearray_make((count), 3, #_1, #_2, #_3)

#define vh_array_idx(array, index)													\
		((vh_getarg1(args...)*) vh_typearray_index((array), (index)))

#define vh_freevar(ptr)					vh_typevar_free((ptr))
#define vh_freearray(ptr)				vh_typearray_free((ptr))

#define vh_returnvar(typevar)													\
		do																		\
		{		 																\
			void *cpy = vh_makecopyvar(vh_stack_parent_mctx, typevar);			\
			vh_stack_unwind_frame();											\
			return cpy;															\
		} while (0);


/*
 * vh_typevar_make
 * 
 * Creates a typevar for the nested stack.  The left most entry should be the
 * outtermost Type in the stack.  These are all allocated on the heap, using
 * the current memory context.
 */
void* vh_typevar_make(int32_t nargs, ...);
void* vh_typevar_make_tys(Type *tys);
void* vh_typevar_make_tvs(TypeVarSlot *tvs);

/*
 * vh_typevar_create
 *
 * More advanced than make, because we can describe a header size and a footer
 * size and alignment.
 * 	tys					type stack, outter most type to inner most type
 * 	tag_count			number of nested types, must be 1 or more
 * 	tag_align_factor	alignment factor for the tags
 * 	header_sz			size of the header in bytes
 * 	header_align		header max align factor
 * 	footer_sz			size of the footer in bytes
 * 	footer_align		footer max align factor
 * 	data_at+			pointer to the actual type
 * 	footer_at+			pointer to the start of the footer
 * 	+	OPTIONAL PARAMETER
 *
 * Returns a pointer to the start of the header.  This allows us to embed TypeVar
 * into other structures.
 *
 * {
 * 		unsigned char header[header_sz];
 * 		TypeTag tags;
 * 		data
 * 		footer_sz
 * }
 * 	
 */

size_t vh_typevar_tys_size(Type *tys, size_t *var_offset);
void* vh_typevar_init(void *at, size_t sz, Type *tys);

void* vh_typevar_create(Type *tys, int32_t tag_count, int32_t *tag_align_factor,
						size_t header_sz, size_t footer_sz, size_t footer_align,
						void **data_at, void **footer_at);

/*
 * vh_typevar Move and Copy Semantics
 */
void* vh_typevar_makecopy(void *typevar);
bool vh_typevar_copy(void *typevar_source, void *target);
bool vh_typevar_move(void *typevar_from, void *typevar_to);

/*
 * vh_typevar Construct/Destruct/Reset Routines
 *
 * _construct: constructs the type
 * _destroy: finalize and free
 * _free: frees the space allocated by the type
 * _finalize: calls the destructor (if any)
 * _reset: essentially does destroy and construct
 *
 */ 
void vh_typevar_construct(void *typevar);
void vh_typevar_destroy(void *typevar);
void vh_typevar_finalize(void *typevar);
void vh_typevar_free(void *typevar);
void vh_typevar_reset(void *typevar);

int8_t vh_typevar_fill_stack(void *typevar, Type *tys);
int8_t vh_typevar_fill_tags(void *typevar, TypeTag *tags);


/*
 * vh_typevar_isa
 *
 * Tests whether the underlying type or type stack represented by the typevar
 * is a particular type.  Helper macros exist for the basic out-of-the-box types
 * supported.
 */
bool vh_typevar_isa(void *typevar, Type ty);
bool vh_typevar_isatys(void *typevar, Type *tys);

#define vh_typevar_isa_String(v)		vh_typevar_isa((v), &vh_type_String)
#define vh_typevar_isa_int8(v)			vh_typevar_isa((v), &vh_type_int8)
#define vh_typevar_isa_int16(v)			vh_typevar_isa((v), &vh_type_int16)
#define vh_typevar_isa_int32(v)			vh_typevar_isa((v), &vh_type_int32)
#define vh_typevar_isa_int64(v)			vh_typevar_isa((v), &vh_type_int64)

/*
 * vh_typearray_make
 *
 * Creates an array |entries| containing typevar of the nested stack.  Like
 * vh_typevar_make, the left most type should be the outter most type in the
 * stack.  By default, typearray does not generate a table of contents to 
 * directly index into members.  A table of contents (array of pointers) can
 * be generated after calling _make.
 */
void* vh_typearray_make(int32_t entries, int32_t nargs, ...);
void** vh_typearray_toc(void *typearray);

/*
 * vh_typearray Operations
 *
 * Standard array operations to push, pop, and resize the array.
 */
int32_t vh_typearray_size(void *typearray);
void* vh_typearray_at(void *typearray, int32_t idx);
void vh_typearray_push(void *typearray, void *typevar, bool move);
void vh_typearray_pop(void *typearray);

typedef bool (*vh_typearray_iterate_cb)(void *typearray,
										Type *tys,
									    int32_t idx,
										void *typevar,
										void *user_data);
bool vh_typearray_iterate(void *typearray, vh_typearray_iterate_cb cb,
						  void *cb_data);



/*
 * vh_typearray Cleanup Routines
 *
 * Per the usual naming convention, _destroy simply deallocates the memory
 * holding the array, typevars, and table of contents if one was requested.  
 * _finalize calls the destructor for each entry in the array.  _free calls 
 * _finalize followed by _destroy.
 */
void vh_typearray_destroy(void *typearray);
void vh_typearray_finalize(void *typearray);
void vh_typearray_free(void *typearray);




/*
 * TypeVar Operators
 *
 * The TypeVar infrastructure allows us to take a TypeVar or a TypeVarPointer
 * and perform an operation.  There are three calls a user may make:
 * 	1) Inplace operator			vh_op
 *	2) Allocation operator		vh_opa
 * 	3) Comparison operator		vh_comp
 *
 * The allocation assignment will use the left hand side typevar as the type to
 * allocate.  Thus if you add a double and an int32_t with vh_opa, the underlying
 * type returned will be a double.
 */



/*
 * Operator Flags
 *
 * Operator Flags allow us to use variadic functions to invoke operators with
 * a variety of data types.  This reduces the need to create hundreds of function
 * definitions to handle all of the variations.
 *
 * A 32-bit operator flag has the following properties (from most significant bit 
 * to least significant bit):
 * 	8 Bit Return Data Type
 * 	8 Bit LHS Data Type
 * 	4 Bit LHS Identifier
 * 	8 Bit RHS Data Type
 * 	4 Bit RHS Identifier
 * =============================
 * 	32 bits
 *
 * Operator Flags for Data Types
 *
 * Data types occupy 8 bits.
 */
#define VH_OP_DT_INVALID		(0x0000u)
#define VH_OP_DT_HTP			(0x0001u)	/* Heap Tuple Pointer (mutable) */
#define VH_OP_DT_HTR			(0x0002u)	/* Heap Tuple Pointer (immutable) */
#define VH_OP_DT_VAR			(0x0003u)	/* TypeVar */
#define VH_OP_DT_TVS			(0x0004u)	/* TypeVarSlot */
#define VH_OP_DT_CHR			(0x0005u)	/* const char */
#define VH_OP_DT_STR			(0x0006u)	/* String */
#define VH_OP_DT_STD			(0x0007u)	/* struct StringData */
#define VH_OP_DT_BOO			(0x0008u)	/* boolean */
#define VH_OP_DT_I16			(0x0009u)	/* int16_t */
#define VH_OP_DT_U16			(0x000au)	/* uint16_t */
#define VH_OP_DT_I32			(0x000bu)	/* int32_t */
#define VH_OP_DT_U32 			(0x000cu)	/* uint32_t */
#define VH_OP_DT_I64			(0x000du)	/* int64_t */
#define VH_OP_DT_U64			(0x000eu)	/* uint64_t */
#define VH_OP_DT_DBL			(0x000fu)	/* double */
#define VH_OP_DT_FLT			(0x0010u)	/* float */

#define VH_OP_DT_HTM			(0x0011u)	/* HeapTuple (mutable) */
#define VH_OP_DT_HTI			(0x0012u)	/* HeapTuple (immutale) */
#define VH_OP_DT_TYSVAR			(0x0013u)	/* TypeStack - TypeVar */

/*
 * Special case for SList, we set the high bit to indicate the data type is 
 * actually housed in an SList.  When its set, we can process in bulk and don't
 * have to do setup and tear down of the operator (i.e. lookups).
 */
#define VH_OP_DT_BOTH			(0x0080u)
#define VH_OP_DT_SLIST			(0x0040u)

#define VH_OP_DT_MASK			(0x00e0u)


/*
 * Operator Flags for Identifiers
 *
 * Identifiers occupy 4 bits.  Only two datatypes require an identifier:
 * HeapTuplePointer (HTP) and HeapTuplePointer read only (HTR).  Otherwise
 * the identifier will be ignored, but should remain set to zero.
 */

#define VH_OP_ID_INVALID		(0x00u)
#define VH_OP_ID_NM				(0x01u)		/* Field Name */
#define VH_OP_ID_HF				(0x02u)		/* Heap Field */
#define VH_OP_ID_FI				(0x03u)		/* Field Index */

#define VH_OP_ID_FMTSTR			(0x04u)		/* const char * Format String */

#define VH_OP_ID_BOTH			(0x08u)		


#define VH_OP_MAKEFLAGS(return_type, lhs_type, lhs_ident, rhs_type, rhs_ident)	\
	(((return_type) << 24) | 						\
	 ((lhs_type) << 16) | 							\
	 ((lhs_ident) << 12) | 							\
	 ((rhs_type) << 4) | (rhs_ident))



void* vh_typevar_op(const char *op, int32_t flags, ...);
bool vh_typevar_comp(const char *op, int32_t flags, ...);
int32_t vh_typevar_comp_impl(int32_t flags, ...);


/*
 * Fast Path Operators
 *
 * Fast path operators allow us to do one parse and then pass thru data pointers
 * for the target types.  Fast path operators should be used when iterating an
 * array.
 */

typedef struct TypeVarOpExecData *TypeVarOpExec;

/*
 * Comparison
 */
TypeVarOpExec vh_typevar_comp_init(const char *op, int32_t flags, ...);
TypeVarOpExec vh_typevar_comp_init_tys(const char *op, 
									   Type *tys_lhs, Type *tys_rhs);
void vh_typevar_comp_swap_op(TypeVarOpExec tvope, const char *op);
void vh_typevar_comp_destroy(TypeVarOpExec tvope);
bool vh_typevar_comp_fp(TypeVarOpExec tvope, ...);

/*
 * Operators
 */
TypeVarOpExec vh_typevar_op_init(const char *op, int32_t flags, ...);
TypeVarOpExec vh_typevar_op_init_tys(const char *op, 
									 Type *tys_lhs, Type *tys_rhs, 
									 Type *tys_ret);
void vh_typevar_op_destroy(TypeVarOpExec tvope);
void *vh_typevar_op_fp(TypeVarOpExec tvope, ...);

#endif

