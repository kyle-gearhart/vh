/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef vh_datacatalog_Type_H
#define vh_datacatalog_Type_H

/*
 * Type and Type Access Method (TAM)
 */

/* 
 * Type Stack
 */

typedef unsigned short TypeTag;

/*
 * Type Stack Key
 *
 * Allows for us to store the type identifer in a key on the stack.  This is
 * helpful when we need to store an entire type stack in a HashTable.
 *
 * The left most bytes are filled until we are out of types in the stack and 
 * the remaining bytes are filled with zeros.
 */
typedef struct TypeStackKey
{
	union
	{
		TypeTag tags[VH_TAMS_MAX_DEPTH];

		struct
		{
			uint64_t upper;
			uint64_t lower;
		};
	};
} TypeStackKey;

void vh_type_stack_init(Type *typestack);
int8_t vh_type_stack_push(Type *stack, Type type);

int8_t vh_type_stack_depth(Type *tys);
size_t vh_type_stack_data_maxalign(Type *stack);
size_t vh_type_stack_data_width(Type *stack);
bool vh_type_stack_has_varlen(Type *stack);
bool vh_type_stack_has_construct(Type *stack);

void vh_type_stack_properties(Type *stack, int8_t scan_limit, 
							  int8_t *depth, 
							  size_t *width, size_t *max_align,
							  bool *constructor);


bool vh_type_stack_match(Type *lhs, Type *rhs);
int8_t vh_type_stack_copy(Type *dest, Type *src);
int8_t vh_type_stack_key(Type *tys, TypeStackKey *tsk);
int8_t vh_type_stack_2tags(TypeTag *tags, Type *tys);
int8_t vh_type_stack_fromtags(Type *tys, TypeTag *tags);

int8_t vh_type_stack_fillaccum(Type *tys_accum, Type *tys);

/* 
 * Type Access Method Stack (TAM Stack)
 *
 * THe TAM Stack allows for types to nest.  For example, the top level type frame
 * may be an Array type.  The type nested in the array may be a range type and the
 * inner most type may be a Date.  A generic TamStack is defined further down.
 *
 * Each TAM has it's own stack structure, although the generic TamStack structure
 * can be substituted provided the user knows what the calling pattern is (i.e.
 * binary get or memset get).
 */

typedef enum TypeAM
{
	TAM_Binary,
	TAM_CStr,
	TAM_MemSet
} TypeAM;

struct TamBinGetStack;
struct TamBinSetStack;

/*
 * Binary Access Method Options
 *
 * These are intended to be more global specific to particular access patttern
 * (i.e. Postgres backend vs. VH.IO internals).  The provide details to the
 * TAM about how it should allocate and format data.
 */

typedef struct BinaryAMOptionData
{
	bool sourceBigEndian;
	bool targetBigEndian;
	bool malloc;
} *BinaryAMOptions;

/*
 * vh_tam_bin_length
 *
 * Returns the length, in bytes, of an instance of VH.IO type.
 */

typedef size_t (*vh_tam_bin_length)(Type type, const void *source);

/*
 * vh_tam_binary_get
 *
 * Gets a value from the VH.IO defined type and copies the value to a caller
 * specified buffer.
 *
 * The |source| field is the underlying type (i.e. struct StringData*) and
 * the target may be one of two options:
 * 	1)	When the |malloc| flag is set in |bopts|, the target has no effect.
 *  	Memory should be allocated up to |length| bytes if a non-zero value
 *  	is indicated by the caller.  Zero value |length| argument indicates
 *  	to malloc the entire length of the source to the target.
 *  2)	Otherwise, the target is a void* of up to |length| bytes.
 *
 * RULES: WITH CURSOR
 * When a non-null, non-zero |cursor| has been provided, with or without
 * malloc, the TAM should start the operation at the byte indicated by the cursor.
 *
 * When a non-null zero cursor and a non-null zero value length length has been
 * provided, we're just looking for the  length.  We do this so we don't have 
 * to defined back end specific length determination functions for each type.  
 * If we've got a custom binary get then we've probably got a different length 
 * calculation.  Avoid doing double the work.
 *
 * RULES: WITH MALLOC
 * If |length| is null and the malloc flag is set, the TAM should throw an 
 * error and abort.
 *
 * A zero for |length| means attempt to allocate the entire amount.  When malloc is
 * supplied, always return the allocated buffer.
 *
 * RULES: WITHOUT MALLOC
 * If |length| is zero or null and the malloc flag is not set, the TAM should
 * throw an error and abort.  The Type should have visibility to the length of
 * the buffer it has been passed when no authorization to malloc has been
 * given.
 *
 * If |length| is specified without the malloc flag and the source length is
 * greater than the |length| specified by the caller, a cursor must be
 * supplied.  If the caller provides a null cursor, the TAM should throw an
 * error and abort.  Otherwise, set the cursor equal to the |length| specified
 * by the caller before exiting.  Update the |length| to the actual length
 * of the underlying type before exiting.
 *
 * Never return a value when malloc is not specified.
 *
 * MALLOC	LENGTH		CURSOR		ACTION
 *	Y		!null, !0	null		May throw an error (1)
 *	Y		!null, !0	!null, !0	May throw an error (1)
 *	N		!null, !0	null		May throw an error (1)
 *
 *	Y		0			!null, 0	Malloc the whole thing (3)
 *	N		0			*			Provide length only
 *	N		!null, !0	!null		Start at cursor, copy up to Length bytes
 *
 * 	INVALID OPTIONS
 *	Y		0			!null, !0	Throw an error (2)
 *	Y		null		*			Throw an error (4)
 *	N		null		*			Throw an error (4)
 *
 * (1) 	If it fits you're good to go
 * (2)	We can't call an unlimited length with a cursor, doesn't make any
 * 		sense
 * (3)	Unless we run out of memory
 * (4)	We gotta have a length pointer, period.
 */
typedef void* (*vh_tam_bin_get)(struct TamBinGetStack *tamstack, 
								const BinaryAMOptions bopts,
  								const void *source, void *target,
  								size_t *length, size_t *cursor);

struct TamBinGetStack
{
	Type *types;
	vh_tam_bin_get *funcs;
};

/*
 * A few check definitions to make sure we're calling everything correctly.
 */

#define vh_tam_bin_get_invalid(bopts, len, cur) { 							\
	assert(!(bopts->malloc && len && *len == 0 && cur && *cur > 0)); 		\
	assert(!(bopts->malloc && !len));										\
	assert(!(!bopts->malloc && !len));										\
}

#define vh_tam_bin_get_warn(bopts, len, cur) { 								\
	assert(!(bopts->malloc && len && *len > 0 && !cur));					\
	assert(!(bopts->malloc && len && *len > 0 && cur && *cur > 0));			\
	assert(!(!bopts->malloc && len && *len > 0 && !cur));					\
}

#define vh_tam_bin_get_check(bopts, len, cur)	vh_tam_bin_get_invalid(bopts, len, cur) \
												vh_tam_bin_get_warn(bopts, len, cur)

/*
 * Takes a pointer to a Type array, plus the usual binary get suspects:
 * BinaryAMOptions, source, target, size and cursor pointers.
 *
 * Callers must only supply a type array and then we'll figure everything else
 * out.  Does not provide backend specific functions.
 */
#define vh_tam_fire_bin_get(t, bopts, src, tgt, sz, csr)						\
	( ( (t)[0] ? ( { 	TamGetUnion tsu[VH_TAMS_MAX_DEPTH];						\
					 	vh_tams_fill_get_funcs((t), &tsu[0], TAM_Binary)		\
				?	vh_tam_fireu_bin_get((t), tsu, bopts, src, tgt, sz, csr)	\
				: 0; } )														\
			 : 0 ) )	\
/*
 * Takes a pointer to a Type array and a pointer to a vh_tam_bin_get array.
 *
 * Fires the TAM by setting up a TamBinGetStack structure and calling the
 * actual function.  Hence the _firea_ stands for fire from array.
 */
#define vh_tam_firea_bin_get(t, f, bopts, src, tgt, sz, csr)	\
	( ( f[0] ? ( { 	struct TamBinGetStack ts = { 				\
				 		.types = (&t[1]), 						\
				 		.funcs = (&f[1]) }; 					\
	  				f[0](&ts, bopts, src, tgt, sz, csr);		\
				 } ) : 0 ) )

/*
 * Takes a pointer to the type array and a pointer to the TamGetUnion
 * array.
 *
 * Fires the TAM by setting up a TamBinGetStack structure and calling the
 * calling TamGetUnion.bin function.  Hence the _fireu_ stands for 
 * fire from union array.
 */
#define vh_tam_fireu_bin_get(t, f, bopts, src, tgt, sz, csr)	\
	( ( f[0].bin ? ( { 	struct TamBinGetStack ts = { 		\
				 		.types = &((t)[1]), 					\
				 		.funcs = &((f)[1].bin) }; 				\
	  				f[0].bin(&ts, bopts, src, tgt, sz, csr);	\
				 } ) : 0 ) )
/*
 * Takes a TamBinGetStack pointer and fires the first level.  Hence, the
 * _fires_ stands for fire start.
 */
#define vh_tam_fires_bin_get(ts, bopts, src, tgt, sz, csr) \
	( ( ts->funcs[0] ? ( ts->funcs[0](ts, bopts, src, tgt, sz, csr) ) : 0 ) )

/*
 * Takes the existing TamBinGetStack and creates another structure by
 * advancing the passed in structure by one.  Assumes the caller has tested
 * for another non-null frame with vh_tam_can_fire_next.
 */
#define vh_tam_firen_bin_get(ts, bopts, src, tgt, sz, csr) \
	( ( ts->funcs[0] ? ( { struct TamBinGetStack tsn = { \
	  		.types = (&ts->types[1]), \
	  		.funcs = (&ts->funcs[1]) }; \
	  	((ts->funcs[0])(&tsn, bopts, src, tgt, sz, csr)); } ) : 0))

/*
 * Fires the vh_tam_bin_get pointer with an empty TamBinGet stack.  Hnce
 * the _firee name.
 */
#define vh_tam_firee_bin_get(func, bopts, src, tgt, sz, csr) 	\
	( ( func ? ( { 												\
				 	Type tys[1] = { };							\
				 	vh_tam_bin_get funcs[1] = { };				\
				 	struct TamBinGetStack ts = {				\
				 		.types = &tys[0],						\
						.funcs = &funcs[0] };					\
					func(&ts, bopts, src, tgt, sz, csr); } ) : 0 ))

/*
 * vh_tam_binary_set
 *
 * Sets a value to a VH.IO defined type from the caller specified |source|
 * buffer of |length| bytes long.  If a non-zero cursor is supplied, the 
 * underlying type should append the |source| buffer starting at byte |cursor|.
 *
 * The |source| field is a pointer to an unsigned char buffer and
 * the target may be one of two options:
 * 	1)	When the |malloc| flag is set in |bopts|, the target is a pointer
 * 		to a pointer of the underying type (i.e. struct StringData **).
 * 		The underlying type should be created with enough space to accomodate
 * 		|length| bytes of data.  A |length| of zero indicates to use the 
 * 		default constructor for the type.
 *  2)	Otherwise, the target's underlying type has already been constructed
 *  	at the location indicated by |target| (i.e. it's a valid struct
 *  	StringData *).
 *
 * RULES: WITH MALLOC
 * A zero cursor must be supplied when malloc is given.  Always return the
 * pointer to the new type.
 *
 * RULES: WITHOUT MALLOC
 * When a cursor is supplied, run an append at the byte indicated by |cursor|.
 */


typedef void* (*vh_tam_bin_set)(struct TamBinSetStack *tamstack, 
								const BinaryAMOptions bopts, 
	 							const void *source, void *target,
	 							size_t length, size_t cursor);

struct TamBinSetStack
{
	Type *types;
	vh_tam_bin_set *funcs;
};

#define vh_tam_fire_bin_set(t, bopts, src, tgt, sz, csr)						\
	( ( (t)[0] ? ( { 	TamSetUnion tsu[VH_TAMS_MAX_DEPTH];						\
					 	vh_tams_fill_set_funcs((t), &tsu[0], TAM_Binary)		\
				?	vh_tam_fireu_bin_set((t), tsu, bopts, src, tgt, sz, csr)	\
				: 0; } )														\
			 : 0 ) )

#define vh_tam_firea_bin_set(t, f, opts, src, tgt, sz, csr)	 	\
	( ( f[0] ? ( { 	struct TamBinSetStack ts = { 				\
				 		.types = (&t[1]), 						\
				 		.funcs = (&f[1]) }; 					\
	  				f[0](&ts, bopts, src, tgt, sz, csr);		\
				 } ) : 0 ) )

#define vh_tam_fireu_bin_set(t, f, bopts, src, tgt, sz, csr)	\
	( ( f[0].bin ? ( { 	struct TamBinSetStack ts = { 		\
				 		.types = &((t)[1]), 					\
				 		.funcs = &((f)[1].bin) }; 				\
	  				f[0].bin(&ts, bopts, src, tgt, sz, csr);	\
				 } ) : 0 ) )

#define vh_tam_fires_bin_set(ts, bopts, src, tgt, sz, csr) \
	( ( ts->funcs[0] ? ( ts->funcs[0](ts, bopts, src, tgt, sz, csr) ) : 0 ) )

#define vh_tam_firen_bin_set(ts, opts, src, tgt, sz, csr) \
	( ( ts->funcs[0] ? ( { struct TamBinSetStack tsn = { \
	  		.types = (&ts->types[1]), \
	  		.funcs = (&ts->funcs[1]) }; \
	  	((ts->funcs[0])(&tsn, bopts, src, tgt, sz, csr)); } ) : 0))

#define vh_tam_firee_bin_set(func, bopts, src, tgt, sz, csr) 	\
	( ( func ? ( { 												\
				 	Type tys[1] = { };							\
				 	vh_tam_bin_set funcs[1] = { };				\
				 	struct TamBinSetStack ts = {				\
				 		.types = &tys[0],						\
						.funcs = &funcs[0] };					\
					func(&ts, bopts, src, tgt, sz, csr); } ) : 0 ))


/*
 * Character Operations
 */


typedef struct CStrAMOptionsData
{
	bool malloc;
} const *CStrAMOptions;

struct TamCStrGetStack;
struct TamCStrSetStack;

/*
 * Some Types may a very specialized formatter that aren't simply a
 * const char*.  The types supported by the ICU4C library are an example,
 * we call vh_tam_cstr_fmt to get the formatter, which may or may not
 * be a const char*.  Thus vh_tam_cstr_get and vh_tam_cstr_set should be
 * fully aware of the memory pointed to by |format| when called.
 *
 * Users should call vh_tam_cstr_format to get the void pointer to the
 * formatter.  It may return null.
 *
 * When processing has been completed, vh_tam_cstr_format_finalize should
 * be called.  These are merely convience _format and _format_finalize
 * are convienence features.
 */

void* vh_tam_cstr_format(Type ty, const char *pattern,
						 const char **patterns, int32_t n_patterns);

void vh_tam_cstr_formats_destroy(Type *tys, void **fmts);
void vh_tam_cstr_format_destroy(Type ty, void *fmt);

typedef void* (*vh_tam_cstr_fmt)(Type ty, const char **patterns, int32_t n_patterns);
typedef void (*vh_tam_cstr_fmt_destroy)(Type ty, void *formatter);

typedef size_t (*vh_tam_cstr_length)(Type *tys, void *data, void *format);

typedef char* (*vh_tam_cstr_get)(struct TamCStrGetStack *tamstack, 
								 CStrAMOptions copts, 
								 const void *source, 
								 char *target,
								 size_t *length, size_t *cursor, 
								 void *formatter);

typedef void* (*vh_tam_cstr_set)(struct TamCStrSetStack *tamstack, 
								 CStrAMOptions copts,
								 const char *source, 
								 void *target, 
								 size_t length, size_t cursor, 
								 void *formatter);

struct TamCStrGetStack
{
	Type *types;
	vh_tam_cstr_get *funcs;
	void **formatters;
};

struct TamCStrSetStack
{
	Type *types;
	vh_tam_cstr_set *funcs;
	void **formatters;
};

#define vh_tam_fire_cstr_get(t, copts, src, tgt, sz, csr, fmt)					\
	( ( (t)[0] ? ( { 	TamGetUnion tsu[VH_TAMS_MAX_DEPTH];						\
					 	vh_tams_fill_get_funcs((t), &tsu[0], TAM_CStr)			\
				?	vh_tam_fireu_cstr_get((t), tsu, copts, src, tgt, sz, csr, fmt)	\
				: 0; } )														\
			 : 0 ) )	\

#define vh_tam_firea_cstr_get(t, f, copts, src, tgt, len, csr, fmt)							\
	( ( (f)[0] ? ( { struct TamCStrGetStack ts = {											\
				  		.types = &((t)[1]),													\
				  		.funcs = &((f)[1]),													\
						.formatters = &((fmt)[1]) };										\
					(f)[0](&ts, (copts), (src), (tgt), (len), (csr), (fmt)[0]);				\
				  } ) : 0 ) )



#define vh_tam_fireu_cstr_get(t, f, copts, src, tgt, len, csr, fmt)							\
	( ( (f)[0].cstr ? ( {	struct TamCStrGetStack ts = { 									\
				 		.types = &((t)[1]), 												\
				 		.funcs = &((f)[1].cstr),											\
					 	.formatters = &((fmt)[1]) }; 										\
	  				(f)[0].cstr(&ts, (copts), (src), (tgt), (len), (csr), (fmt)[0]);		\
				 } ) : 0 ) )

#define vh_tam_firea_cstr_set(t, f, copts, src, tgt, len, csr, fmt)							\
	( ( (f)[0] ? ( { struct TamCStrSetStack ts = {											\
				   		.types = &((t)[1]),													\
				   		.funcs = &((f)[1]),													\
				   		.formatters = &((fmt)[1]) };										\
				   (f)[0](&ts, (copts), (src), (tgt), (len), (csr), (fmt)[0]);				\
				   } ) : 0 ) )

#define vh_tam_fireu_cstr_set(t, f, copts, src, tgt, len, csr, fmt)							\
	( ( (f)[0].cstr ? ( {	struct TamCStrSetStack ts = { 									\
				 		.types = &((t)[1]), 												\
				 		.funcs = &((f)[1].cstr),											\
					 	.formatters = &((fmt)[1]) }; 										\
	  				(f)[0].cstr(&ts, (copts), (src), (tgt), (len), (csr), (fmt)[0]);		\
				 } ) : 0 ) )

struct TamGenStack;

typedef void (*vh_tam_generic)(struct TamGenStack *tamstack, 
							   void *source, void *target);

struct TamGenStack
{
	Type *types;
	vh_tam_generic *funcs;
	bool copy_varlendat;
};


/*
 * MemSet uses the generic TAM function call.  Implementations should copy the
 * HeapBufferNo from the source to the target.  If the HeapBufferNo changes,
 * then we should be careful to deallocate any old out of line data in the old
 * buffer and new data in the new buffer.
 */

#define vh_tam_fireh_memset_set(hf, src, tgt, varlen) 						\
	( vh_tam_fire_memset_set(&(hf->types[0]), src, tgt, varlen) ) 

#define vh_tam_fire_memset_set(t, src, tgt, varlen)							\
	( ( (t)[0] ? ( { 	TamSetUnion tsu[VH_TAMS_MAX_DEPTH];					\
					 	vh_tams_fill_set_funcs((t), &tsu[0], TAM_MemSet)	\
					 		?	vh_tam_fireu_memset_set((t), 				\
														&tsu[0], 			\
														src, 				\
														tgt,				\
														varlen)				\
							: 0; } )										\
			 : 0 ) )	\

#define vh_tam_firea_memset_set(t, f, src, tgt, varlen)					 	\
	( ( (f)[0] ? ( { 	struct TamGenStack ts = { 							\
				 		.types = &((t)[1]), 								\
				 		.funcs = &((f)[1]),									\
				  		.copy_varlendat = (varlen) }; 						\
	  				(f)[0](&ts, src, tgt);									\
				 } ) : 0 ) )

#define vh_tam_fireu_memset_set(t, f, src, tgt, varlen)		 				\
	( ( (f)[0].memset ? ( { 	struct TamGenStack ts = { 					\
				 		.types = &((t)[1]), 								\
				 		.funcs = &((f)[1].memset),							\
						.copy_varlendat = (varlen) }; 						\
	  				(f)[0].memset(&ts, src, tgt);							\
				 } ) : 0 ) )

#define vh_tam_fires_memset_set(ts, src, tgt) 								\
	( ( ts->funcs[0] ? ( ts->funcs[0](ts, src, tgt) ) : 0 ) )

#define vh_tam_firen_memset_set(ts, src, tgt)								\
	( ( ts->funcs[0] ? ( { struct TamGenStack tsn = { 						\
	  		.types = (&ts->types[1]), 										\
	  		.funcs = (&ts->funcs[1]),										\
			.copy_varlendat = ts->copy_varlendat }; 						\
	  	((ts->funcs[0])(&tsn, src, tgt)); } ) : 0))

#define vh_tam_firee_memset_set(func, src, tgt, varlen) 					\
	( ( func ? ( { 															\
				 	Type tys[1] = { };										\
				 	vh_tam_bin_set funcs[1] = { };							\
				 	struct TamGenStack ts = {								\
				 		.types = &tys[0],									\
						.funcs = &funcs[0],									\
						.copy_varlendat = (varlen) };						\
					func(&ts, src, tgt); } ) : 0 ))

/*
 * Generic TAM Stack
 */
struct TamStack
{
	Type *types;
	
	union
	{
		vh_tam_bin_get *bin_get;
		vh_tam_bin_set *bin_set;

		vh_tam_cstr_get *cstr_get;
		vh_tam_cstr_set *cstr_set;

		vh_tam_generic *memset_get;
		vh_tam_generic *memset_set;
	};
};

#define vh_tam_can_fire_next(ts) (((struct TamStack*)ts)->types[0])

typedef union TamGetUnion
{
	vh_tam_bin_get bin;
	vh_tam_cstr_get cstr;
	vh_tam_generic memset;
	void *ptr;
} TamGetUnion;

typedef union TamSetUnion 
{
	vh_tam_bin_set bin;
	vh_tam_cstr_set cstr;
	vh_tam_generic memset;
	void *ptr;
} TamSetUnion;

typedef union TamUnion
{
	TamGetUnion get;
	TamSetUnion set;
} TamUnion;


/*
 * Fills the associated TAM function union with the requested access method.
 *
 * Assumes the get/set array is atleast VH_TAMS_MAX_DEPTH in size.
 *
 * Returns true if atleast one TAM was filled in the array.
 */
bool vh_tams_fill_set_funcs(Type *stack, TamSetUnion *set, TypeAM tam);
bool vh_tams_fill_get_funcs(Type *stack, TamGetUnion *get, TypeAM tam);

bool vh_tams_contains_varlen(Type *typestack);
size_t vh_tams_fixed_size(Type *typestack);
size_t vh_tams_length_from_get(Type *typestack, TamGetUnion *funcs, 
							   TypeAM tam, const void *source);

typedef struct TypeAMFuncs
{
	/* Binary operations */
	vh_tam_bin_get bin_get;
	vh_tam_bin_set bin_set;
	vh_tam_bin_length bin_length;

	vh_tam_cstr_fmt cstr_fmt;
	vh_tam_cstr_fmt_destroy cstr_fmt_destroy;
	vh_tam_cstr_get cstr_get;
	vh_tam_cstr_set cstr_set;
	vh_tam_cstr_length cstr_length;
	const char *cstr_format;

	/* MemSet operations */
	vh_tam_generic memset_get;
	vh_tam_generic memset_set;
} TypeAMFuncs;

/*
 * TYPE OPERATOR METHODS (TOM)
 */

#define vh_tom_assert_bottom(tcs)	{										\
			if (tcs)														\
			{																\
				assert(!tcs->types[0]);										\
				assert(!tcs->funcs[0]); 									\
			}																\
		}

struct TomCompStack;
struct TomConstructStack;
struct TomDestructStack;


/*
 * vh_tom_comp
 *
 * Compares two types, including any type nesting.  Users should always start
 * with the firea variant.  The actual TOM implementation may call firen.
 */
typedef int32_t (*vh_tom_comp)(struct TomCompStack *tomstack,
							   const void *lhs, const void *rhs);
struct TomCompStack
{
	Type *types;
	vh_tom_comp *funcs;
};

bool vh_toms_has_comp_func(Type *stack);
bool vh_toms_fill_comp_funcs(Type *stack, vh_tom_comp *comp);

/*
 * vh_tom_firea_comp
 *
 * Stands up a TomCompStack and the associated methods.  This is a little
 * tricky because if a user hasn't defined comparison method and we've got
 * nested types, then we'll stand in a stub to do it.  All the stub is going
 * to do is call vh_tom_firen_comp.
 */
//int32_t vh_tom_firea_comp(Type *stack, const void *lhs, const void *rhs);


#define vh_tom_fireh_comp(hf, lhs, rhs) 									\
	( vh_tom_fire_comp(&(hf->types[0]), lhs, rhs) ) 

#define vh_tom_fire_comp(t, lhs, rhs)										\
	( ( (t)[0] ? ( { 	vh_tom_comp tsc[VH_TAMS_MAX_DEPTH];					\
					 	vh_toms_fill_comp_funcs((t), &tsc[0])				\
					 		?	vh_tom_firea_comp((t), tsc, lhs, rhs)		\
							: 0; } )										\
			 : 0 ) )	\

#define vh_tom_firea_comp(t, f, lhs, rhs)								 	\
	( ( (f)[0] ? ( { 	struct TomCompStack ts = { 								\
				 		.types = &((t)[1]), 								\
				 		.funcs = &((f)[1]) }; 								\
	  				(f)[0](&ts, (lhs), (rhs));									\
				 } ) : 0 ) )

#define vh_tom_firen_comp(ts, lhs, rhs) ( \
		( ts && ts->funcs[0] ? ( { struct TomCompStack tsn = {				\
							   		.types = &ts->types[1],					\
							   		.funcs = &ts->funcs[1] }; 				\
						   		   ts->funcs[0](&tsn, lhs, rhs); } ) : 0) )

#define vh_tom_firee_comp(func, lhs, rhs) 									\
	( ( func ? ( { 															\
				 	Type tys[1] = { };										\
				 	vh_tom_comp funcs[1] = { };								\
				 	struct TomCompStack tsn = {								\
				 		.types = &tys[0],									\
						.funcs = &funcs[0] };								\
					func(&tsn, lhs, rhs); } ) : 0 ))
/*
 * vh_tom_construct
 * 
 * Calls the type constructors.
 */


typedef void (*vh_tom_construct)(struct TomConstructStack *tomstack,
								 void *target, HeapBufferNo hbno);
struct TomConstructStack
{
	Type *types;
	vh_tom_construct *funcs;
};

bool vh_toms_has_construct_func(Type *stack);
bool vh_toms_fill_construct_funcs(Type *stack, vh_tom_construct *funcs);

#define vh_tom_fireh_construct(hf, tgt, hbno) 								\
	( vh_tom_fire_construct(&(hf->types[0]), tgt, hbno) ) 

#define vh_tom_fire_construct(t, tgt, hbno)									\
	( ( (t)[0] ? ( { 	vh_tom_construct tsc[VH_TAMS_MAX_DEPTH];			\
					 	vh_toms_fill_construct_funcs((t), &tsc[0])			\
					 		?	vh_tom_firea_construct((t), tsc, tgt, hbno)	\
							: 0; } )										\
			 : 0 ) )

#define vh_tom_firea_construct(t, f, tgt, hbno)							 	\
	( ( (f)[0] ? ( { 	struct TomConstructStack ts = { 						\
				 		.types = &((t)[1]), 								\
				 		.funcs = &((f)[1]) }; 								\
	  				(f)[0](&ts, tgt, hbno);									\
				 } ) : 0 ) )

#define vh_tom_firen_construct(ts, t, hbno) ( 								\
		( ts && ts->funcs[0] ? ( { struct TomConstructStack tsc = {			\
							   			.types = &ts->types[1],				\
							   			.funcs = &ts->funcs[1] }; 			\
								   ts->funcs[0](&tsc, t, hbno); } ) 		\
		  					 : 0 ) )



/*
 * vh_tom_destruct
 *
 * Destructor for each type.  Intended to free any out of line memory the Type
 * may autonomously allocate.
 */

typedef void (*vh_tom_destruct)(struct TomDestructStack *tomstack,
								void *target);												 
struct TomDestructStack
{
	Type *types;
	vh_tom_destruct *funcs;
};


bool vh_toms_has_destruct_func(Type *stack);
bool vh_toms_fill_destruct_funcs(Type *stack, vh_tom_destruct *destruct);
void vh_tom_firea_destruct(Type *stack, void *target);

#define vh_tom_fireh_destruct(hf, tgt) 										\
	( vh_tom_fire_destruct(&(hf->types[0]), tgt) ) 

#define vh_tom_fire_destruct(t, tgt)										\
	( ( (t)[0] ? ( { 	vh_tom_destruct tsc[VH_TAMS_MAX_DEPTH];				\
					 	vh_toms_fill_destruct_funcs((t), &tsc[0])			\
					 		?	vh_tom_firea_destruct((t), tsc, tgt)		\
							: 0; } )										\
			 : 0 ) )	\

#define vh_tom_firea_destruct(t, f, tgt)								 	\
	( ( (f)[0] ? ( { 	struct TomDestructStack ts = { 							\
				 		.types = &((t)[1]), 								\
				 		.funcs = &((f)[1]) }; 								\
	  				(f)[0](&ts, tgt);											\
				 } ) : 0 ) )

#define vh_tom_firen_destruct(ts, tgt) ( \
		( ts && ts->funcs[0] ? ( { struct TomDestructStack tsn = {			\
							   			.types = &ts->types[1],				\
							   			.funcs = &ts->funcs[1] }; 			\
								   ts->funcs[0](&tsn, tgt); } ) 			\
		  					 : 0 ) )




struct TomGenStack
{
	Type *types;

	union
	{
		vh_tom_comp *comp;
		vh_tom_construct *construct;
		vh_tom_destruct *destruct;
	};
};

typedef struct TypeOMFuncs
{
	vh_tom_comp comp;
	vh_tom_construct construct;
	vh_tom_destruct destruct;
} TypeOMFuncs;

struct TypeOperRegData;
/*
 * |construct_for_htd|
 * 		always call the constructor when constructing a HeapTuple
 *
 * |varlen|
 * 		indicates this type may have out of line storage
 *
 * |varlen_has_header|
 * 		indicates the type has the varlen header as its first member
 *
 * |include_inner_for_size|
 * 		check the inner type for sizing constraints
 *
 * |inner_for_size_multiplier|
 * 		if |include_inner_for_size| is set then we'll multiply the inner
 * 		size by the value here.  Assume a Range >> Date nesting.  The range 
 * 		itself is going to report a size of 1 (for the flags).  The inner 
 * 		type is a date which will report a size of 4.  Then the multiplier 
 * 		specifies to multiply the value by 2 so the total calculated size is:
 * 		1 byte + (4 bytes * 2) = 9 bytes.
 *
 * 		Zeros are assumed as one.
 */


struct TypeData
{
	TypeTag id;
	TypeAMFuncs tam;
	TypeOMFuncs tom;
	const struct TypeOperRegData * const regoper;
	size_t regoper_sz;

	const char *name;

	Type accumulator;

	uint32_t size;
	uint32_t alignment;
	uint32_t max_size;			// set to 8 for SList
	
	bool byval;
	bool construct_forhtd;
	bool varlen;
	bool varlen_has_header;
	bool include_inner_for_size;
	bool allow_inner;
	bool require_inner;

	uint8_t inner_for_size_multiplier;
};


struct TypeData* vh_type_create(const char *name);
void vh_type_destroy(struct TypeData* ty);
void vh_type_init(struct TypeData *type);


/*
 * Operators
 *
 * We look thru the operator catalog for the tys_lhs first, then the operator
 * characters.  If we can't find an appropriate function, then we'll look thru
 * the commutable properties of the tys_rhs.  We're always going to return the
 * same type as tys_lhs.
 *
 */

typedef struct TomOperStack TomOperStack;

typedef int32_t (*vh_tom_oper)(TomOperStack *stack, 
								void *data_lhs, void *data_rhs,
								void *data_res);

struct TomOperStack
{
	Type *tys_lhs;
	Type *tys_rhs;
	vh_tom_oper *funcs;
};

#define vh_tom_firea_oper(ty_lhs, fs, ty_rhs, data_lhs, data_rhs, data_ret)		\
	( ( (fs)[0] ? ( { 	struct TomOperStack ts = { 								\
				 				.tys_lhs = &((ty_lhs)[1]), 						\
					   			.tys_rhs = &((ty_rhs)[1]),						\
				 				.funcs = &((fs)[1]) }; 							\
	  				(fs)[0](&ts, (data_lhs), (data_rhs), (data_ret));			\
				 } ) : 0 ) )

vh_tom_oper vh_type_oper(Type ty_lhs, const char *oper, Type ty_rhs,
						  int32_t *flags);
void vh_type_oper_register(Type ty_lhs, const char *oper, Type ty_rhs,
		 				   vh_tom_oper func, bool assign_lhs);

/*
 * struct TypeOperRegData
 *
 * Matches the signature of vh_type_oper_register so that when Types are
 * compiled in and added to the catalog, their operator functions can
 * be automatically added.
 */

struct TypeOperRegData
{
	Type lhs;
	const char *oper;
	Type rhs;
	vh_tom_oper func;
	int32_t flags;
};




/*
 * Native VH.IO types, which will be automatically registered in the type catalog.
 */

extern const struct TypeData vh_type_Array;
extern const struct TypeData vh_type_bool;
extern const struct TypeData vh_type_cstr;
extern const struct TypeData vh_type_Date;
extern const struct TypeData vh_type_DateTime;
extern const struct TypeData vh_type_dbl;
extern const struct TypeData vh_type_float;
extern const struct TypeData vh_type_int8;
extern const struct TypeData vh_type_int16;
extern const struct TypeData vh_type_int32;
extern const struct TypeData vh_type_int64;
extern const struct TypeData vh_type_numeric;
extern const struct TypeData vh_type_Range;
extern const struct TypeData vh_type_String;

#endif

