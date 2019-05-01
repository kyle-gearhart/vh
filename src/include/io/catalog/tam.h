/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_catalog_tam_H
#define vh_io_catalog_tam_H

#include "io/catalog/HeapField.h"

/*
 * There are four locations of TAM available to callers, the preference can be
 * set via the caller.  Or, the standard macros can be used which provide the
 * following location preference ordering (most preferred to least preferred):
 *
 *
 * vh_tam_tf_be			TableField with BackEnd
 *
 * 	1)	TableField->TableFieldBackEndOpts	[TableField, BackEnd]
 * 	2)	BackEnd->Type						[BackEnd]
 * 	3)	TableField							[TableField]
 * 	4)	Type
 *
 *
 * vh_tam_be			Type/Type Stack with BackEnd
 * 	1)	BackEnd->Type						[BackEnd]
 * 	2)	Type
 *
 *
 * vh_tam_tf			TableField
 * 	1)	TableField							[TableField]
 * 	2)	Type
 *
 * vh_tam_type			Type/Type Stack
 * 	1)	Type
 *
 *
 * We use four bits to identify a location, as this give us 15 distinct
 * locations that we can identify with four bits (2 ^ 4) - 1.  Zero
 * does not indicate a location.  With a 32-bit integer, we can specify
 * up to 8 locations orderings (32 bits / 4 bits).  To simplify our calling
 * convention, we use a bunch of defines to always call vh_tam_fill_stack
 * based on our location preference.  This way we've only got one function 
 * to maintain known as vh_tam_fill_stack.
 *
 * The right most three bits indicates the most preferred TAM location
 * while the left most three bits indicates the least preferred TAM
 * location.
 *
 * This allows us to fill the TAM in any preference order, from one function
 * call.  We use a bunch of macros to change the locations and preference
 * ordering, plus the context.
 */ 

#define vh_tam_loc_invalid		0x0
#define vh_tam_loc_type 		0x1
#define vh_tam_loc_tf			0x2
#define vh_tam_loc_betype		0x3
#define vh_tam_loc_tfbe			0x4

#define vh_tam_type 	( ( 0 << 4 ) | ( vh_tam_loc_type ) )
#define vh_tam_be		( ( 0 << 8 ) | ( vh_tam_loc_type << 4 ) | vh_tam_loc_betype )
#define vh_tam_tf 		( ( 0 << 8 ) | ( vh_tam_loc_type << 4 ) | vh_tam_loc_tf )
#define vh_tam_tf_be 	( ( 0 << 16 ) | ( vh_tam_loc_type << 12 ) | \
						  ( vh_tam_loc_tf << 8 ) | \
						  ( vh_tam_loc_betype << 4 ) | \
						  ( vh_tam_loc_tfbe ) )




/*
 * vh_tam_fill_stack
 *
 * Note: we should be careful to only supply TableField as the tf parameter.
 * The only data needed on HeapField is the type stack, which can be placed in
 * the tys parameter.
 */
bool vh_tam_fill_stack(int32_t fields, TypeAM tam, 
					   Type *tys, int8_t tys_depth,
					   BackEnd be, TableField tf,
			  		   TamUnion *funcs, void **cstr_format,
					   bool get);


/*
 * Basic Type without BackEnd
 */

#define vh_tam_type_fill_get(tam, ty, funcs, fmt) \
	vh_tam_fill_stack(vh_tam_type, tam, &(ty), 1, 0, 0, (TamUnion*)funcs, fmt, true)

#define vh_tam_type_fill_set(tam, ty, funcs, fmt) \
	vh_tam_fill_stack(vh_tam_type, ta, &(ty), 1, 0, 0, (TamUnion*)funcs, fmt, false)


/*
 * Basic Type with BackEnd
 */

#define vh_tam_be_type_fill_get(tam, be, ty, funcs, fmt) \
	vh_tam_fill_stack(vh_tam_be, &(ty), 1, be, 0, (TamUnion*)funcs, fmt, true)

#define vh_tam_be_type_fill_set(tam, be, ty, funcs, fmt) \
	vh_tam_fill_stack(vh_tam_be, &(ty), 1, be, 0, (TamUnion*)funcs, fmt, false)


/*
 * Basic Type Stack without BackEnd
 */
#define vh_tam_types_fill_get(tam, tys, tys_depth, funcs, fmt) \
	vh_tam_fill_stack(vh_tam_type, tam, tys, tys_depth, 0, 0, (TamUnion*)funcs, fmti, true)

#define vh_tam_types_fill_set(tam, tys, tys_depth, funcs, fmt) \
	vh_tam_fill_stack(vh_tam_type, tam, tys, tys_depth, 0, 0, (TamUnion*)funcs, fmti, false)


/*
 * Basic Type Stack with BackEnd
 */

#define vh_tam_be_fill_get(tam, be, tys, tys_depth, funcs, fmt)					\
	vh_tam_fill_stack(vh_tam_be, tam, tys, tys_depth, be, 0, (TamUnion*)funcs, fmt, true)
#define vh_tam_be_fill_set(tam, be, tys, tys_depth, funcs, fmt) 				\
	vh_tam_fill_stack(vh_tam_be, tam, tys, tys_depth, be, 0, (TamUnion*)funcs, fmt, false)


/*
 * TableField/HeapField without BackEnd
 *
 * Use the vh_tam_field_fill variant to automatically determine a HeapField
 * versus a TableField.  A HeapField will cause a vh_tam_be preference while a
 * TableField triggers a vh_tam_tf_be preference.
 */

#define vh_tam_tf_fill_get(tam, tf, funcs, fmt)									\
	vh_tam_fill_stack(vh_tam_tf, 												\
					  tam, 														\
					  &(tf)->heap.types[0],										\
					  (tf)->heap.type_depth,									\
					  0, 														\
					  (tf),														\
					  (TamUnion*)funcs, 										\
					  fmt,														\
					  true)

#define vh_tam_tf_fill_set(tam, tf, funcs, fmt)									\
	vh_tam_fill_stack(vh_tam_tf, 												\
					  tam, 														\
					  &(tf)->heap.types[0],										\
					  (tf)->heap.type_depth,									\
					  0, 														\
					  (tf),														\
					  (TamUnion*)funcs, 										\
					  fmt,														\
					  false)

#define vh_tam_field_fill_get(tam, hf, funcs, fmt)		\
	( vh_hf_is_tablefield(hf) ? vh_tam_tf_fill_get(tam, (TableField)hf, funcs, fmt) : \
	  vh_tam_types_fill_get(tam, &hf->types[0], hf->type_depth, funcs, fmt) )

#define vh_tam_field_fill_set(tam, hf, funcs, fmt)		\
	( vh_hf_is_tablefield(hf) ? vh_tam_tf_fill_set(tam, (TableField)hf, funcs, fmt) : \
	  vh_tam_types_fill_set(tam, &hf->types[0], hf->type_depth, funcs, fmt) )


/*
 * TableField/HeapField with BackEnd
 *
 * Use the vh_tam_be_field_fill variant to automatically determine a HeapField
 * versus a TableField.  A HeapField will cause a vh_tam_be preference while a
 * TableField triggers a vh_tam_tf_be preference.
 */


#define vh_tam_be_tf_fill_get(tam, be, tf, funcs, fmt) 							\
	vh_tam_fill_stack(vh_tam_tf_be, 											\
					  tam,														\
					  &(tf)->heap.types[0],										\
					  (tf)->heap.type_depth, 									\
   					  be,														\
   					  (tf),														\
   					  (TamUnion*)funcs, 										\
   					  fmt, 														\
   					  true)

#define vh_tam_be_tf_fill_set(tam, be, tf, funcs, fmt) 							\
	vh_tam_fill_stack(vh_tam_tf_be, 											\
					  tam,														\
					  &(tf)->heap.types[0],										\
					  (tf)->heap.type_depth, 									\
					  be,														\
					  (tf),														\
					  (TamUnion*)funcs, 										\
					  fmt, 														\
					  false)

#define vh_tam_be_field_fill_get(tam, be, hf, funcs, fmt)		\
	( vh_hf_is_tablefield(hf) ? vh_tam_be_tf_fill_get(tam, be, (TableField)hf, funcs, fmt) : \
	  vh_tam_be_fill_get(tam, be, &((HeapField)hf)->types[0], ((HeapField)hf)->type_depth, funcs, fmt) )

#define vh_tam_be_field_fill_set(tam, be, hf, funcs, fmt)		\
	( vh_hf_is_tablefield(hf) ? vh_tam_be_tf_fill_set(tam, be, (TableField)hf, funcs, fmt) : \
	  vh_tam_be_fill_set(tam, be, &((HeapField)hf)->types[0], ((HeapField)hf)->type_depth, funcs, fmt) )





/*
 * vh_tam_htd_create
 */

TamUnion** vh_tam_htd_create(int32_t fields, TypeAM tam,
							 HeapTupleDef htd, BackEnd be,
							 void ****cstr_format,
							 bool get);



#endif

