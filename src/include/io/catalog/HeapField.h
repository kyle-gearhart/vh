/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef vh_datacatalog_HeapField_H
#define vh_datacatalog_HeapField_H


/*
 * Note: |type_depth| does NOT include the null trailer pointer even
 * though it's always set in Types.  Thus the actual depth for Types
 * is V_TAMS_MAX_DEPTH - 1.
 */


#define vh_hf_tag_heapfield 		0x01
#define vh_hf_tag_tablefield 		0x02

#define vh_hf_is_tablefield(p)		(((HeapField)(p))->tag & vh_hf_tag_tablefield)
#define vh_hf_tf(h)					(vh_hf_is_tablefield(h) ? (TableField)(h) : 0)

typedef struct HeapFieldData
{
	uint32_t tag;
	Type types[VH_TAMS_MAX_DEPTH];
	uint32_t offset;
	uint32_t padding;
	uint32_t dord;
	uint32_t heapord;
	uint8_t maxalign;
	uint8_t type_depth;

	bool hasvarlen;
	bool hasconstructor;
	bool hasdestructor;
} HeapFieldData, *HeapField;

void vh_hf_init(HeapField hf);
bool vh_hf_push_type(HeapField hf, Type ty);


#define vh_hf_type_stack(hf)	(&((hf)->types[0]))

/*
 * FAST TYPE ACCESS METHOD FUNCTION STACK FOR HEAP FIELD
 *
 * Uses a HeapField type stack to fill a TamGetUnion or TamSetUnion with
 * the default functions defined on each nested type.
 *
 * |tam| specifies which function to set.
 */

#define vh_hf_tam_fill_get(hf, funcs, tam)	( \
			vh_tam_fill_get_funcs(&hf->types[0], funcs, tam) )
#define vh_hf_tam_fill_set(hf, funcs, tam) 	( \
			vh_tam_fill_set_funcs(&hf->types[0], funcs, tam) )

/*
 * FAST TYPE OPERATOR METHODS (TOM) FOR HEAP FIELD
 *
 * We've provided a few stubs to call a TOM quickly if there's only one
 * type on the HeapField stack.  In order for this to work correctly, the
 * HeapField must have its type stack built by vh_hf_push_type.
 *
 * We expect the HF to be casted to a HeapField.  The TOM will be called
 * using the HeapField's type stack indicated in |types|.
 *
 * vh_hf_tom_compare	Compares lhs with rhs
 * vh_hf_tom_construct	Constructs at location target
 * vh_hf_tom_destruct	Destructs at location target
 */

#define vh_hf_tom_comp(hf, lhs, rhs)  (											\
		( (hf)->type_depth == 1 ? (hf)->types[0]->tom.comp(0, lhs, rhs)			\
		  		: vh_tom_fireh_comp(hf, lhs, rhs) ) )

#define vh_hf_tom_construct(hf, target, hbno)  ( 								\
		( (hf)->type_depth == 1 && (hf)->types[0]->tom.construct 				\
		  		? (hf)->types[0]->tom.construct(0, target, hbno)				\
		  		: vh_tom_fireh_construct(hf, target, hbno) ) )

#define vh_hf_tom_destruct(hf, target) 	(										\
		( (hf)->type_depth == 1 && (hf)->types[0]->tom.destruct					\
		  		? (hf)->types[0]->tom.destruct(0, target)						\
	  			: vh_tom_fireh_destruct(hf, target) ) )

#define vh_hf_tom_setvarlen_hb(hf, target, hbno)		(0 == 0)

#endif

