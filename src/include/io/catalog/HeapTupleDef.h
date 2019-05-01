/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef vh_datacatalog_HeapTupleDef_H
#define vh_datacatalog_HeapTupleDef_H

/*
 * Defines how to construct a HeapTuple based on it's
 * members.
 *
 * |nfields|	the number of fields contained
 * |tupsize|	sum of native size of the fields
 * |tupasize|	same as |tupsize| but with alignment
 * 				between each field
 * |padding|	padding necessary to fully align a
 * 				HeapTuple
 * |heapsize|	size of the entire HeapTuple structure,
 * 				includes:
 * 					1) tupasize
 * 					2) nfields * sizeof(char)
 * 					3) padding
 */

typedef struct HeapTupleDefData
{
	uint32_t nfields;
	uint32_t hdrpad;
	uint32_t tupsize;
	uint32_t tupasize;
	uint32_t tuppad;
	uint32_t heapsize;
	uint32_t heapasize;

	uint32_t tupoffset;				/* Where the tuple begins, after the standard header and flags */

	uint32_t extraoffset;			/* Start of the extra data, this is mostly likely to be the Relations, but there's nothing stopping what can be put here.  When it's 0, then no extra has been provided. */
	SList fields;
	SList type_stack;				/* Array of pointers to Type (i.e. HeapField->types[0]) */
} HeapTupleDefData, *HeapTupleDef;

/*
 * vh_ht_tuple_offset
 *
 * We stopped calculating the offset on the fly and instead just store on the 
 * HeapTupleDef.  The only time it's going to change is when fields are added
 * or removed from the HTD.  The HeapTupleDef is capable of updating the offset
 * on its own.
 */
#define vh_htd_tuple_offset(htd)	(sizeof(struct HeapTupleData) + 			\
									 (sizeof(uint8_t) * (htd->nfields)) + 		\
									 htd->hdrpad)

HeapTupleDef vh_htd_create(size_t size);
void vh_htd_init(HeapTupleDef htd);
void vh_htd_finalize(HeapTupleDef htd, void (*for_each_field)(HeapTupleDef, void*));
bool vh_htd_add_field(HeapTupleDef htd, HeapField hf);
uint32_t vh_htd_add_extra(HeapTupleDef htd, uint32_t bytes);

Type** vh_htd_type_stack(HeapTupleDef htd);

/*
 * Calculates the size of the TAM Function array for a given back end.
 *
 * First array aligns with the field number and the second array matches the
 * Type stack.
 */
size_t vh_htd_tam_calcsize(HeapTupleDef htd);

int32_t vh_htd_tam_depth(HeapTupleDef htd);

HeapField vh_htd_field_by_idx(HeapTupleDef htd, uint16_t field_idx);

/*
 * Null Bit Map
 */
#define vh_htd_nbm_size(htd)		((((htd)->nfields) / 8) + 1)

#endif

