/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/catalog/Type.h"
#include "io/utils/SList.h"




static void HTD_AddPK(HeapTupleDef htd,
					  HeapField hf);

size_t 
vh_htd_tam_calcsize(HeapTupleDef htd)
{
	size_t sz = 0;
	HeapField *hf_head, hf;
	uint32_t hf_sz, i;

	hf_sz = vh_SListIterator(htd->fields, hf_head);
	for (i = 0; i < hf_sz; i++)
	{
		hf = hf_head[i];
		sz += sizeof(HeapField);
		sz += sizeof(Type) * (hf->type_depth + 1);
	}

	return sz;
}

int32_t
vh_htd_tam_depth(HeapTupleDef htd)
{
	int32_t depth = 0, hf_sz, i;
	HeapField *hf_head;

	hf_sz = vh_SListIterator(htd->fields, hf_head);
	
	for (i = 0; i < hf_sz; i++)
		depth += hf_head[i]->type_depth;

	return depth;
}

/*
 * vh_htd_add_extra
 *
 * Adds a non-datatype field the HeapTupleDef, at the end of the data segment
 * for the fields.  This allows for us to map in "extras" such as a relationship
 * pointer directly on the HeapTuple itself.  Keep in mind these extras are
 * system wide.
 *
 * We get to have some fun here and take into account alignment.  If the user
 * calls with 8 bytes or clean multiple of 8, we're going to put it on an 8 byte 
 * boundary.  We'll test 4 bytes, then 2 bytes until we can detect alignment.
 *
 * We'll retun the number of bytes from the start of a HeapTuple instance where
 * this extra data will begin.  The caller will need to keep track of this,
 * we're not going to save each individual "extra" field's offset!
 *
 * vh_htd_add_field will need to be taught how to align and re-align if extras
 * are present and then a subsequent field is added!
 */
uint32_t
vh_htd_add_extra(HeapTupleDef htd, uint32_t bytes)
{
	return 0;
}

bool
vh_htd_add_field(HeapTupleDef htd, HeapField hf)
{
	HeapField *hf_head, hf_i;
	uint32_t hfsz, hf_prior_pad, nhdrpad;
	int32_t delta_hdrpad;
	size_t hft_width, hft_maxalign, hfi_width;

	hf_prior_pad = 0;

	hfsz = vh_SListIterator(htd->fields, hf_head);

	if (hf->types[0])
	{
		hf->heapord = htd->nfields;
		hf->dord = htd->nfields;
		hf->offset = 0;
		
		hft_width = vh_type_stack_data_width(&hf->types[0]);
		assert(hft_width);

		hf->maxalign = vh_type_stack_data_maxalign(&hf->types[0]);

		if (hfsz)
		{
			hf_i = hf_head[hfsz-1];
			hfi_width = vh_type_stack_data_width(&hf_i->types[0]);
			hft_maxalign = vh_type_stack_data_maxalign(&hf_i->types[0]);

			if ((hf_i->offset + hft_width) % hft_maxalign)
			{
				hf_i->padding = hft_maxalign - ((hf_i->offset + hft_width) % hft_maxalign);
				hf_prior_pad = hf_i->padding;
			}
			
			hf->offset = hf_i->offset + hfi_width + hf_prior_pad;
		}

		htd->nfields++;

		/*
		 * We want to align the top of the actual data structure on an 8-byte
		 * alignment, so we pad the top of the header after the flags.  To do
		 * this properly, we should calculate a padding delta from what already
		 * exists versus what the added field now requires (since a new field
		 * inherently adds another byte of flags).  This way we can simply add
		 * the delta back into the overall htd->heapsize value.
		 */
		nhdrpad = (htd->nfields % 8 ? 8 - (htd->nfields % 8) : 0);
		delta_hdrpad = nhdrpad - htd->hdrpad;
		htd->hdrpad = nhdrpad;

		htd->tupsize += hft_width;
		htd->heapsize += hf_prior_pad + hft_width + sizeof(uint8_t) + 
						 delta_hdrpad;
		htd->tupoffset = vh_htd_tuple_offset(htd);

		/*
		 * Calculate the aligned sizes, the tupasize doesn't really matter
		 * because we never use it for allocation.  It's more of a sanity
		 * check to ensure our alignment logic is correct.
		 */
		htd->tupasize = hf->offset + hft_width;		
		htd->heapasize = htd->heapsize + (htd->heapsize % 8 ? (8 - (htd->heapsize % 8)) : 0);
		
		hf->hasvarlen = vh_type_stack_has_varlen(vh_hf_type_stack(hf));
		hf->hasconstructor = hf->hasdestructor = hf->hasvarlen;

		vh_SListPush(htd->fields, hf);
		vh_SListPush(htd->type_stack, &hf->types[0]);

		return true;
	}

	return false;
}

HeapTupleDef
vh_htd_create(size_t size)
{
	HeapTupleDef htd;

	assert(size >= sizeof(HeapTupleDefData));

	htd = (HeapTupleDef)
		vhmalloc(sizeof(size));

	vh_htd_init(htd);

	return htd;
}

void
vh_htd_init(HeapTupleDef htd)
{
	memset(htd, 0, sizeof(HeapTupleDefData));

	htd->heapsize = sizeof(HeapTupleData);
	htd->fields = vh_SListCreate();
	htd->type_stack = vh_SListCreate();
}

void
vh_htd_finalize(HeapTupleDef htd, 
				void (*for_each_field)(HeapTupleDef, void*))
{
	HeapField hf, *hf_head;
	int32_t i, hf_sz;

	hf_sz = vh_SListIterator(htd->fields, hf_head);
	assert(hf_sz == htd->nfields);

	for (i = 0; i < hf_sz; i++)
	{
		hf = hf_head[i];

		if (for_each_field)
			for_each_field(htd, hf);

		vhfree(hf);
	}

	vh_SListFinalize(htd->fields);
	vhfree(htd->fields);

	memset(htd, 0x0f, sizeof(HeapTupleDefData));
}

/*
 * We call this so much it doesn't make sense to build it on the fly each time.
 *
 * When a HeapField gets added, we just push the address of the HeapField
 * |types| member onto an SList.  Then we just take the iterator to the SList
 * to get the start of our array of arrays of pointers to TypeData.
 */
Type**
vh_htd_type_stack(HeapTupleDef htd)
{
	Type **ty_head;
	uint32_t sz;

	sz = vh_SListIterator(htd->type_stack, ty_head);

	assert (sz == htd->nfields);

	return ty_head;
}


/*
 * vh_htd_field_by_idx
 *
 * Return the HeapField by index number.
 */
HeapField
vh_htd_field_by_idx(HeapTupleDef htd, uint16_t field_idx)
{
	HeapField *hfs;
	int32_t hfs_sz;

	if (htd)
	{
		hfs_sz = vh_SListIterator(htd->fields, hfs);
		
		if (field_idx >= 0 &&
			field_idx < hfs_sz)
		{
			return hfs[field_idx];
		}
	}

	return 0;
}

