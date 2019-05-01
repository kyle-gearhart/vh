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

#define VH_HT_FlagsSet(ht, fgs)		(vh_ht_flags(ht) |= (fgs))
#define VH_HT_FlagsClear(ht, fgs)	(vh_ht_flags(ht) &= ~(fgs))

#define VH_HT_FieldFlagsClear(ht, hf, flags)	(vh_htf_flags(ht, hf) &= \
												 (~(flags)))
#define VH_HT_FieldFlagsSet(ht, hf, flags)		(vh_htf_flags(ht, hf) |= \
												 (flags))

/*
 * Compares each field on the heap tuple, must ensure the
 * lhs and rhs come from the identical HeapTupleDef pointer
 * before proceeding, otherwise this function will return
 * a -2.
 *
 * If the |track| parameter is set, the rhs CHANGED flag
 * for each field will be set when the comparison function
 * is not equal.  Evaluates nulls like the SQL standard, if
 * one side is null, the other side must be null for both to
 * be equal.  In the event the |track| flag is set, all fields
 * will be evaluated.  If there are differences between the same
 * fields on different tuples, then we will set the changed
 * flag on the |rhs| tuple.  This allows callers to quickly
 * determine what fields are different on the entire tuple.
 *
 * When it is not set, as soon as it's 
 * determined the HeapTuples cannot have similar data, the
 * function will return.
 *
 * |lhs| is assumed to be the immutable copy.
 */
int32_t 
vh_ht_compare(HeapTuple lhs, HeapTuple rhs, bool track)
{
	int32_t tcomp, fcomp;
	HeapField *hf_head, hf;
	uint32_t i, sz;
	void *lval, *rval;
	bool lnull, rnull;

	if (lhs->htd != rhs->htd)		
		return -2;

	tcomp = 0;
	fcomp = 0;

	sz = vh_SListIterator(lhs->htd->fields, hf_head);

	for (i = 0; i < sz; i++)
	{
		hf = hf_head[i];
		
		lval = vh_ht_field(lhs, hf);
		rval = vh_ht_field(rhs, hf);
		
		lnull = vh_htf_isnull(lhs, hf);
		rnull = vh_htf_isnull(rhs, hf);
			
		fcomp = vh_hf_tom_comp(hf, lval, rval);

		if (!lnull && !rnull)
		{		
			if (fcomp)
			{
				if (track)
				{
					VH_HT_FieldFlagsSet(rhs, hf, VH_HTF_FLAG_CHANGED);
					tcomp = (!tcomp ? fcomp : tcomp);
				}
				else
					return fcomp;
			}
			else
			{
				if (track)
					VH_HT_FieldFlagsClear(rhs, hf, VH_HTF_FLAG_CHANGED);
			}
		}
		else if (lnull && rnull)
		{
			if (fcomp)
			{
				vh_htf_clearnull(rhs, hf);

				if (track)
					VH_HT_FieldFlagsSet(rhs, hf, VH_HTF_FLAG_CHANGED);
			}
		}
		else if (lnull && !rnull)
		{
			if (fcomp && track)
				VH_HT_FieldFlagsSet(rhs, hf, VH_HTF_FLAG_CHANGED);

		}
		else if (!lnull && rnull)
		{
			if (track)
			{
				VH_HT_FieldFlagsSet(rhs, hf, VH_HTF_FLAG_CHANGED);
				tcomp = (!tcomp ? -1 : tcomp);
			}
			else
				return (vh_htf_isnull(lhs, hf)) ? -1 : 1;
		}
	}

	return tcomp;
}

HeapTuple
vh_ht_create(HeapTupleDef htd)
{
	HeapTuple ht;

	ht = vhmalloc(htd->heapsize);
	ht->htd = htd;
	ht->tupcpy = 0;

	vh_ht_construct(htd, ht, 0);

	return ht;	
}

/*
 * Iterate all of the fields on the HeapTupleDef and call their constructor
 * if the Type indicates to do so.  We make assumptions the HeapTuple passed 
 * in was already zero-ed out by the caller at some point.  We do make some 
 * attempt to set a few of the flags properly.
 */
HeapTuple
vh_ht_construct(HeapTupleDef htd, HeapTuple ht, HeapBufferNo hbno)
{
	HeapField *hf_head, hf;
	void *field;
	uint32_t i, sz;

	ht->htd = htd;
	ht->shard = 0;
	ht->tupcpy = 0;
	ht->flags[0] = 0;

	sz = vh_SListIterator(htd->fields, hf_head);

	for (i = 0; i < sz; i++)
	{
		hf = hf_head[i];
		field = vh_ht_field(ht, hf);

		VH_HT_FieldFlagsSet(ht, hf, 
							VH_HTF_FLAG_NULL |
							VH_HTF_FLAG_CONSTRUCTED);

		if (hf->hasvarlen)
		{
			if (hf->type_depth == 1)
			{
				struct vhvarlenmpad* vl = field;
				vl->hbno = hbno;
			}
			else
			{
				vh_hf_tom_construct(hf, field, hbno);
			}
		}
		else if (hf->hasconstructor)
		{
			vh_hf_tom_construct(hf, field, hbno);
		}
	}

	return ht;
}

/*
 * Calls the destructor if it's present for each field on the HeapTuple.  After
 * the destructor has been called for each HeapTuple instance, it can be free-d 
 * using the standard vhfree function call.
 *
 * It's possible the underlying types for fields on the HeapTuple natively move 
 * data out-of-line for the HeapTuple data area.  Calling the destructor for 
 * each field ensures those out-of-line memory allocations are free-d prior to
 * loosing the pointer to them.
 *
 * Zero out the space occupied by the HeapTuple.
 */
void
vh_ht_destruct(HeapTuple ht)
{
	HeapTupleDef htd;
	HeapField *hf_head, hf;
	uint8_t flags;
	uint32_t i, sz;
	void *field;

	htd = ht->htd;

	sz = vh_SListIterator(htd->fields, hf_head);

	for (i = 0; i < sz; i++)
	{
		hf = hf_head[i];
		field = vh_ht_field(ht, hf);
		flags = vh_htf_flags(ht, hf);

		if (hf->hasdestructor && (flags & VH_HT_FLAG_CONSTRUCTED))
			vh_hf_tom_destruct(hf, field);
	}

	memset(ht, 0xf, htd->heapasize);
}


/*
 * Commits all of the mutable values to back to the immutable HeapTuple.  This 
 * does not perform operations against the backend and it will reset the change
 * flags.
 *
 * We count on the caller using the HeapBuffer to release the mutable copy once
 * it has updated the immutable copy.
 *
 * |ht_m| 	Mutable HeapTuple
 * |ht_im|	Immutable HeapTuple
 */
bool
vh_ht_commit(HeapTuple ht_m, HeapTuple ht_im)
{
	HeapTupleDef htd;
	HeapField hf, *hf_head;
	uint32_t i, hf_sz;
	void *lfield, *rfield;

	assert(ht_m);
	assert(ht_im);
	assert(ht_m->htd == ht_im->htd);

	htd = ht_m->htd;

	hf_sz = vh_SListIterator(htd->fields, hf_head);

	for (i = 0; i < hf_sz; i++)
	{
		hf = hf_head[i];

		lfield = vh_ht_field(ht_m, hf);
		rfield = vh_ht_field(ht_im, hf);
		
		if ((vh_htf_isnull(ht_m, hf) &&
			vh_htf_isnull(ht_im, hf)))
		{

			/*
			 * Just because the corresponding fields are flagged a null
			 * on both the static copy and the working copy doesn't mean
			 * the working copy remains null.  If a user explicitly sets
			 * a field value on the working copy, we'll assume it's no
			 * longer considered null.  Run the comparison to see if the
			 * field values are different.  When they are, clear the null
			 * flag and copy to the static.
			 */

			if (vh_hf_tom_comp(hf, lfield, rfield))
			{
				VH_HT_FieldFlagsClear(ht_im,
									  hf,
									  VH_HTF_FLAG_NULL);

				vh_tam_fireh_memset_set(hf, lfield, rfield, true);
			}
		}
		else
		{
			if (vh_htf_isnull(ht_m, hf))
			{
				/*
				 * In the null case on the transient copy:
				 * Set the null flag on the static copy and deconstruct the
				 * static field if it had been previously constructed.
				 */

				if (vh_htf_isconstructed(ht_im, hf))
				{
					if (hf->hasdestructor)
					{
						vh_hf_tom_destruct(hf, rfield); 

						if (hf->hasconstructor)
							vh_hf_tom_construct(hf, rfield, 0);
					}
				}

				VH_HT_FieldFlagsSet(ht_im, hf, VH_HTF_FLAG_NULL);
			}
			else
			{
				/*
				 * Non-null case on the transient copy:
				 * Copy the transient value to the static copy.
				 */
				vh_tam_fireh_memset_set(hf, lfield, rfield, true);
			}
			
			VH_HT_FieldFlagsClear(ht_im, hf, VH_HTF_FLAG_CHANGED);
		}
	}

	return true;
}

bool
vh_ht_copy(HeapTuple source, HeapTuple target, HeapBufferNo hbno)
{
	HeapTupleDef htd;
	HeapField *hf_head, hf;
	uint32_t i, sz;
	uint8_t sflags;
	void *sfield, *tfield;

	htd = source->htd;
	vh_ht_flags(target) = vh_ht_flags(source);

	sz = vh_SListIterator(htd->fields, hf_head);

	for (i = 0; i < sz; i++)
	{
		hf = hf_head[i];

		sflags = vh_htf_flags(source, hf);
		sfield = vh_ht_field(source, hf);
		tfield = vh_ht_field(target, hf);

		/*
		 * Copy the flags and then call the memset access
		 * method to deep copy the values on each field
		 */

		vh_htf_flags(target, hf) = sflags;

		if (sflags & VH_HTF_FLAG_NULL &&
			vh_htf_flags(target, hf) & ~VH_HTF_FLAG_CONSTRUCTED)
		{
			/*
			 * Source field is null, so just set the construct flag and
			 * handle the variable length fields.  Call the constructor
			 * if present.
			 */

			VH_HT_FieldFlagsSet(target, hf, VH_HTF_FLAG_CONSTRUCTED);

			if (hf->hasconstructor)
				vh_hf_tom_construct(hf, tfield, hbno);
		}
		else
		{
			vh_tam_fireh_memset_set(hf, sfield, tfield, true);
		}
	}

	return true;
}

HeapTuple
vh_ht_Create(HeapTupleDef htd)
{
	CatalogContext cc = vh_ctx();
	HeapTuple ht;
	HeapField hf, *hf_head;
	uint32_t hf_sz, i;
	void *field;
	HeapBufferNo hbno;

	if (cc)
	{
		hbno = cc->hbno_general;
		ht = (HeapTuple)vhmalloc_ctx(vh_hb_memoryctx(hbno), htd->heapasize);
	}
	else
	{
		ht = (HeapTuple)vhmalloc(htd->heapasize);
	}

	memset(ht, 0, htd->heapasize);

	ht->htd = htd;

	hf_sz = vh_SListIterator(htd->fields, hf_head);

	for (i = 0; i < hf_sz; i++)
	{
		hf = hf_head[i];
		field = vh_ht_field(ht, hf);
		
		if (hf->hasconstructor)
		{
			vh_hf_tom_construct(hf, field, hbno);

			VH_HT_FieldFlagsSet(ht,
								hf,
								VH_HTF_FLAG_CONSTRUCTED | VH_HTF_FLAG_NULL);
		}
		else
		{
			VH_HT_FieldFlagsSet(ht,
								hf,
								VH_HTF_FLAG_NULL);
		}
	}

	return ht;
}

void
vh_ht_set(HeapTuple ht, HeapField hf, void *value)
{
	void *tvalue;

	if (vh_ht_flags(ht) & ~VH_HT_FLAG_MUTABLE)
	{
		elog(WARNING,
			 emsg("vh_ht_set called with an immutable HeapTuple, changes will "
				  "be ignored by the backend!"));
	}

	/*
	 * We assume the caller isn't trying to fill a null value and thus clear
	 * the flag.
	 */

	tvalue = vh_ht_field(ht, hf);
	vh_tam_fireh_memset_set(hf, value, tvalue, true);
	vh_htf_clearnull(ht, hf);
}

/*
 * vh_ht_formkey
 *
 * Fills the |buffer| using the binary_get TAM.  If there isn't enough space
 * in the buffer then we abort the fill and return false.
 */
size_t
vh_ht_formkey(unsigned char *buffer, size_t buffer_sz,
			  HeapTuple ht, HeapField *hfs, uint32_t nhfs)
{
	struct BinaryAMOptionData bopts = { .malloc = false,
		.sourceBigEndian = false, .targetBigEndian = false };

	unsigned char *bufferc = buffer, *buffer_end = buffer + buffer_sz;
	size_t buffer_len, buffer_cursor, bytes = 0;
	uint32_t i;
	bool filled = true;

	for (i = 0; i < nhfs; i++)
	{
		if (bytes >= buffer_sz)
		{
			filled = false;
			break;
		}

		if (vh_htf_isnull(ht, hfs[i]))
			continue;

		buffer_len = (size_t)(buffer_end - bufferc);
		buffer_cursor = 0;

		vh_tam_fire_bin_get(&hfs[i]->types[0], &bopts, 
							vh_ht_field(ht, hfs[i]),
							bufferc, &buffer_len, &buffer_cursor);

		if (buffer_len == buffer_cursor)
		{
			bufferc += buffer_cursor;
			bytes += buffer_cursor;	
		}
		else
		{
			filled = false;
			break;
		}
	}

	if (filled)
		return bytes;

	return 0;
}

/*
 * vh_ht_nullbitmap
 *
 * Form a bitmap of the null values on a given HeapTuple.  If the bit is set,
 * the field is null on the HeapTuple.
 *
 * Returns true if there was enough space in the caller passed bitmap array
 * to set the values.
 */
bool
vh_ht_nullbitmap(HeapTuple ht, char *bitmap, size_t sz)
{
	HeapField *hf_head, hf;
	int32_t i, hf_sz;
	
	assert(ht);
	assert(bitmap);

	if (sz < vh_ht_nbm_size(ht))
		return false;

	hf_sz = vh_SListIterator(ht->htd->fields, hf_head);

	for (i = 0; i < hf_sz; i++)
	{
		hf = hf_head[i];

		if (vh_htf_isnull(ht, hf))
			vh_ht_nbm_setnull(bitmap, i);
		else
			vh_ht_nbm_clearnull(bitmap, i);
	}

	return true;
}

size_t
vh_htf_tostring(HeapTuple ht, HeapField hf,
				char *buffer, size_t buffer_sz)
{
	static const struct CStrAMOptionsData cstr = { .malloc = false };

	size_t cursor, length;
	void *fmts[VH_TAMS_MAX_DEPTH] = { 0, 0, 0, 0, 0, 0, 0, 0 };

	length = buffer_sz;
	cursor = 0;

	if (vh_htf_isnull(ht, hf))
	{
		if (buffer_sz > 0)
			*buffer = '\0';
	}
	else
	{
		vh_tam_fire_cstr_get(hf->types,
							 &cstr,
							 vh_ht_field(ht, hf),
							 buffer,
							 &length,
							 &cursor,
							 fmts);

		return length;
	}

	return 0;
}

