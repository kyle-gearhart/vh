/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/buffer/HeapBuffer.h"
#include "io/buffer/HeapPage.h"
#include "io/buffer/ItemPtr.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/catalog/HeapTuple.h"


void
vh_hp_init(HeapPage hp)
{
	hp->pins = 0;
	hp->d_fupper = VH_HEAPPAGE_SIZE;
	hp->d_flower = offsetof(HeapPageData, items);
	hp->d_freespace = VH_HEAPPAGE_SIZE - sizeof(HeapPageData);
	hp->n_items = 0;
}

HeapItemSlot
vh_hp_construct_tup(HeapBuffer hb,
					HeapPage hp,
					HeapTupleDef htd)
{
	HeapItemSlot slot;
	HeapTuple htat;
	char *ptr;

	if (vh_hp_freespaceitm(hp) >= htd->heapasize)
	{
		/*
		 * Check to see if we need to reclaim freespace on the page.
		 */
		if (hp->d_fupper - hp->d_flower < htd->heapasize + sizeof(HeapItemPtrData))
			vh_hp_collapse_empty(hp);

		ptr = (char*) hp;
		hp->d_flower += sizeof(HeapItemPtrData);
		hp->d_fupper -= htd->heapasize;

		assert(hp->d_flower <= hp->d_fupper);

		hp->d_freespace -= (htd->heapasize + sizeof(HeapItemPtrData));
		
		slot = hp->n_items++;
		assert(slot < 256);

		hp->items[slot].length = htd->heapasize;
		hp->items[slot].offset = hp->d_fupper;
		hp->items[slot].empty = 0;
		
		htat = (HeapTuple)(ptr + hp->d_fupper);
		
		//memset(htat, 0, htd->heapasize);
		vh_ht_construct(htd, htat, hb->idx);

		return slot;
	}
	else
	{
		elog(ERROR1,
			 emsg("Page no longer contains enough space to accomodate "
				  "a new HeapTuple structure of size %d!",
				  htd->heapasize));
	}

	return 0;
}



/*
 * Desconstructs a HeapTuple at a given item number on a page.  If
 * the deconstruction on this item pointer results in the page being
 * empty, the |empty| flag will be set to true if the caller provides
 * one.
 * 
 */
void
vh_hp_freetup(HeapPage hp,
			  HeapItemSlot hidx)
{
	HeapTuple ht;
	HeapTupleDef htd;
	HeapItemPtr hip;

	if (hidx <= hp->n_items)
	{
		ht = (HeapTuple)VH_HP_TUPLE(hp, hidx);
		hip = &hp->items[hidx];
		htd = ht->htd;

		if (!hip->empty)
		{
			vh_ht_destruct(ht);
			memset(ht, 0, htd->heapsize);

			hip->empty = 1;
			hp->d_freespace += hip->length;
			vh_hp_setdirty(hp);	
		}
	}
	else
	{
		elog(ERROR1,
			 emsg("Invalid HeapItemPtr provided!  Only %d items "
				  "exist on the page",
				  hp->n_items));
	}
}

void
vh_hp_collapse_empty(HeapPage hp)
{
	HeapItemSlot i, j;
	HeapItemPtr hip;
	uint16_t upper, length, offset;

	if (!vh_hp_isdirty(hp))
		return;

	for (i = 0; i < hp->n_items; i++)
	{
		hip = &hp->items[i];

		if (!hip->empty)
			continue;
		else if (hip->empty && hip->length == 0)
			continue;

		upper = hp->d_fupper;
		length = hip->length;
		offset = hip->offset;

		assert(offset >= upper);

		hp->d_fupper += length;

		/*
		 * Special consideration needs to be paid to the underlying data types.
		 * If they contain pointers within themselves, rather than offsets,
		 * we get in trouble because we do not know how to adjust those after
		 * the memmove gets called.
		 *
		 * It took a while to track this down, but in a past life our string
		 * implementation contained a buffer data member that was a char pointer
		 * that always got set whether the actual string data was small enough to
		 * store inline or if it needed to be moved out into allocated memory.
		 *
		 * Of course when the memmove below fired, the pointer to the inline 
		 * string got copied.  So we corrected the string implementation to
		 * derive a pointer on the fly.  Either from the inline char array or
		 * the allocated memory depending on its internal state.
		 *
		 * Lesson of the day: types must use offsets if they are to reference
		 * addresses within themselves!
		 */

		memmove(((char*)hp) + upper + length,
				((char*)hp) + upper,
				offset - upper);

		hip->empty = 1;
		hip->length = 0;

		for (j = i + 1; j < hp->n_items; j++)
		{
			hip = &hp->items[j];

			if (hip->empty && hip->length == 0)
				continue;

			hip->offset += length;
		}
	}

	vh_hp_cleardirty(hp);
}

