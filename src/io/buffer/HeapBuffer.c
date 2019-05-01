/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/buffer/HeapBuffer.h"
#include "io/buffer/HeapPage.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/utils/kvmap.h"
#include "io/utils/SList.h"

#define HB_BLOCK_PAGE(blk) 	((HeapPage)(((char*)blk) + offsetof(struct BlockData, page)))


typedef struct BlockData
{
	BufferBlockNo blockno;
	uint32_t pins;
	struct BlockData *prev, *next;
	struct HeapPageData page;
} *Block;

static inline HeapTuplePtr hb_allocht(HeapBuffer hb, HeapTupleDef htd, 
		   							  HeapTuple *ht, BufferBlockNo *hint);

static void hb_extend(HeapBuffer hb, uint32_t blocks);
static Block hb_fetch(HeapBuffer hb, BufferBlockNo blockno);
static void hb_insert(HeapBuffer, Block blk);
static void hb_markblock_hot(HeapBuffer hb, Block blk);

/*
 * vh_hb_heaptuple
 *
 * |flags|		
 * 	Should match VH_HT_FLAG, only VH_HT_FLAG_MUTABLE is relevant at the
 * 	moment.  If a users requests the MUTABLE copy, we go all lengths to
 * 	attempt to grab it.
 *
 * |compare|
 * 	Calls vh_ht_compare when the mutable HeapTuple is requested and tells
 * 	it to set the change flags.  This way we don't have to depend on users
 * 	to clear the null flags.
 */
HeapTuple
vh_hb_heaptuple(HeapBuffer hb, HeapTuplePtr htp, uint16_t flags)
{
	BufferBlockNo blockno;
	HeapItemSlot slot;
	Block blk = 0, mutable_blk, immutable_blk;
	HeapPage hp, mutable_hp, immutable_hp;
	HeapTuple ht, mutable_ht, immutable_ht;
	HeapTuplePtr htp_copy;

	blockno = vh_HTP_BLOCKNO(htp);
	slot = vh_HTP_ITEMNO(htp);

	/*
	 * Scan the LRU cache first for the block, if it's there then we won't
	 * have to do a hash lookup.
	 */
	if (hb->lru_first)
	{
		if (hb->lru_first->blockno == blockno)
		{
			blk = hb->lru_first;
		}
	}

	if (!blk)
		blk = hb_fetch(hb, blockno);

	if (blk)
	{
		hp = HB_BLOCK_PAGE(blk);
		ht = (HeapTuple)VH_HP_TUPLE(hp, slot);

		assert(blk->blockno == blockno);

		if (!ht)
			return 0;

		if (flags & VH_HT_FLAG_MUTABLE)
		{
			if (vh_ht_flags(ht) & VH_HT_FLAG_MUTABLE)
			{
				assert(ht->tupcpy);

				return ht;
			}
			else if (ht->tupcpy)
			{
				/*
				 * If the caller has requested the mutable HeapTuple then we'll
				 * need to make another call to hb_fetch if the mutable version
				 * has been created.  If no mutable version exists, we get to make
				 * one here.
				 */

				assert(vh_hb(vh_HTP_BUFF(ht->tupcpy)) ? 
					   vh_hb(vh_HTP_BUFF(ht->tupcpy))->xid ==
					   vh_HTP_XID(ht->tupcpy) : 0);

				mutable_blk = hb_fetch(hb, vh_HTP_BLOCKNO(ht->tupcpy));

				if (mutable_blk)
				{
					mutable_hp = HB_BLOCK_PAGE(mutable_blk);
					mutable_ht = (HeapTuple)VH_HP_TUPLE(mutable_hp, 
														vh_HTP_ITEMNO(ht->tupcpy));

					assert(mutable_blk->blockno == vh_HTP_BLOCKNO(ht->tupcpy));
					assert(vh_ht_flags(mutable_ht) & VH_HT_FLAG_MUTABLE);

					if (flags & VH_HB_HT_FLAG_COMPARE)
					{
						vh_ht_compare(ht, mutable_ht, true);
					}

					return mutable_ht;
				}
			}
			else
			{
				htp_copy = vh_hb_allocht(hb, ht->htd, &mutable_ht);

				if (htp_copy && mutable_ht)
				{
					/*
					 * Refetch the original HeapTuple pointer off the page since
					 * vh_hb_allocht may have invoked a collapse on the page to
					 * insert a new HeapTupleItemPtr.  We should probably teach
					 * vb_hb_allocht to create a new page in this scenario, rather
					 * than risk invalidating someone's HeapTuple address.
					 */
					ht = (HeapTuple)VH_HP_TUPLE(hp, slot);

					assert(mutable_ht->htd == ht->htd);
					assert(vh_HTP_XID(htp) == hb->xid);

					vh_ht_copy(ht, mutable_ht, hb->idx);
					vh_ht_flags(mutable_ht) |= VH_HT_FLAG_MUTABLE;

					mutable_ht->tupcpy = htp;
					ht->tupcpy = htp_copy;

					return mutable_ht;
				}
				else
				{
					elog(ERROR1,
						 emsg("Unable to create a mutable heap tuple in buffer %d",
							  hb->idx));
				}
			}
		}
		else
		{
			/*
			 * Check to make sure we didn't get the mutable HTP before
			 * returning.  The user didn't request it!
			 */

			if (!(vh_ht_flags(ht) & VH_HT_FLAG_MUTABLE))
			{
				return ht;
			}
			else if (ht->tupcpy)
			{
				/*
				 * Looks like caller passed the HeapTuplePtr to the mutable
				 * copy.  Yet they've requested the immutable copy.  Let's
				 * go ahead and grab it, but issue a warning.  There's possible
				 * corruption here, as users shouldn't be grabbing the mutable
				 * HeapTuplePtr.  It's never explicitly passed outside of the
				 * library.
				 */

				assert(vh_hb(vh_HTP_BUFF(ht->tupcpy)) ?
					   vh_hb(vh_HTP_BUFF(ht->tupcpy))->xid ==
					   vh_HTP_XID(ht->tupcpy) : 0);

				immutable_blk = hb_fetch(hb, vh_HTP_BLOCKNO(ht->tupcpy));

				if (immutable_blk)
				{
					assert(immutable_blk->blockno == vh_HTP_BLOCKNO(ht->tupcpy));

					immutable_hp = HB_BLOCK_PAGE(immutable_blk);
					immutable_ht = (HeapTuple)VH_HP_TUPLE(immutable_hp,
		  												  vh_HTP_ITEMNO(ht->tupcpy));

					return immutable_ht;
				}
			}
			else
			{
				elog(ERROR1,
					 emsg("HeapTupePtr corruption detected, the provided "
						  "HeapTuplePtr %d describes itself as the mutable "
						  "copy but doesn't reference an immutable HTP!"));

				return 0;
			}
		}
	}
	else
	{
		/*
		 * If we can fetch from disk without eviction, do it.
		 *
		 * Otherwise we'll have to run the eviction strategy to
		 * free a block.
		 */
	}

	return 0;
}

HeapTuplePtr
vh_hb_allocht(HeapBuffer hb, HeapTupleDef htd,
			  HeapTuple *ht)
{
	return hb_allocht(hb, htd, ht, 0);
}

/*
 * Traverses the freespace map for a Block with enough freespace to construct
 * a HeapTuple.  If there is not enough adequate freespace on blocks in the 
 * buffer, attempt an extension.  If no extension can occur, then run the
 * eviction technique.
 *
 * If we're passed a non-null pointer to a HeapTuple via |ht|, we'll pin the
 * buffer and assign the pointer to the newly allocated ht.
 */
static inline HeapTuplePtr
hb_allocht(HeapBuffer hb,
 		   HeapTupleDef htd,
 		   HeapTuple *ht,
		   BufferBlockNo *hint)
{
	Block blk;
	HeapItemSlot slot;
	HeapTuplePtr htp;
	uint32_t self_calls = 0;

	if (hint)
	{
		/*
		 * Try to allocate another HeapTuple in the same block.
		 */
		blk = hb_fetch(hb, *hint);

		if (blk)
		{
			if (vh_hp_freespaceitm(HB_BLOCK_PAGE(blk)) >= htd->heapasize)
			{
				slot = vh_hp_construct_tup(hb, HB_BLOCK_PAGE(blk), htd);
				htp = vh_HTP_FORM(blk->blockno,
								  hb->xid,
								  hb->idx,
								  slot);
				
				assert(vh_HTP_XID(htp) == hb->xid);
				assert(vh_HTP_BLOCKNO(htp) == blk->blockno);

				hb_markblock_hot(hb, blk);

				if (ht)
				{
					*ht = (HeapTuple)VH_HP_TUPLE(HB_BLOCK_PAGE(blk), slot);
					assert((*ht)->htd == htd);
				}

				return htp;
			}
		}
	}

	if (hb->lru_first)
	{
		if (vh_hp_freespaceitm(HB_BLOCK_PAGE(hb->lru_first)) >=
							   htd->heapasize)
		{
			slot = vh_hp_construct_tup(hb,
	 								   HB_BLOCK_PAGE(hb->lru_first),
	 								   htd);

			htp = vh_HTP_FORM(hb->lru_first->blockno,
							  hb->xid,
							  hb->idx,
							  slot);

			assert(vh_HTP_XID(htp) == hb->xid);
			assert(vh_HTP_BLOCKNO(htp) == hb->lru_first->blockno);

			/*
			 * Let's run thru the LRU from the end and mark
			 * freespace for the category we just used.  We
			 * work from the least used end of the list to attempt
			 * at filling a page densely before it get evicted.
			 */

			hb_markblock_hot(hb, hb->lru_first);
			
			if (ht)
			{
				*ht = (HeapTuple)VH_HP_TUPLE(HB_BLOCK_PAGE(hb->lru_first),
								  slot);
			}

			return htp;
		}
	}

use_freelist:
	blk = hb->free_list;

	if (blk)
	{
		hb->free_list = blk->next;

		blk->next = 0;
		blk->prev = 0;

		blk->pins = 0;
		blk->blockno = ++hb->nblocks;
		
		vh_hp_init(HB_BLOCK_PAGE(blk));
		slot = vh_hp_construct_tup(hb,
		 						  HB_BLOCK_PAGE(blk),
		 						  htd);
		htp = vh_HTP_FORM(blk->blockno,
						  hb->xid,
						  hb->idx,
						  slot);

		assert(vh_HTP_XID(htp) == hb->xid);
		assert(vh_HTP_BLOCKNO(htp) == blk->blockno);

		hb_insert(hb, blk);
		hb_markblock_hot(hb, blk);
		
		if (ht)
		{
			*ht = (HeapTuple)VH_HP_TUPLE(HB_BLOCK_PAGE(blk),
							  slot);
			assert((*ht)->htd == htd);
		}

		return htp;
	}

	/*
	 * If we get to here, we need to extend the relation by one and
	 * then go back to the use_freelist section.
	 */

	if (++self_calls > 5)
		return 0;
	else
		hb_extend(hb, hb->allocfactor);

	goto use_freelist;

	return 0;
}

HeapTuplePtr
vh_hb_allocht_nearby(HeapTuplePtr htp, HeapTupleDef htd,
					 HeapTuple *target)
{
	BufferBlockNo blockno = vh_HTP_BLOCKNO(htp);

	return hb_allocht(vh_hb(vh_HTP_BUFF(htp)), htd, target, &blockno); 
}


/*
 * Copies the HeapTuple pointed to be |source|.  When |source_hint| is provided,
 * we'll go ahead and copy the source_hint without doing a lookup for the HeapBuffer.
 *
 * It's assumed the caller will have pinned source_hint to prevent it from being
 * evicted from the buffer.
 *
 * Users should always use the vh_hb_copyht interface rather than the vh_ht_copy.
 * This way all HeapTuple can be managed by the HeapBuffer.
 *
 * If the user passes a valid dest pointer, we won't unpin the new buffer before
 * exiting the function.  Otherwise, we'll pin the new buffer and then release it
 * before exiting.  It's assumed the user will release the buffer if they
 * explicitly  pass a dest pointer.
 */

HeapTuplePtr
vh_hb_copyht(HeapTuplePtr source, HeapTuple source_hint, 
			 HeapTuple *dest)
{
	HeapTuple ht, newht = 0;
	HeapTupleDef htd = 0;
	HeapTuplePtr htp;

	if (source_hint)
		ht = source_hint;
	else
		ht = vh_htp(source);

	if (ht && source)
	{
		htd = ht->htd;
		htp = vh_hb_allocht(vh_hb(vh_HTP_BUFF(source)), htd, &newht);

		if (htp)
		{
			assert(vh_HTP_BUFF(htp) == vh_HTP_BUFF(source));
			assert(vh_HTP_XID(htp) == vh_HTP_XID(source));

			if (vh_ht_copy(ht, newht, vh_HTP_BUFF(source)))
			{
				newht->tupcpy = 0;	
				vh_ht_flags(newht) &= ~VH_HT_FLAG_MUTABLE;

				if (dest)
					*dest = newht;

				return htp;
			}
			else
			{
				//vh_hb_freehtp(htp);
			}
		}	
	}

	return 0;
}

HeapTuplePtr
vh_hb_copyht_nearby(HeapTuplePtr source, HeapTuple source_hint, 
					HeapTuple *dest)
{

	return 0;
}

/*
 * If the page isn't pinned, we'll want to free the tuple and rewrite the
 * page so all the remaining items are at the end of the page.  This optimizes
 * the free space available on the page.  If the entire page is empty by this
 * action, then we'll just jam it into the freelist for the buffer.  Since
 * pages are allocated in blocks, we cannot release the underlying memory back
 * to the operating system without additional infrastructure.
 */
void
vh_hb_free(HeapBuffer hb,
	  	   BufferBlockNo block,
  		   HeapItemSlot hidx)
{
	Block blk;
	HeapPage hp;
	HeapTuple ht;
	HeapTuplePtr htp_copy = 0;

	if (block > hb->nblocks)
	{
		elog(ERROR2,
			 emsg("Invalid block number %d requested.  "
				  "Only %d blocks exist in HeapBuffer!",
				  block,
				  hb->nblocks));
		return;
	}

	blk = hb_fetch(hb, block);

	if (blk)
	{
		hp = HB_BLOCK_PAGE(blk);

		if (hp)
		{
			ht = (HeapTuple)VH_HP_TUPLE(hp, hidx);

			/*
			 * Check to see if we have a tuple copy and store it so we
			 * can free it afterwards.
			 */

			if (ht && ht->tupcpy)
				htp_copy = ht->tupcpy;

			vh_hp_freetup(hp, hidx);

			if (htp_copy)
			{
				blk = hb_fetch(vh_hb(vh_HTP_BUFF(htp_copy)), 
							   vh_HTP_BLOCKNO(htp_copy));

				if (blk)
				{
					hp = HB_BLOCK_PAGE(blk);
					vh_hp_freetup(hp, vh_HTP_ITEMNO(htp_copy));
				}	
			}

		}
	}
	else
	{
		elog(ERROR1, 
			 emsg("Unable to locate block %d as requested!",
				  block));
	}

	return;
}

/*
 * Preallocates enough blocks to accomodate |tups| of |htd|.  Pre-allocations
 * do not affect the freespace map or the |blocks| lookup table until the block
 * is used for atleast one HeapTuple.  vh_hb_allocht has been taught to check the
 * freespace map and then the empty list.  This way if we're sequentially forming
 * HeapTuple from a back end query, the chances of maintaining order in the buffer
 * is higher.
 */
void
vh_hb_prealloc(HeapBuffer hb,
			   HeapTupleDef htd,
			   uint32_t tups)
{
}

void
vh_hb_destroyblktbl(HeapBuffer hb)
{
	if (hb && hb->blocks)
		vh_kvmap_destroy(hb->blocks);
}

/*
 * Prints basic statistics about the state of the HeapBuffer.
 */
void
vh_hb_printstats(HeapBuffer hb)
{
	printf("\nHeapBuffer %d statistics:\n\t"
		   "Number of blocks allocated:\t%d\n\t"
		   "Number of free buffers:\t%d\n",
		   hb->idx,
		   hb->nblocks,
		   0);
}


/*
 * Marks the block as hot by moving it to the top of the LRU list.  If we need
 * to evict pages from the buffer, we take the least recently used.
 */
static void 
hb_markblock_hot(HeapBuffer hb, Block blk)
{
	if (blk == hb->lru_first)
		return;

	if (!hb->lru_first)
	{
		hb->lru_first = hb->lru_last = blk;
		blk->prev = 0;
		blk->next = 0;
		
		return;
	}
	else
	{
		hb->lru_first->prev = blk;
		hb->lru_first = blk;
	}

	if (blk == hb->lru_last)
		hb->lru_last = blk->prev;

	if (blk->prev)
		blk->prev->next = blk->next;

	if (blk->next)
		blk->next->prev = blk->prev;
}

static void 
hb_extend(HeapBuffer hb, uint32_t blocks)
{
	const uint32_t advsz = (sizeof(struct BlockData) +
		(VH_HEAPPAGE_SIZE - sizeof(struct HeapPageData)));
	char* blk;
	Block blki, blkp = 0;
	int32_t i = 0;


	if (blocks > 0)
	{
		blk = (char*) vhmalloc_ctx(hb->mctx, advsz * blocks);

		if (blk)
		{
			memset(blk, 0, advsz * blocks);

			blki = (Block)blk;
			blkp = 0;

			for (i = 0; i < blocks; i++)
			{
				if (blkp)
					blki->prev = blkp;
				else
					blki->prev = 0;

				blkp = blki;
				blki = (Block)(((char*)blki) + advsz);
				
				blkp->next = blki;
			}

			if (blkp)
				blkp->next = 0;

			hb->free_list = (Block)blk;
		}
	}
}

/*
 * The goal is to get the page containing the desired HeapTuple
 * into the buffer.  First we check the lookup table to see if
 * the block number is already available on the heap.  If it hasn't
 * been evicted, the pavl_table will return a Block pointer.  The page
 * can be accessed 

 * In the event the block cannot be found on the heap, we should first
 * determine if additional blocks can be added to the buffer.  If so,
 * add a new block and go to disk for the requested block.  If no
 * more blocks can be added, we must first gracefully evict an existing
 * block and refill it from the desired block on the disk.
 *
 * The |lkp| table on the HeapBuffer tracks all blocks that are available
 * on the Heap and the corresponding slot in the |pages| array.
 */
static Block
hb_fetch(HeapBuffer hb, BufferBlockNo blockno)
{
	Block *blk;

	if (blockno > hb->nblocks)
	{
		elog(ERROR2,
			 emsg("Invalid block number %d requested.  "
				  "Only %d blocks exist in HeapBuffer %d!",
				  blockno,
				  hb->nblocks,
				  hb->idx));
		return 0;
	}


	blk = vh_kvmap_find(hb->blocks, &blockno);

	if (blk)
	{
		hb_markblock_hot(hb, *blk);

		return *blk;
	}
	else
	{
		/*
		 * Eventually, we'll teach this to go to disk and run the eviction
		 * routine if necessary to bring the block into memory.
		 */
	}

	return 0;
}

static void
hb_insert(HeapBuffer hb, Block blk)
{
	Block *ptr;

	if (!vh_kvmap_value(hb->blocks, &blk->blockno, ptr))
	{
		*ptr = blk;
	}
	else
	{
		elog(ERROR2,
				emsg("Block number %d already exists in HeapBuffer %d.  "
					 "The HeapBuffer is corrupt.",
					 blk->blockno,
					 hb->idx));
	}
}

