/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/utils/mmgr/Pool.h"

/*
 * MemoryBlocks are allocated directly by malloc.  MemoryChunkHeader are
 * allocated out of MemoryBlocks by the pool.  When MemoryChunkHeaders are
 * returned, we determine if they should on the freelist or if they should
 * be directly freed.
 *
 * If they go on the free list, we place a MemoryPoolChunk on the free list
 * in a single link list for the size category.
 */

typedef struct MemoryBlockData *MemoryBlock;
typedef struct MemoryPoolChunkData *MemoryPoolChunk;
typedef struct MemoryChunkHeaderData MemoryChunkHeaderData;

struct MemoryBlockData
{
	MemoryBlock next;
};

#define MPC_MAX_FREELIST	10

typedef struct MemoryPoolData
{
	MemoryContextData header;
	MemoryBlock block;
	size_t blockSize;
	MemoryPoolChunk freelist[MPC_MAX_FREELIST];
} MemoryPoolData, *MemoryPool;

struct MemoryPoolChunkData
{
	struct MemoryPoolChunkData *next;
	size_t size;
};

static void* MemPool_Alloc(void* context, size_t size);
static void MemPool_Destroy(void* context);
static void MemPool_Free(void* context, void *ptr);
static void* MemPool_ReAlloc(void* context, 
							   void* pointer,
							   size_t size);


static MemoryContextOpsTable MemoryPoolOpsTable = {
	MemPool_Alloc,
	MemPool_ReAlloc,
	MemPool_Free,
	MemPool_Destroy
};

#define VH_MALLOC_BLOCKSZ(req)	(sizeof(struct MemoryBlockData) + \
								 req)

#define pool_freelist_category(pool, size)	(((size) / ((pool)->blockSize / MPC_MAX_FREELIST)))

MemoryContext 
vh_MemoryPoolCreate(MemoryContext parent, 
					uint32_t blockSize, 
					const char* name)
{
	MemoryContext mctx;
	MemoryPool mempool;
	MemoryPoolChunk mpc;
	MemoryBlock block;
	size_t allocblock;
	uint32_t i;

	assert(blockSize > 0);

	mempool = (MemoryPool)vh_mctx_create(parent,
												 sizeof(MemoryPoolData),
												 &MemoryPoolOpsTable,
												 name);

#ifdef VH_MMGR_DEBUG
	printf("vh_MemoryPoolCreate: %s\n",
		   name);
#endif

	mctx = (MemoryContext)mempool;
	mctx->tag = MT_Pool;

	mempool->blockSize = blockSize;

	allocblock = blockSize;

	block = malloc(allocblock);
	block->next = 0;

	mpc = (MemoryPoolChunk)(block + 1);
	mpc->size = allocblock - sizeof(struct MemoryBlockData);
	mpc->next = 0;

	mctx->stats.freespace = allocblock;
	mctx->stats.blocks++;

	for (i = 0; i < MPC_MAX_FREELIST; i++)
		mempool->freelist[i] = 0;

	/*
	 * Put the newly allocated block at the back of the free list.
	 */
	mempool->freelist[9] = mpc;
	mempool->block = block;

	return mctx;
}

static void* 
MemPool_Alloc(void* context, size_t size)
{
	MemoryPool pool = context;
	MemoryBlock block;
	MemoryChunkHeader chunk;
	MemoryPoolChunk mpc, mpce, mpcep, mpc_chain;
	size_t requestedsize, align_delta, allocsz;
	int32_t freelist_cat, new_freelist_cat;
	
	requestedsize = size + sizeof(MemoryChunkHeaderData);
	align_delta = sizeof(uintptr_t) - (requestedsize % sizeof(uintptr_t));
	allocsz = requestedsize + (align_delta == sizeof(uintptr_t) ? 0 : align_delta);

	assert(allocsz % VHB_SIZEOF_VOID == 0);

	block = pool->block;

	if (allocsz < pool->blockSize - sizeof(struct MemoryBlockData))
	{
		mpc = 0;
			
		freelist_cat = pool_freelist_category(pool, allocsz);
		assert(freelist_cat < MPC_MAX_FREELIST);

		while (freelist_cat < MPC_MAX_FREELIST)
		{
			mpc = pool->freelist[freelist_cat];
			mpc_chain = 0;

			while (mpc)
			{
				if (mpc->size >= allocsz)
				{
					if (mpc->size - allocsz > sizeof(struct MemoryPoolChunkData))
					{
						/*
						 * We really need to work some magic and calculate the size that
						 * is left on this chunk and splice it into the appropriate
						 * free list.  What happened here is we found a chunk big enough
						 * but it's actually too big and we don't want to just throw 
						 * away the trailing free space that we don't use.  Who knows
						 * how long this allocation could be around, especially if it's in
						 * the top level context.
						 */

						new_freelist_cat = pool_freelist_category(pool, mpc->size - allocsz);
						assert(new_freelist_cat < MPC_MAX_FREELIST);

						if (mpc_chain)
						{
							mpc_chain->next = mpc->next;
						}
						else
						{
							pool->freelist[freelist_cat] = mpc->next;
						}

						mpce = (MemoryPoolChunk)(((char*)mpc) + allocsz);
						mpce->size = mpc->size - allocsz;
						mpce->next = 0;

						if ((mpcep = pool->freelist[new_freelist_cat]))
						{
							pool->freelist[new_freelist_cat] = mpce;

							if (mpcep == mpc)
							{
								/*
								 * The chunk we used was at the start of the freelist.
								 */
								mpce->next = mpc->next;
							}
							else
							{
								/*
								 * Shift the freelist right.
								 */
								mpce->next = mpcep;
							}
						}
						else
						{
							/*
							 * New freelist was empty, so put the what's left at the
							 * head of it.
							 */
							pool->freelist[new_freelist_cat] = mpce;
						}
					}
					else
					{
						/*
						 * There was not enough room on the chunk after allocating from
						 * it to be worthwhile to keep in a free list.
						 */

						if (mpc_chain)
						{
							mpc_chain->next = mpc->next;
						}
						else
						{
							pool->freelist[freelist_cat] = mpc->next;
						}
					}

					chunk = (MemoryChunkHeader)mpc;
					chunk->context = context;
					chunk->size = allocsz;

					pool->header.stats.allocs_from_list++;
					pool->header.stats.allocs++;

					assert(pool->header.stats.freespace > pool->header.stats.freespace - requestedsize);
					pool->header.stats.freespace -= allocsz;
					pool->header.stats.space += allocsz;

					return chunk + 1;
				}
				else
				{
					mpc_chain = mpc;
					mpc = mpc->next;
				}
			}

			freelist_cat++;
		}
	}

	if (allocsz >= pool->blockSize)
	{
		block = malloc(VH_MALLOC_BLOCKSZ(requestedsize));
		block->next = pool->block;
		pool->block = block;	

		chunk = (MemoryChunkHeader)(block + 1);
		chunk->context = context;
		chunk->size = requestedsize;

		pool->header.stats.allocs++;
		pool->header.stats.chunks++;
		pool->header.stats.space += allocsz;

		return chunk + 1;
	}
	else
	{
		/*
		 * Stand up a new block, put whatever is left on the appropriate free list
		 */
		block = malloc(pool->blockSize);
	   	block->next = pool->block;
		pool->block = block;

		chunk = (MemoryChunkHeader)(block + 1);
		chunk->context = context;
		chunk->size = allocsz;

		if (pool->blockSize - sizeof(struct MemoryBlockData) - allocsz >
			sizeof(struct MemoryPoolChunkData))
		{	
			mpc = (MemoryPoolChunk)((char*)chunk + allocsz);
			mpc->size = pool->blockSize - sizeof(struct MemoryBlockData) - allocsz;

			freelist_cat = pool_freelist_category(pool, mpc->size);
			assert(freelist_cat < MPC_MAX_FREELIST);

			if ((mpce = pool->freelist[freelist_cat]))
			{
				mpc->next = mpce;
				pool->freelist[freelist_cat] = mpc;
			}
			else
			{
				mpc->next = 0;
				pool->freelist[freelist_cat] = mpc;
			}
		}

		pool->header.stats.allocs++;
		pool->header.stats.blocks++;
		pool->header.stats.space += allocsz;
		pool->header.stats.freespace += mpc->size;
		
		return chunk + 1;	
	}

	return 0;
}

static void 
MemPool_Destroy(void* context)
{
	MemoryPool pool = context;
	MemoryBlock block;
	void *ptr;

	block = pool->block;

#ifdef VH_MMGR_DEBUG
	printf("MemPool_Destroy: %s\n",
		   context->name);
#endif

	while (block)
	{
		ptr = block;
		block = block->next;

		free(ptr);
	}

	if (pool->header.parentContext)
	{
		memset(context, 0x7F, sizeof(MemoryPoolData));
		free(context);
	}
	else
		free(context);
}

static void
MemPool_Free(void* context, void *pointer)
{
	MemoryPool pool = context;
	MemoryChunkHeader mch = ((MemoryChunkHeader)pointer) - 1;
	MemoryBlock tmb, pmb, mb;
	MemoryPoolChunk mpc, mpcn;
	uint32_t freelist_cat;
	size_t sz = mch->size;


	if (mch->size > (pool->blockSize - sizeof(struct MemoryBlockData)))
	{
		/*
		 * Blocks larger than blockSize will always be allocatd by a single
		 * malloc call.  Thus we search the block list for this block and
		 * splice it out prior to calling free directly.
		 */

		tmb = ((MemoryBlock)mch) - 1;
		
		pmb = 0;
		mb = pool->block;

		while (mb)
		{
			if (tmb == mb) break;

			pmb = mb;
			mb = mb->next;
		}

		if (mb)
		{
			if (pmb)
				pmb->next = mb->next;
			else
				pool->block = mb->next;

			free(mb);

			pool->header.stats.chunks--;
			pool->header.stats.frees++;
		}
	}
	else
	{
		/*
		 * Put the chunk on the free list.
		 */

		mpc = (MemoryPoolChunk)mch;
		freelist_cat = pool_freelist_category(pool, sz);
		assert(freelist_cat < MPC_MAX_FREELIST);

		mpcn = pool->freelist[freelist_cat];
		pool->freelist[freelist_cat] = mpc;	
		
		mpc->next = mpcn;
		mpc->size = sz;
			
		pool->header.stats.frees++;
		pool->header.stats.freespace += sz;
	}
}

static void*
MemPool_ReAlloc(void *context, void *pointer, size_t size)
{
	MemoryChunkHeader hdr;
	void *nptr;

	hdr = ((MemoryChunkHeader)pointer) - 1;

	nptr = vh_mctx_alloc(context, size);
	memcpy(nptr, pointer, hdr->size - sizeof(MemoryChunkHeaderData));
	
	vh_mctx_free(context, pointer);

	return nptr;
}

