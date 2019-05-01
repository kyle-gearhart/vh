/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_utils_mmgr_superblock_H
#define vh_utils_mmgr_superblock_H

/*
 * Memory Super Blocks
 *
 * Memory super blocks are used when memory regions need to be shared but could
 * possibly be mapped to different address spaces.
 *
 * The MemoryChunkHeader knows when it was allocated using a superblock technique
 * and it's flags are set accordingly.  When the MemoryChunkHeader is "walked 
 * back" to the super block, this is what we should find.
 *
 * We use a magic field at the start which every super block by default should set
 * to the same value.
 *
 * There's a tag field which is used to map us to a MemoryOps table in the local
 * process.  
 *
 * The identifier field allows for the local process to find it's local handle for
 * the MemorySuperBlock (or create one!).
 */

#define VH_MCTX_SUPERBLOCK_MAGIC 		(0xf391d9ae)
#define VH_MCTX_SUPERBLOCK_ID_SZ		(16)

struct MemorySuperBlockId
{
	char id[VH_MCTX_SUPERBLOCK_ID_SZ];
};

struct MemorySuperBlockData
{
	int32_t magic;
	int32_t tag;
	char identifier[16];
};


#endif

