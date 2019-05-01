/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_utils_mmgr_shared_H
#define vh_io_utils_mmgr_shared_H

#include "io/utils/mmgr/superblock.h"

/*
 * This is our shared memory allocator that works within the existing
 * MemoryContext framework.  We require no locks in the SharedMemoryBlock,
 * as all "locking" is done within the freespace B+Tree on a "page" by
 * "page" basis.  A page user defined by block_size.
 */

struct SharedMemoryBlock
{
	struct MemorySuperBlockData super;

	size_t block_size;
	size_t min_size;

	int32_t ref_count;

	size_t root_freespace;
};

#endif

