/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_mctx_sharedctx_H
#define vh_mctx_sharedctx_H

#include "io/utils/mmgr/MemoryContext.h"


/*
 * SharedMemoryContext
 *
 * The process local handle for a shared memory region.
 */

struct SharedMemoryContextData
{
	struct MemoryContextData mctx;
	struct MemorySuperBlockId ident;

	char *mapped_address_space;
	void *free_root;

	int32_t fd;
};


MemoryContext vh_mctx_shared_attach(MemorySuperBlockId ident);
MemoryContext vh_mctx_shared_create(struct MemorySuperBlockId ident,
									size_t block_size,
					  				size_t minimum_size,
				  					bool allow_growth,
				  					const char *name);


#endif

