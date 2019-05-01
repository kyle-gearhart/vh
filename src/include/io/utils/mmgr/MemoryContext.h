/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_utils_mmgr_MemoryContext_H
#define vh_datacatalog_utils_mmgr_MemoryContext_H

typedef struct MemoryContextData *MemoryContext;

typedef enum
{
	MT_Pool,
	MT_ObjectPool
} MemoryTag;

typedef struct MemoryContextOpsTable
{
	/*
	 * Note: The first parameter is always a MemoryContext.  We make it opaque
	 * to keep all the casts out.
	 */

	void* (* const alloc)(void*, size_t);
	void* (* const realloc)(void*, void*, size_t);
	void (* const free)(void*, void*);
	void (* const destroy)(void*);
} MemoryContextOpsTable, *MemoryContextOps;

/*
 * This POD should be the first data member
 */
typedef struct MemoryContextData
{
	MemoryContextOps ops;
	MemoryContext parentContext;
	MemoryContext firstChild;
	MemoryContext nextSibling;

	struct
	{
		size_t allocs;
		size_t allocs_from_list;
		size_t frees;
		size_t blocks;
		size_t chunks;
		size_t freespace;
		size_t space;
	} stats;

	char *name;	
	MemoryTag tag;
} MemoryContextData, *MemoryContext;

/*
 * We do a little bit of magic with our MemoryChunkHeader that comes immediately
 * before an pointer returned by the allocators.  If the high bit on size is set, 
 * then we know the header doesn't contain a pointer to a MemoryContext, but rather
 * the number of bytes that must be walked backwards (traversing over the entire
 * MemoryChunkHeader structure) to reach a superblock.
 *
 * The superblock strategy is typically used by shared memory contexts.
 */
#define VH_MCHDR_FLAG_SUPERBLOCK		( 1ll << ((sizeof(uintptr_t) * 8) - 1))

#define vh_mchdr_is_superblock(mchdr)	((mchdr)->size & VH_MCHDR_FLAG_SUPERBLOCK)
#define vh_mchdr_make_superblock(mchdr)	((mchdr)->size |= VH_MCHDR_FLAG_SUPERBLOCK)
#define vh_mchdr_size(mchdr)			((mchdr)->size & ~VH_MCHDR_FLAG_SUPERBLOCK)
#define vh_mchdr_set_size(mchdr, sz)	((mchdr)->size = (sz))

#define vh_mchdr_superblock(ptr)		( vh_mchdr_is_superblock(ptr) 			?	\
	((void*)(((unsigned char*)ptr) - ((((MemoryChunkHeader)ptr) - 1)->offset))) : 0 )


typedef struct MemoryChunkHeaderData
{
	union
	{
		MemoryContext context;
		size_t offset;
	};

	size_t size;
} *MemoryChunkHeader;


/*
 * Create and destroy
 */
void* vh_mctx_create(MemoryContext parent, uint32_t size, MemoryContextOps ops,
					 const char *name);
void vh_mctx_destroy(MemoryContext context);
void vh_mctx_destroy_children(MemoryContext context);

/*
 * Clear a MemoryContext but don't make it unusable.
 */
void vh_mctx_clear(MemoryContext context);
void vh_mctx_clear_children(MemoryContext context);

/* Scope functions for standard allocations */
MemoryContext vh_mctx_current(void);
MemoryContext vh_mctx_top(void);
MemoryContext vh_mctx_switch(MemoryContext context);


/* CRUD functions */

void* vh_mctx_alloc(MemoryContext mctx, size_t size);
void vh_mctx_free(MemoryContext mctx, void *ptr);
void* vh_mctx_realloc(MemoryContext mctx, void *ptr, size_t size);

MemoryContext vh_mctx_from_pointer(void *ptr);


/*
 * Debug help
 */
void vh_mctx_print_stats(MemoryContext mctx, bool nest);

#endif

