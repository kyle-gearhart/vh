/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"


void* 
vhmalloc(size_t size)
{
	CatalogContext context;
	MemoryContext mem;

	context = vh_ctx();
	mem = context->memoryCurrent;
	
	return mem->ops->alloc(mem, size);
}

void*
vhmalloc_ctx(MemoryContext context, size_t size)
{
	return context->ops->alloc(context, size);
}

void 
vhfree(void *pointer)
{
	MemoryChunkHeader chunk;

	chunk = (MemoryChunkHeader)pointer - 1;
	chunk->context->ops->free(chunk->context, pointer);
}


void*
vhrealloc(void *ptr, size_t size)
{
	MemoryContext mem;
	MemoryChunkHeader chunk = ptr;

	chunk -= 1;
	mem = chunk->context;

	return mem->ops->realloc(mem, ptr, size);
}

