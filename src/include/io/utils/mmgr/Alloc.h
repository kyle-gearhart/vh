/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_utils_mmgr_alloc_H
#define vh_utils_mmgr_alloc_H


void* vhmalloc(size_t size);
void* vhmalloc_ctx(MemoryContext context, size_t size);
void vhfree(void *pointer);

void* vhrealloc(void *ptr, size_t size);

#endif

