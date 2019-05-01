/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <stdio.h>

#include "vh.h"

static void MemoryContextDestroyImpl(MemoryContext context);
static void	MemoryContextDestroyChildren(MemoryContext context, MemoryContext top);
static void MemoryContextPrintNest(MemoryContext mctx, uint32_t level);

void*
vh_mctx_create(MemoryContext parent, uint32_t size, MemoryContextOps ops,
	 		   const char *name)
{
	MemoryContext mctx;
	CatalogContext ccontext;
	void *ptr;
	
	ccontext = vh_ctx();

	if (!ccontext)
	{
		/* this will be the top memory context */
		ptr = malloc(size + strlen(name) + 1);
		
	}
	/*
	else if (parent)
	{
		ptr = vh_mctx_alloc(parent, size + strlen(name) + 1);
	}
	*/
	else
	{
		ptr = malloc(size + strlen(name) + 1);
	}

	mctx = (MemoryContext)ptr;
	mctx->parentContext = parent;
	mctx->firstChild = 0;
	mctx->nextSibling = 0;
	mctx->ops = ops;

	/*
	 * Set the statistics counters to zero
	 */
	mctx->stats.allocs = 0;
	mctx->stats.chunks = 0;
	mctx->stats.blocks = 0;
	mctx->stats.frees = 0;
	mctx->stats.space = 0;
	mctx->stats.freespace = 0;
	mctx->stats.allocs_from_list = 0;

	mctx->name = ((char*)(mctx)) + size;
	memcpy(mctx->name, name, strlen(name));
	mctx->name[strlen(name)] = '\0';

	if (parent)
	{
		mctx->nextSibling = parent->firstChild;
		parent->firstChild = mctx;
	}

	return mctx;
}

MemoryContext 
vh_mctx_current(void)
{
	CatalogContext cctext = vh_ctx();

	return (MemoryContext)cctext->memoryCurrent;
}

MemoryContext 
vh_mctx_top(void)
{
	CatalogContext cctext = vh_ctx();

	return (MemoryContext)cctext->memoryTop;
}

MemoryContext 
vh_mctx_switch(MemoryContext context)
{
	MemoryContext old;
	CatalogContext cctext = vh_ctx();
	
	old = (MemoryContext)cctext->memoryCurrent;
	cctext->memoryCurrent = context;

	return old;
}

void*
vh_mctx_alloc(MemoryContext mctx, size_t size)
{
	return mctx->ops->alloc(mctx, size);
}

void
vh_mctx_free(MemoryContext mctx, void *ptr)
{
	return mctx->ops->free(mctx, ptr);
}

void*
vh_mctx_realloc(MemoryContext mctx,	void *ptr, size_t size)
{
	return mctx->ops->realloc(mctx, ptr, size);
}

MemoryContext
vh_mctx_from_pointer(void *pointer)
{
	MemoryChunkHeader chunk = pointer;

	chunk -= 1;

	if (vh_mchdr_is_superblock(chunk))
	{
		elog(ERROR2,
				emsg("Not implemented!"));
	}
	else
	{
		return chunk->context;
	}

	return 0;
}

void
vh_mctx_print_stats(MemoryContext mctx, bool nest)
{
	printf("\nMemory Context Statistics: %s\n"
		   "allocs:\t\t\t%ld\n"
		   "allocs from list:\t%ld\n"
		   "frees:\t\t\t%ld\n"
		   "blocks:\t\t\t%ld\n"
		   "chunks:\t\t\t%ld\n"
		   "freespace:\t\t%ld\n"
		   "space:\t\t\t%ld\n"
		   "total occupied:\t\t%ld\n",
		   mctx->name,
		   mctx->stats.allocs,
		   mctx->stats.allocs_from_list,
		   mctx->stats.frees,
		   mctx->stats.blocks,
		   mctx->stats.chunks,
		   mctx->stats.freespace,
		   mctx->stats.space,
		   mctx->stats.space + mctx->stats.freespace);

	if (nest)
	{
		MemoryContextPrintNest(mctx, 1);
		printf("\n\n");
	}
}

static void
MemoryContextPrintNest(MemoryContext mctx, uint32_t level)
{
	uint32_t i = 0, t = 0;
	MemoryContext nctx;
	size_t slen;

	printf("\n");

	while (i < level)
	{
		printf("\t");
		i++;
	}

	slen = strlen(mctx->name);

	if (slen < 40)
		t = (40 - slen) / 8 + 1;
	else
		t = 0;

	printf("%.100s", mctx->name);
	while (t--) printf("\t");
	printf("%ld", mctx->stats.space);

	if (mctx->firstChild)
		MemoryContextPrintNest(mctx->firstChild, level + 1);

	nctx = mctx->nextSibling;

	while (nctx)
	{
		i = 0;
		printf("\n");

		while (i < level)
		{
			printf("\t");
			i++;
		}


		slen = strlen(nctx->name);

		if (slen < 40)
			t = (40 - slen) / 8 + 1;
		else
			t = 0;

		printf("%.100s", nctx->name);
		while (t--) printf("\t");
		printf("%ld", nctx->stats.space);

		if (nctx->firstChild)
			MemoryContextPrintNest(nctx->firstChild, level + 1);

		nctx = nctx->nextSibling;
	}
}

static void
MemoryContextDestroyImpl(MemoryContext context)
{
	MemoryContext parent, left, right;

	parent = context->parentContext;

	if (context == vh_mctx_current())
		vh_mctx_switch(parent);

	if (parent)
	{
		left = parent->firstChild;
		right = parent->firstChild;
		while (right)
		{
			if (right == context)
			{
				left->nextSibling = context->nextSibling;

				if (right == parent->firstChild)
					parent->firstChild = context->nextSibling;

				break;
			}

			left = right;
			right = right->nextSibling;
		}
	}

	context->ops->destroy(context);
}

/*
 * Destroys the context and all children
 */
void
vh_mctx_destroy(MemoryContext context)
{
	MemoryContextDestroyChildren(context, context);
	MemoryContextDestroyImpl(context);
}

void
vh_mctx_destroy_children(MemoryContext context)
{
	MemoryContextDestroyChildren(context, context);
}

static void
MemoryContextDestroyChildren(MemoryContext context, MemoryContext top)
{
	MemoryContext parent;

	if (context->firstChild)
		MemoryContextDestroyChildren(context->firstChild, context);
	
	if (!context->firstChild && context != top)
	{
		parent = context->parentContext;

		while (parent->firstChild)
		{
			MemoryContextDestroyChildren(parent->firstChild, parent->firstChild);
			MemoryContextDestroyImpl(parent->firstChild);
		}
	}
}

