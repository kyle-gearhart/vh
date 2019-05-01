/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/utils/SList.h"


static void SListGrow(SList list, uint32_t minimum);

SList
vh_SListCreate(void)
{
	return vh_SListCreate_cap(8);
}

SList
vh_SListCreate_cap(uint32_t capacity)
{
	return vh_SListCreate_ctxcap(vh_mctx_current(), capacity);
}

SList
vh_SListCreate_ctx(MemoryContext context)
{
	return vh_SListCreate_ctxcap(context, 8);
}

SList
vh_SListCreate_ctxcap(MemoryContext context, uint32_t capacity)
{
	SList list;

	list = (SList)vhmalloc_ctx(context, sizeof(struct SListData));
	list->capacity = capacity;
	list->mctx = context;
	list->size = 0;
	list->deref = false;
	list->value_sz = sizeof(uintptr_t);
	list->head = vhmalloc_ctx(context, list->value_sz * capacity);

	memset(list->head, list->value_sz * capacity, 0);

	return list;
}

void
vh_SListDestroy(SList list)
{
	if (list->head)
		vhfree(list->head);

	vhfree(list);
}

void
vh_SListFinalize(SList list)
{
	if (list->head)
		vhfree(list->head);

	list->head = 0;
	list->capacity = 0;
	list->size = 0;
}

void
vh_SListInit(SList list, uint32_t value_size, bool deref)
{
	if (value_size < list->value_sz)
		list->capacity *= (list->value_sz / value_size);
	else
		list->capacity /= (value_size / list->value_sz);

	list->value_sz = value_size;
	list->deref = deref;
}

void
vh_SListInitR(SList list, uint32_t value_size, bool deref)
{
	list->capacity = 8;
	list->mctx = vh_mctx_current();
	list->size = 0;
	list->deref = deref;
	list->value_sz = value_size;
	list->head = vhmalloc_ctx(list->mctx, list->value_sz * list->capacity);

	memset(list->head, list->value_sz * list->capacity, 0);
}

void*
vh_SListAt(SList list, uint32_t index)
{
	char *head = list->head;

	if (index < list->size &&
		index >= 0)
	{
		head += list->value_sz * index;
	}

	return head;
}

void
vh_SListClear(SList list)
{
	vhfree(list->head);
	list->size = 0;
	list->capacity = 8;
	list->value_sz = sizeof(uintptr_t);
	list->head = vhmalloc_ctx(list->mctx,
		   					  list->value_sz * list->capacity); 
	memset(list->head, 0, 
		   list->value_sz * list->capacity);
}

void
vh_SListCopyRaw(SList to, SList from)
{
	if (from->value_sz != to->value_sz)
	{
		elog(ERROR2,
				emsg("Unable to copy the SList, to [%p] and from [%p] have "
					 "different value sizes!",
					 to,
					 from));

		return;
	}

	if (to->capacity < from->size)
		SListGrow(to, from->size);

	if (from->size < to->capacity)
	{
		memcpy(to->head, from->head, from->value_sz * from->size);
		to->size = from->size;
	}
}

void*
vh_SListFirst(SList list)
{
	char *head = list->head;

	return head;
}

void 
vh_SListPush(SList list, const void *object)
{
	char *head;

	if (list->size + 1 > list->capacity)
	{
		SListGrow(list, list->size);
	}
	
	head = list->head;
	head += list->value_sz * list->size;
	
	/*
	 * Check to see if we should dereference the @object passed to assign
	 * it's value at the memory address it belongs in the array.  An example
	 * of this is storing a HeapTuplePtr (8 bytes) on a 32-bit system.
	 */
	if (list->deref)
	{
		switch (list->value_sz)
		{
			case 1:
				*head = *((char*)head);
				break;

			case 2:
				*((uint16_t*)head) = *((uint16_t*)object);
				break;

			case 4:
				*((uint32_t*)head) = *((uint32_t*)object);
				break;

			case 8:
				*((uint64_t*)head) = *((uint64_t*)object);
				break;

			default:
				memcpy(head, object, list->value_sz);
				break;
		}
	}
	else
	{
		assert(list->value_sz == sizeof(uintptr_t));

		*((const void**)head) = object;
	}
	
	list->size++;
}

void* 
vh_SListPop(SList list)
{
	char *head = list->head;

	if (list->size)
	{
		head += (list->value_sz * (--list->size));

		return head;
	}

	return 0;
}

void
vh_SListRemoveAt(SList list, uint32_t index)
{
	char *head, *item;

	if (index >= 0 &&
		index < list->size)
	{
		head = list->head;
		item = head + (list->value_sz * index);

		if (index < list->size - 1)
			memmove(item, 
					item + list->value_sz, 
					(list->size - index - 1) * list->value_sz);
	
		list->size--;
	}

}

uint32_t
vh_SListSize(SList list)
{
	return list->size;
}

uint32_t
vh_SListWillGrowToOnPush(SList list)
{
	if (list->size + 1 > list->capacity)
	{
		return list->capacity << 1u;
	}

	return 0;
}

SList
vh_SListCopy(SList from)
{
	SList n;

	n = vh_SListCreate_ctxcap(from->mctx,
							  from->capacity);
	memcpy(n->head, from->head, 
		   sizeof(uintptr_t) * from->size);
	n->size = from->size;

	return n;
}

void
vh_SListMerge(SList target, SList source)
{
	void **source_head;
	uint32_t source_sz, i;

	source_sz = vh_SListIterator(source, source_head);

	for (i = 0; i < source_sz; i++)
		vh_SListPush(target, source_head[i]);
}

static void
SListGrow(SList list, uint32_t minimum)
{
	size_t delta, offset;

	list->capacity = list->capacity << 1u;
	list->head = vh_mctx_realloc(list->mctx,
								 list->head,
								 list->value_sz * list->capacity);

	delta = list->value_sz * (list->capacity - list->size);
	offset = list->value_sz * list->size;
	memset((char*)list->head + offset, 0, delta);
}

