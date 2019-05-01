/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_utils_SList_H
#define vh_utils_SList_H

struct SListData
{
	MemoryContext mctx;
	uint32_t size;
	uint32_t capacity;
	uint32_t value_sz;
	void *head;
	bool deref;
};

SList vh_SListCreate(void);
SList vh_SListCreate_ctx(MemoryContext context);
SList vh_SListCreate_cap(uint32_t capacity);
SList vh_SListCreate_ctxcap(MemoryContext context, uint32_t capacity);
void vh_SListDestroy(SList list);
void vh_SListFinalize(SList list);
void vh_SListInit(SList list, uint32_t value_sz, bool deref);
void vh_SListInitR(SList list, uint32_t value_sz, bool deref);
void vh_SListResize(SList list);
SList vh_SListCopy(SList from);
void vh_SListCopyRaw(SList from, SList to);
void vh_SListClear(SList list);

void* vh_SListAt(SList list, uint32_t index);
void* vh_SListAtPointer(SList list, uint32_t index);
uint32_t vh_SListSize(SList list);
void* vh_SListFirst(SList list);
void vh_SListPush(SList list, const void *object);
void* vh_SListPop(SList list);
void vh_SListRemove(SList list, void *object);
void vh_SListRemoveAt(SList list, uint32_t index);
void vh_SListMerge(SList target, SList source);

#define vh_SListIterator(slist, start)		\
	( { \
	  	(start) = (slist)->head; \
	  	(slist)->size; \
	  } )

#define vh_slist_create(value_sz)												\
	( { 																		\
	  	SList list = vh_SListCreate();											\
	  	vh_SListInit(list, (value_sz));											\
	  	list;																	\
	  } )

#endif

