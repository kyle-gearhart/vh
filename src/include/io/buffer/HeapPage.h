/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef VH_DATACATALOG_BUFFER_HEAPPAGE_H
#define VH_DATACATALOG_BUFFER_HEAPPAGE_H

#include "io/buffer/ItemPtr.h"

#define VH_HEAPPAGE_SIZE 8192

typedef struct HeapPageData
{
	uint16_t pins;
	uint16_t d_freespace;
	uint16_t d_begin;
	uint16_t d_flower;		/* Last item pointer */
	uint16_t d_fupper;		/* Bottom of the page */
	uint16_t n_items;
	uint8_t flags;
	HeapItemPtrData items[1];
} HeapPageData, *HeapPage;

#define VH_HP_FLAGDIRTY			((uint8_t)0x1)

#define vh_hp_isdirty(hp)		(hp->flags & VH_HP_FLAGDIRTY)
#define vh_hp_cleardirty(hp)	(hp->flags &= ~VH_HP_FLAGDIRTY)
#define vh_hp_setdirty(hp)		(hp->flags |= VH_HP_FLAGDIRTY)

#define VH_HP_TUPLE(hp, hip)	(hp->items[hip].empty ? 0 : \
								 (((char*)hp) + (hp->items[hip].offset)))

#define vh_hp_freespace(hp) 		(hp->d_freespace)
#define vh_hp_freespaceitm(hp)		(hp->d_freespace ? 									\
									 hp->d_freespace > sizeof(HeapItemPtrData) ? 		\
									 hp->d_freespace - sizeof(HeapItemPtrData) : 0		\
									 : 0 )

void vh_hp_init(HeapPage hp);
HeapItemSlot vh_hp_construct_tup(HeapBuffer hb, HeapPage hp, HeapTupleDef htd);
void vh_hp_freetup(HeapPage hp, HeapItemSlot hidx);
void vh_hp_collapse_empty(HeapPage hp);



#endif

