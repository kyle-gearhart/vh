/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_IO_BUFFER_ITEMPTR_H
#define VH_IO_BUFFER_ITEMPTR_H



typedef struct HeapItemPtrData
{
	uint32_t offset:16,
			 length:15,
			 empty:1;
} HeapItemPtrData, *HeapItemPtr;

typedef uint16_t HeapItemOffset;
typedef uint8_t HeapItemSlot;

#endif

