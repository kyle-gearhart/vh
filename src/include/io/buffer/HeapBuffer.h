/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef VH_DATACATALOG_BUFFER_HEAPBUFFER_H
#define VH_DATACATALOG_BUFFER_HEAPBUFFER_H

#include "io/buffer/ItemPtr.h"

typedef uint32_t BufferBlockNo;
typedef uint8_t HeapBufferNo;
typedef uint64_t HeapTuplePtr;


/*
 * |blocks|
 * 		Lookup table by blockno.  Contains all blocks that are
 * 		available on the heap.
 *
 * |nblocks|
 * 		Total number of blocks managed by the buffer, on disk and
 * 		in memory.
 */

typedef struct HeapBufferData
{
	KeyValueMap blocks;
	MemoryContext mctx;
	struct BlockData *lru_first, *lru_last, *free_list;

	BufferBlockNo nblocks;
	uint16_t allocfactor;
	uint16_t xid;
	HeapBufferNo idx;
} *HeapBuffer;


#define VH_HB_HT_FLAG_FORREAD			0x8000
#define VH_HB_HT_FLAG_COMPARE			0x4000	

/*
 * vh_hb_heaptuple
 *
 * The lower byte of |flags| is used to denote VH_HT_FLAG flags.  The upper
 * byte is used for VH_HB_HT_FLAG flags.  Setting VH_HB_HT_FLAG_FORREAD
 * will only fetch the mutable copy if one exists, otherwise it's going to
 * to return the immutable copy
 */
HeapTuple vh_hb_heaptuple(HeapBuffer hb, HeapTuplePtr htp, uint16_t flags);

HeapTuplePtr vh_hb_allocht(HeapBuffer hb,
		   				   HeapTupleDef htd,
						   HeapTuple *ht);
HeapTuplePtr vh_hb_allocht_nearby(HeapTuplePtr htp,
								  HeapTupleDef htd,
								  HeapTuple *ht);
HeapTuplePtr vh_hb_copyht(HeapTuplePtr source,
						  HeapTuple source_hint,
						  HeapTuple *target);
HeapTuplePtr vh_hb_copyht_nearby(HeapTuplePtr htp,
								 HeapTuple source_hint,
								 HeapTuple *ht);

void vh_hb_free(HeapBuffer hb,
	  			BufferBlockNo block,
  				HeapItemSlot hpidx);
void vh_hb_prealloc(HeapBuffer hb,
					HeapTupleDef htd,
					uint32_t tups);

void vh_hb_printstats(HeapBuffer hb);


#define vh_HTP_BLOCKNO_MASK	0xffffffff00000000ULL
#define vh_HTP_XID_MASK 	0x00000000ffff0000ULL
#define vh_HTP_BUFF_MASK 	0x000000000000ff00ULL
#define vh_HTP_ITEMNO_MASK	0x00000000000000ffULL

#define vh_HTP_BLOCKNO(htp) ((((uint64_t)htp) & vh_HTP_BLOCKNO_MASK) >> 32)
#define vh_HTP_XID(htp) 	((((uint64_t)htp) & vh_HTP_XID_MASK) >> 16)
#define vh_HTP_BUFF(htp) 	((((uint64_t)htp) & vh_HTP_BUFF_MASK) >> 8)
#define vh_HTP_ITEMNO(htp)	(((uint64_t)htp) & vh_HTP_ITEMNO_MASK)
#define vh_HTP_FORM(blockno, xid, buff, itemidx) ((((uint64_t)blockno) << 32) | \
												  (((uint64_t)xid) << 16) | \
												  (((uint64_t)buff) << 8) | \
												  ((uint64_t)itemidx))

#define vh_HTP_PRINTERR(htp)	printf("%llu htp->block: %llu\titemno: %llu", \
									   htp, \
									   (vh_HTP_BLOCKNO(htp)), \
									   (vh_HTP_ITEMNO(htp)))

#define vh_htp_flags(htp, flags) ( \
	vh_hb(vh_HTP_BUFF(htp)) ? vh_hb(vh_HTP_BUFF(htp))->xid == \
	vh_HTP_XID(htp) ? \
	vh_hb_heaptuple(vh_hb(vh_HTP_BUFF(htp)), \
					 htp, \
					 (flags)) : \
	 ( { elog(ERROR2, emsg("XID %d Buffer %d mismatch, unable to locate " \
					   "requested heap tuple.\n\nBlockNo: %d\n", \
					   vh_HTP_XID(htp), \
					   vh_HTP_BUFF(htp), \
					   vh_HTP_BLOCKNO(htp))); \
				  (HeapTuple)0; }) : (HeapTuple)0)


#define vh_hb(hbno)		(vh_buffers[hbno])
#define vh_hb_memoryctx(hbno) 	(vh_hb(hbno) ? vh_hb(hbno)->mctx : 0)


#define vh_htp(htp)		(vh_htp_flags(htp, VH_HT_FLAG_MUTABLE | VH_HB_HT_FLAG_COMPARE))
#define vh_htp_immutable(htp)	(vh_htp_flags(htp, 0))
#define vh_htp_free(htp)		vh_hb_free(vh_hb(vh_HTP_BUFF(htp)), \
										   vh_HTP_BLOCKNO(htp), \
										   vh_HTP_ITEMNO(htp))
#define vh_htp_free_list(l, n)	do \
								 { \
									uint32_t i;		\
								 	for(i = 0; i < n; i++) \
								 	{ \
								 		vh_htp_free(l[i]); \
								 	} \
								 } \
								 while (0)


#define vh_htp_get(htptr, hf)		(vh_ht_get(vh_htp(htptr), hf))
#define vh_htp_set(htptr, hf, v)	(vh_ht_set(vh_htp(htptr), hf, v))
#define vh_htp_tuple(htptr)			(vh_ht_tuple(vh_htp(htptr)))

extern HeapBuffer* vh_buffers;

/*
 * We need to account for an SList of HeapTuplePtr on non-64 bit systems.  Set
 * the SList to an 8 byte value_sz and make a macro so we don't have to remember
 * to do this all over the place.
 */
#if VHB_SIZEOF_VOID == 8

#define vh_htp_SListCreate(list)			(list = vh_SListCreate(), list->deref = true)
#define vh_htp_SListCreate_ctx(list, mctx)	(list = vh_SListCreate_ctx((mctx)), list->deref = true)

#else
	
#define vh_htp_SListCreate(list)												\
	( {																			\
		list = vh_SListCreate();												\
		vh_SListInit(list, sizeof(HeapTuplePtr), true); 						\
	  } )
	  
#define vh_htp_SListCreate_ctx(list, mctx)										\
	( {																			\
		list = vh_SListCreate_ctx((mctx));										\
		vh_SListInit(list, sizeof(HeapTuplePtr), true); 						\
	  } )

#endif

#define vh_htp_SListPush(list, htp)		(vh_SListPush((list), &(htp)))

#endif

