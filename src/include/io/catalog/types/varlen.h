/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_types_varlen_H
#define vh_datacatalog_types_varlen_H

/*
 * Variable length data structures are all 13 bytes in length.  The uniform
 * size is required to ensure stable offsets of the type in the two states
 * maintained by the HeapBuffer mechanism.  Without uniform sizes, the location
 * of each field on the HeapTuple would require offset and alignment adjustments
 * when transitioning state.
 *
 * Variable length data structures are used to describe the two manners
 * variable length data types can exist within the HeapBuffer:
 * 	1) In memory
 * 	2) On disk
 *
 * Initially, all variable length data types will exist in memory.  In the event
 * the HeapBuffer must evict a HeapPage, variable length data will be written
 * to a variable length data file block.  The HeapBuffer will call the evictbuf
 * function for the data type to flush to disk.
 *
 * In the event an evicted HeapPage is requested from disk, the variable length
 * data will not be brought into memory.  When the HeapPage becomes pinned,
 * HeapTuple requested, or specific variable length field requested, the variable
 * length data will be fetched from disk and reconstructed by the read function.
 *
 * After a fetch from disk, the BlockNumber(s) required to store the variable
 * length data are marked as free pages, but not erased.  There is no BlockNumber,
 * HeapItemPtr dangling, as these will be invalidated in the vhvarlenm after a
 * successful readbuf call.
 *
 * The HeapBuffer is responsible for interchanging vhvarlenm and vhvarlend.  We
 * store the size on disk in vhvarlend to avoid seek time in the file.  The
 * HeapBufferNo is also dropped, if we're accessing the disk we know what buffer
 * we're in.
 */

#define vhvarlenm_sz		(sizeof(size_t) + sizeof(HeapBufferNo))

struct vhvarlenm
{
	size_t size;
	HeapBufferNo hbno;
	
	/*
	 * 8 bytes	- size
	 * 1 byte	- HeapBufferNo
	 * ============================
	 * 9 bytes total
	 */
};

struct vhvarlenmpad
{
	size_t size;
	HeapBufferNo hbno;
	unsigned char padding[7];
};

#endif

