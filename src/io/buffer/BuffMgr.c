/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/executor/xact.h"
#include "io/buffer/HeapBuffer.h"
#include "io/buffer/HeapPage.h"
#include "io/buffer/BuffMgr.h"
#include "io/utils/kvmap.h"
#include "io/utils/SList.h"

#define VH_HB_MAXBUFFERS 		10

HeapBuffer* vh_buffers = 0;

/*
 * vh_hbmgr_startup
 *
 * The HeapBuffer array local to a process.
 */

bool
vh_hbmgr_startup(MemoryContext mctx)
{
	MemoryContext mctx_old;

	if (vh_buffers)
		elog(FATAL,
			 emsg("Buffer manager has already been started!"));

	mctx_old = vh_mctx_switch(mctx);

	vh_buffers = vhmalloc(sizeof(HeapBuffer) * VH_HB_MAXBUFFERS);
	memset(vh_buffers, 0, sizeof(HeapBuffer) * VH_HB_MAXBUFFERS);

	vh_mctx_switch(mctx_old);

	return true;
}

HeapBufferNo
vh_hb_open(MemoryContext mctx)
{
	MemoryContext mctx_old, mctx_hb;
	HeapBufferNo buffno;
	HeapBuffer hb = 0;
	char name_buffer[50];

	for (buffno = 5; buffno < VH_HB_MAXBUFFERS - 1; buffno++)
	{
		if (!vh_hb(buffno))
		{
			snprintf(&name_buffer[0], 50, "HeapBuffer %d context", buffno);
			mctx_hb = vh_MemoryPoolCreate(mctx,
   										  VH_HEAPPAGE_SIZE * 5,
   										  &name_buffer[0]);
			mctx_old = vh_mctx_switch(mctx_hb);

			hb = vhmalloc(sizeof(struct HeapBufferData));
			memset(hb, 0, sizeof(struct HeapBufferData));

			hb->mctx = mctx_hb;
			hb->idx = buffno;		
			hb->xid = 0;	
			hb->free_list = 0;
			hb->allocfactor = 10;
			
			/*
			 * Create the block table here
			 */
			hb->blocks = vh_kvmap_create_impl(sizeof(BufferBlockNo),
											  sizeof(void*),
											  vh_htbl_hash_int32,
											  vh_htbl_comp_int32,
											  mctx_hb);

			mctx_hb = vh_mctx_switch(mctx_old);
			
			vh_hb(buffno) = hb;

			return buffno;
		}
	}

	elog(ERROR2,
		 emsg("Unable to find a free HeapBuffer!  "
			  "Check transaction depth and number of current "
			  "transactions running in the cluster"));

	return 0;
}

bool
vh_hb_close(HeapBufferNo buffno)
{
	HeapBuffer hb = vh_hb(buffno);

	assert(hb);

	vh_mctx_print_stats(vh_ctx()->memoryTop, true);

	vh_kvmap_destroy(hb->blocks);
	vh_mctx_destroy(hb->mctx);

	vh_hb(buffno) = 0;

	return true;
}

bool
vh_hb_isopen(HeapBufferNo buffno)
{
	return vh_hb(buffno) != 0;
}

