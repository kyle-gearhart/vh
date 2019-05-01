/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_DATACATALOG_BUFFER_BUFFMGR_H
#define VH_DATACATALOG_BUFFER_BUFFMGR_H

/*
 * The buffer manager is resposible for assigning HeapBuffer to transactions.
 *
 * When a buffer is requested, the buffer manager finds an open slot and
 * initializes the buffer by calling vh_hb_open.  When a caller is done with
 * the buffer (i.e. XAct is destroyed), the caller will call vh_hb_close.
 */

bool vh_hbmgr_startup(MemoryContext mctx);
HeapBufferNo vh_hb_open(MemoryContext mctx);
bool vh_hb_close(HeapBufferNo buffno);
bool vh_hb_isopen(HeapBufferNo buffno);

#endif

