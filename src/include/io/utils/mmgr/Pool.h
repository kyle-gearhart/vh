/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_utils_mmgr_Pool_H
#define vh_datacatalog_utils_mmgr_Pool_H

MemoryContext vh_MemoryPoolCreate(MemoryContext parent, uint32_t blockSize, const char* name);

#endif

