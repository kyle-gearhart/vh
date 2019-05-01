/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_CatalogContext_H
#define vh_datacatalog_CatalogContext_H


typedef struct CatalogContextData
{
	/* Portal run */
	void *errorQueue;
	MemoryContext memoryTop;
	MemoryContext memoryCurrent;
	XAct xactTop;
	XAct xactCurrent;

	/* Catalog */
	TableCatalog catalogTable;
	BackEndCatalog catalogBackEnd;
	BeaconCatalog catalogBeacon;
	ConnectionCatalog catalogConnection;
	TypeCatalog catalogType;

	/* Default Heap Buffer */
	HeapBufferNo hbno_general;

	/* Default Shard */
	Shard shard_general;
} CatalogContextData, *CatalogContext;

/*
 * Startup and Shutdown (in catalog/CatalogContext.c)
 */
CatalogContext vh_start(void);
void vh_shutdown(void);
void vh_ctx_attach(CatalogContext context);
void vh_ctx_init(CatalogContext context);
void vh_ctx_destroy(CatalogContext context);


CatalogContext vh_ctx(void);


#endif

