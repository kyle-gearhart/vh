/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/shard/Beacon.h"
#include "io/shard/BeaconCatalog.h"
#include "io/utils/htbl.h"

typedef struct BeaconCatalogData
{
	HashTable htbl;
	MemoryContext mctx;
} BeaconCatalogData, *BeaconCatalog;



void
vh_BeaconCatalogAdd(BeaconCatalog catalog, Beacon beacon,
				   	const char* name)
{
}

BeaconCatalog
vh_BeaconCatalogCreate(void)
{
	CatalogContext cc = vh_ctx();
	MemoryContext mctx_old, mctx_cc;
	BeaconCatalog catalog;

	if (cc)
	{
		mctx_cc = vh_MemoryPoolCreate(cc->memoryTop,
									  8192,
									  "Beacon Catalog");
		mctx_old = vh_mctx_switch(mctx_cc);

		catalog = (BeaconCatalog)vhmalloc(sizeof(BeaconCatalogData));
		catalog->mctx = mctx_cc;

		vh_mctx_switch(mctx_old);
		
		return catalog;
	}
	else
	{
		elog(FATAL,
			 emsg("Unable to create Beacon Catalog, Catalog Context was not "
				  "found!"));
	}

	return 0;
}

void
vh_BeaconCatalogDestroy(BeaconCatalog catalog)
{

}

Beacon
vh_BeaconCatalogGetBeacon(BeaconCatalog catalog, const char *name)
{
	return 0;
}

void
vh_BeaconCatalogRemove(BeaconCatalog catalog, const char *name)
{

}

