/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_shard_BeaconCatalog_H
#define vh_datacatalog_shard_BeaconCatalog_H


typedef struct BeaconCatalogData *BeaconCatalog;

BeaconCatalog vh_BeaconCatalogCreate(void);
void vh_BeaconCatalogAdd(BeaconCatalog catalog, Beacon beacon, const char *name);
void vh_BeaconCatalogDestroy(BeaconCatalog catalog);
void vh_BeaconCatalogRemove(BeaconCatalog catalog, const char *name);
Beacon vh_BeaconCatalogGetBeacon(BeaconCatalog catalog, const char *name);

#endif

