/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_shard_ConnectionCatalog_H
#define vh_datacatalog_shard_ConnectionCatalog_H

typedef struct BackEndConnectionData *BackEndConnection;
typedef struct ConnectionCatalogData ConnectionCatalogData, *ConnectionCatalog;
typedef struct ShardData *Shard;
typedef struct ShardAccessData ShardAccessData, *ShardAccess;


ConnectionCatalog vh_ConnectionCatalogCreate(void);
Shard vh_ConnectionCatalogGetDefault(ConnectionCatalog);
void vh_ConnectionCatalogSetDefault(ConnectionCatalog, Shard);
void vh_ConnectionCatalogShutDown(ConnectionCatalog cc);
void vh_ConnectionCatalogDestroy(ConnectionCatalog catalog);

BackEndConnection vh_ConnectionGet(ConnectionCatalog catalog, ShardAccess shardam);
void vh_ConnectionReturn(ConnectionCatalog catalog, BackEndConnection nconn);

#endif

