/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/BackEndCatalog.h"
#include "io/be/griddb/griddb-int.h"
#include "io/be/griddb/griddb-ext.h"

BackEndConnection grdb_createconn(void);
void grdb_freeconn(BackEndConnection);

bool grdb_connect(BackEndConnection,
				  BackEndCredentialVal*,
				  String);

typedef struct GRDBbeConnData *GRDBbeConn;
struct GRDBbeConnData
{
	struct BackEndConnectionData conn;
	GRDBconn gr;
};

struct BackEndData vh_be_griddb = {
	.id = 101,
	.name = "GridDB",
	.tam = TAM_Binary,

	.at = {
		.createconn = grdb_createconn,
		.freeconn = grdb_freeconn,

		.connect = grdb_connect
	}
};

BackEndConnection
grdb_createconn(void)
{
	GRDBbeConn beconn;

	beconn = vh_be_conn_create(&vh_be_griddb, sizeof(struct GRDBbeConnData));
	beconn->gr = 0;

	return &beconn->conn;
}

void
grdb_freeconn(BackEndConnection bec)
{
}

bool 
grdb_connect(BackEndConnection bec,
  			 BackEndCredentialVal* val,
			 String db)
{
	GRDBbeConn beconn = (GRDBbeConn)bec;
	const char *hostname, *port, *dbname, *user, *pass;

	user = &val->username[0];
	pass = &val->password[0];
	hostname = &val->hostname[0];
	port = &val->hostport[0];

	if (db)
	{
		dbname = vh_str_buffer(db);
	}

	beconn->gr = GRDBconnect(hostname, port, dbname, user, pass);

	if (beconn->gr)
		return true;

	return false;
}

