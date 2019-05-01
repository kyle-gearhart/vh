/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/be/griddb/griddb-ext.h"
#include "io/catalog/BackEnd.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/shard/Shard.h"

static Shard sh = 0;
static ShardAccess sa = 0;
static ConnectionCatalog cc = 0;

static void test_grdb_setup_connection(void);
static void test_grdb_connect(void);


void
test_grdb_entry(void)
{
	test_grdb_setup_connection();
	test_grdb_connect();
}

static void
test_grdb_setup_connection(void)
{
	BackEndCredentialVal becredval = { };
	BackEndCredential becred;
	
	cc = vh_ctx()->catalogConnection;

	strcpy(&becredval.username[0], "admin");
	strcpy(&becredval.password[0], "password");
	strcpy(&becredval.hostname[0], "127.0.0.1");
	strcpy(&becredval.hostport[0], "10001");

	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_USERNAME);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_PASSWORD);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_HOSTNAME);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_HOSTPORT);

	becred = vh_be_cred_create(BECSM_PlainText);
	vh_be_cred_store(becred, &becredval);

	sa = vh_sharda_create(becred, &vh_be_griddb);
	sa->database = 0;

	sh = vh_shard_create((ShardId){}, sa, sa);
}

void
test_grdb_connect(void)
{
	BackEndConnection beconn;

	beconn = vh_ConnectionGet(cc, sa);

	if (beconn)
	{
		vh_ConnectionReturn(cc, beconn);
	}
}

