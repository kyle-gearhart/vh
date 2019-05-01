/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_postgres_impl_Impl_H
#define vh_datacatalog_sql_postgres_impl_Impl_H

#include <libpq-fe.h>

#include "io/catalog/BackEnd.h"
#include "io/executor/param.h"


typedef struct PostgresConnectionData
{
	struct BackEndConnectionData bec;
	PGconn *pgconn;
	PGresult *pgres;
	ConnStatusType connStatus;
	PGTransactionStatusType xactStatus;
} PostgresConnectionData, *PostgresConnection;

typedef struct PgresParameterData
{
	struct ParameterData p;
	Oid oid;
} *PgresParameter;

void vh_pgres_ty_array_register(BackEnd be);
void vh_pgres_ty_date_register(BackEnd be);
void vh_pgres_ty_datetime_register(BackEnd be);
void vh_pgres_ty_range_register(BackEnd be);

#endif

