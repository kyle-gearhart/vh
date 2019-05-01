/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef griddb_client_int_H
#define gribdb_client_int_H

#include "io/utils/tcpstream.h"

#define EE_MAGIC_NUMBER 			65021048
#define SPECIAL_PARTITION_ID		0

typedef enum
{
	CONNECT,
	DISCONNECT,
	LOGIN,
	LOGOUT,
	GET_PARTITION_ADDRESS,
	GET_CONTAINER,
	GET_TIME_SERIES,
	PUT_CONTAINER,
	PUT_TIME_SERIES,		
	DROP_COLLECTION,
	DROP_TIME_SERIES,		
	CREATE_SESSION,
	CLOSE_SESSION,
	CREATE_INDEX,
	DROP_INDEX,
	CREATE_EVENT_NOTIFICATION,
	DROP_EVENT_NOTIFICATION,
	FLUSH_LOG,
	COMMIT_TRANSACTION,
	ABORT_TRANSACTION,
	GET_ROW,
	QUERY_TQL,
	QUERY_COLLECTION_GEOMETRY_RELATED,
	QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
	PUT_ROW,
	PUT_MULTIPLE_ROWS,
	UPDATE_ROW_BY_ID,
	DELETE_ROW,
	DELETE_ROW_BY_ID,
	APPEND_TIME_SERIES_ROW,
	GET_TIME_SERIES_ROW,
	GET_TIME_SERIES_ROW_RELATED,
	INTERPOLATE_TIME_SERIES_ROW,
	AGGREGATE_TIME_SERIES,
	QUERY_TIME_SERIES_TQL,
	QUERY_TIME_SERIES_RANGE,
	QUERY_TIME_SERIES_SAMPLING,
	PUT_TIME_SERIES_ROW,
	PUT_TIME_SERIES_MULTIPLE_ROWS,
	DELETE_TIME_SERIES_ROW,
	GET_CONTAINER_PROPERTIES,
	GET_MULTIPLE_ROWS,
	GET_TIME_SERIES_MULTIPLE_ROWS,
	GET_PARTITION_CONTAINER_NAMES,
	DROP_CONTAINER,
	CREATE_MULTIPLE_SESSIONS,
	CLOSE_MULTIPLE_SESSIONS,
	EXECUTE_MULTIPLE_QUERIES,
	GET_MULTIPLE_CONTAINER_ROWS,
	PUT_MULTIPLE_CONTAINER_ROWS,
	CLOSE_ROW_SET,
	FETCH_ROW_SET,
	CREATE_TRIGGER,
	DROP_TRIGGER,
	GET_USERS,
	PUT_USER,
	DROP_USER,
	GET_DATABASES,
	PUT_DATABASE,
	DROP_DATABASE,
	PUT_PRIVILEGE,
	DROP_PRIVILEGE,
} GRDBstatement;

enum GSType
{
	GST_STRING,
	GST_BOOL,
	GST_BYTE,
	GST_SHORT,
	GST_INTEGER,
	GST_LONG,
	GST_FLOAT,
	GST_DOUBLE,
	GST_TIMESTAMP,
	GST_GEOMETRY,
	GST_BLOB,
	GST_STRING_ARRAY,
	GST_BOOL_ARRAY,
	GST_SHORT_ARRAY,
	GST_INTEGER_ARRAY,
	GST_LONG_ARRAY,
	GST_FLOAT_ARRAY,
	GST_DOUBLE_ARRAY,
	GST_TIMESTAMP_ARRAY
};

typedef enum
{
	NONE,
	BASIC,
	CHALLENGE
} GRDBauthMode;

typedef enum
{
	SR_SUCCESS,
	SR_STATEMENT_ERROR,
	SR_NODE_ERROR,
	SR_DENY
} GRDBstatementResult;

struct GRDBconnData
{
	struct TcpStreamConnectionData conn;

	GRDBauthMode auth_mode;
	int32_t protocolVersion;
	int32_t eeHeadLength;

	char *database;
	char *user;
	char *pass;

	int32_t txTimeout;
	bool connected;
	bool loggedIn;
	bool ipv6Enabled;
};

struct GRDBbufferData
{
	struct TcpBufferData buf;
	int32_t lenword_at;
};

typedef struct GRDBconnData *GRDBconn;
typedef struct GRDBbufferData *GRDBbuffer;

GRDBbuffer grdb_buffer_create(void);

GRDBconn grdb_conn_bec(BackEndConnection bec);

void grdb_tcpbuf_get_str(String str, TcpBuffer buf);
int32_t grdb_tcpbuf_put_str(String str, TcpBuffer buf);
int32_t grdb_tcpbuf_put_cstr(const char *str, TcpBuffer buf);

int32_t executeStatement(GRDBconn conn, int64_t statementId,
						 GRDBbuffer input, TcpBuffer *bufout);

int32_t fillRequestHeader(GRDBbuffer buf, GRDBconn conn,
						  GRDBstatement statement, int32_t partitionId,
						  int64_t statementId, bool firstStatement);

typedef struct GRDBoptRequestData *GRDBoptRequest;
struct GRDBoptRequestData
{
	TcpBuffer buffer;
	int32_t lenword_at;
	int32_t body_len;
};

typedef enum
{
	TRANSACTION_TIMEOUT = 1,
	FOR_UPDATE = 2,
	CONTAINER_LOCK_REQUIRED = 3,
	SYSTEM_MODE = 4,
	DB_NAME = 5,
	CONTAINER_ATTRIBUTE = 6,
	ROW_INSERT_UPDATE = 7,
	REQUEST_MODULE_TYPE = 8,
	STATEMENT_TIMEOUT = 10001,
	FETCH_LIMIT = 10002,
	FETCH_SIZE = 10003
} GRDBoptRequestType;

GRDBoptRequest grdb_optrequest_start(TcpBuffer buf);
void grdb_optrequest_pbool(GRDBoptRequest req, GRDBoptRequestType ty, bool val);
void grdb_optrequest_pi32(GRDBoptRequest req, GRDBoptRequestType ty, int32_t val);
void grdb_optrequest_pi64(GRDBoptRequest req, GRDBoptRequestType ty, int64_t val);
void grdb_optrequest_pstr(GRDBoptRequest req, GRDBoptRequestType ty, String str);
void grdb_optrequest_pcstr(GRDBoptRequest req, GRDBoptRequestType ty, const char *str);
void grdb_optrequest_finish(GRDBoptRequest req);

#endif

