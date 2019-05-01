/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_BackEnd_H
#define vh_datacatalog_BackEnd_H


#include "io/catalog/Type.h"



typedef struct BackEndConnectionData *BackEndConnection;
typedef struct BackEndCredentialData *BackEndCredential;
typedef struct BackEndCredentialVal BackEndCredentialVal;
typedef struct BackEndExecPlanData *BackEndExecPlan;
typedef struct NodeData *Node;
typedef struct ParameterData *Parameter;
typedef struct ParameterListData *ParameterList;

/*
 * We've typedef'd all of the back end function pointers so that these may be
 * easily stood on the stack as a variable.  Otherwise we'd just forget the
 * typedef and define the function pointer directly in the action table.  In
 * theory there's something to gain here beyond just a cluttered namespace.
 */

typedef BackEndConnection (*vh_beat_createconn)(void);
typedef void (*vh_beat_freeconn)(BackEndConnection);

/*
 * vh_beat_connect
 *
 * It's the responsibility of the back end to take a set of credentials and
 * form a connection string.  Thus each back end will have to do the
 * vh_be_cred_retrieve and vh_be_cred_wipe routine.
 */
typedef bool (*vh_beat_connect)(BackEndConnection, BackEndCredentialVal*, String);
typedef bool (*vh_beat_reset)(BackEndConnection);
typedef bool (*vh_beat_disconnect)(BackEndConnection);
typedef bool (*vh_beat_ping)(BackEndConnection);

typedef void (*vh_beat_exec)(BackEndExecPlan);

/*
 * vh_beat_command		Forms a command (i.e. SELECT * FROM a WHERE a.id = $1)
 * vh_beat_param		Creates a parameter and gets the value to transfer
 */
typedef String (*vh_beat_command)(Node nquery, int32_t param_offset, 
								  TypeVarSlot **param_values, int32_t *param_count);
typedef Parameter (*vh_beat_param)(ParameterList pl, 
								   Type *tam_type, TamGetUnion *tam_funcs, 
								   void **tam_formats,
								   void *ptr,
								   bool null);

typedef bool (*vh_beat_savepoint)(BackEndConnection, String sp);
typedef bool (*vh_beat_xactbegin)(BackEndConnection);
typedef bool (*vh_beat_xactcommit)(BackEndConnection);
typedef bool (*vh_beat_xactrollback)(BackEndConnection);
typedef bool (*vh_beat_xactrollbackto)(BackEndConnection, String sp);
typedef bool (*vh_beat_tpccommit)(BackEndConnection);
typedef bool (*vh_beat_tpcrollback)(BackEndConnection);

/*
 * vh_beat_schema_get
 *
 * When a back end does not have the ANSI SQL compliant INFORMATION_SCHEMA
 * infrastructure, we allow a custom schema get function to be provided.
 *
 * In order to avoid changing the signature of the function, we use a structure
 * called SqlInfoSchemeContextData to pass context from InfoScheme.c to the 
 * back end.
 */
typedef struct SqlInfoSchemeContextData
{
	SList schemas;
	HeapBufferNo hbno;
} SqlInfoSchemeContextData, *SqlInfoSchemeContext;

typedef struct SqlInfoSchemePackage (*vh_beat_schema_get)(BackEndConnection, 
														  SqlInfoSchemeContext);


/*
 * Back End Definition
 */

struct BackEndData
{
	int32_t id;
	const char *name;
	HashTable native_types;
	HashTable type_am;

	TypeAM tam;

	struct
	{
		/* Connection */
		vh_beat_createconn createconn;
		vh_beat_freeconn freeconn;

		vh_beat_connect connect;
		vh_beat_reset reset;
		vh_beat_disconnect disconnect;
		vh_beat_ping ping;

		/* Execution */
		vh_beat_exec exec;

		/* Command */
		vh_beat_command command;
		vh_beat_param param;

		/* Transaction */
		vh_beat_savepoint savepoint;
		vh_beat_xactbegin xactbegin;
		vh_beat_xactcommit xactcommit;
		vh_beat_xactrollback xactrollback;
		vh_beat_xactrollbackto xactrollbackto;
		vh_beat_tpccommit tpccommit;
		vh_beat_tpcrollback tpcrollback;

		/* Schema */
		vh_beat_schema_get schemaget;
	} at;
};

#define vh_be_has_schema_op(be)		(be->at.schemaget != 0)

/*
 * BackEndConnectionData
 *
 * We should be care to only contain information about the connection's state.
 * Only stuff that's important to the planner and transaction manager should be
 * included on this structure.
 *
 * Credentials should be stored in another structure, BackEndCredentialData.
 * This allows for clear separation of credentials vs. operating state, minimizing
 * the amount of time credentials are requried to be held in memory (i.e. when
 * the catalog creates and initiates a connection).
 */

struct BackEndConnectionData
{
	BackEnd be;
	char *currentdb;

	bool intx;
	bool in2pc;
};

void* vh_be_conn_create(BackEnd be, size_t sz);
void vh_be_conn_init(BackEndConnection bec);

#define vh_be_conn_intx(bec)		(bec->intx)


/*
 * Back End Credentials
 *
 * We've got two primary structures exposed to users.  The first is the 
 * BackEndCredentialVal structure, which is intended to sit on the stack (i.e.
 * no dynamic allocation thru vhmalloc or similar).  BackEndCredentialVal is
 * intended to simply serve as a very intermediate structure for passing
 * credentials around.  When credentials are first defined, the should be
 * read into a BackEndCredentialVal structure on the stack.
 *
 * The second structure is opaque to callers, known as the BackEndCredential.
 *
 * This allows for some abstraction of the underlying, transient in memory
 * storage method.  Users should always call vh_be_cred_store to set the
 * values to the BackEndCredential structure.  Likewise, calling
 * vh_be_cred_retrieve returns the values on the stack via a BackEndCredentialVal
 * structure.
 *
 * After using the BackEndCredentialVal structure, users should immediately call
 * vh_be_cred_wipe.
 */

typedef enum BackEndSocket
{
	BES_TCIP,
	BES_UNIX,
	BES_URI
} BackEndSocket;

typedef enum BackEndCredStorageMode
{
	BECSM_PlainText,
	BECSM_SHA512
} BackEndCredStorageMode;

#define VH_BE_CREDVAL_MEMBER_COUNT				8
#define VH_BE_CREDVAL_MEMBER_ISNULL(cv, idx)	( cv.nulls[idx] )
#define vh_be_credval_member_set(cv, idx)		( cv.nulls[idx] = true )

#define VH_BE_CREDVAL_USERNAME					0
#define VH_BE_CREDVAL_PASSWORD					1
#define VH_BE_CREDVAL_CLIENT_SSL_URI			2
#define VH_BE_CREDVAL_CLIENT_SSL_KEY			3
#define VH_BE_CREDVAL_SOCKET					4
#define VH_BE_CREDVAL_HOSTNAME					5
#define VH_BE_CREDVAL_HOSTPORT					6
#define VH_BE_CREDVAL_URI						7

#define VH_BE_CREDVAL_USERNAME_ISNULL(cv)		( VH_BE_CREDVAL_MEMBER_ISNULL(cv, VH_BE_CREDVAL_USESRNAME) )
#define VH_BE_CREDVAL_PASSWORD_ISNULL(cv)		( VH_BE_CREDVAL_MEMBER_ISNULL(cv, VH_BE_CREDVAL_PASSWORD) )

struct BackEndCredentialVal
{
	bool nulls[8];
	char username[256];	
	char password[256];

	/*
	 * Client side SSL certificate
	 */
	char client_ssl_uri[256];
	char client_ssl_key[2048];

	/*
	 * Location
	 */
	BackEndSocket socket;
	char hostname[256];
	char hostport[8];
	char uri[256];
};

/*
 * vh_be_cred_create
 *
 * Creates an empty, initialized BackEndCredential structure based on the
 * desired storage mode.
 */

BackEndCredential vh_be_cred_create(BackEndCredStorageMode storagemode);

/*
 * vh_be_cred_create_custom
 *
 * Similar to vh_be_cred_create, but allows the caller to define it's own
 * set of functions for BackEndCredential actions.  These will be invoked
 * by the corresponding top level functions.
 *
 * These should all run synronously.  Currently we do not support actions
 * that are going to block for IO in the planner/transaction manager.
 *
 * Ideally we'd like to get to a point where we can validate an application's
 * rights to a given back end periodically thru an asyncronous technique.
 *
 * This will require significant changes to the planner/transaction manager
 * and still deliver performance.  This goes back to our entire theory 
 * about not wanting to store encryption keys in memory, but rather request
 * them.
 */
BackEndCredential vh_be_cred_create_custom(void *user_data,
										   bool (*create)(BackEndCredential),
										   bool (*store)(BackEndCredential, BackEndCredentialVal*),
										   BackEndCredentialVal (*retrieve)(BackEndCredential),
										   bool (*finalize)(BackEndCredential));

bool vh_be_cred_store(BackEndCredential becred, BackEndCredentialVal *input);
BackEndCredentialVal vh_be_cred_retrieve(BackEndCredential becred);

#define vh_be_cred_wipe(becredval)	( memset(&becredval, 0, sizeof(struct BackEndCredentialVal)) )


/*
 * Back End native types allow for the back end to map it's native types
 * to VH.IO types.  Users may call vh_be_type_getnative to find the VH.IO
 * type registered to the native type's name.  vh_be_type_setnative registers
 * a Type to the back end with it's native name.
 */

Type* vh_be_type_getnative(BackEnd be, const char* ty_name);
const char* vh_be_type_getbe(BackEnd be, Type *tys);

bool vh_be_type_setnative(BackEnd be, const char* ty_name, Type *type);
struct TypeAMFuncs* vh_be_type_getam(BackEnd be, Type type);
bool vh_be_type_hasam(BackEnd be, Type type);
bool vh_be_type_setam(BackEnd be, Type type, TypeAMFuncs funcs);

/*
 * A few helper functions to allow us to connect to Backend without going thru
 * the catalog interface.
 */

BackEndConnection vh_be_connect(BackEnd be, BackEndCredential becred);
bool vh_be_disconnect(BackEndConnection bec);

/*
 * Instead of directly accessing the function table, there are a few BackEnd
 * functions that should be called thru these prototypes.
 *
 * We do a few checks and set a few properties.
 */

bool vh_be_exec(BackEndConnection bec, BackEndExecPlan beep);
bool vh_be_xact_begin(BackEndConnection bec);
bool vh_be_xact_commit(BackEndConnection bec);
bool vh_be_xact_rollback(BackEndConnection bec);

bool vh_be_command(BackEnd be, Node node, String *cmd,
				   int32_t param_offset,
				   TypeVarSlot **param_values, int32_t *param_count);

bool vh_be_param(BackEnd be, ParameterList pl, TypeVarSlot *tvs, Parameter *out);

#endif

