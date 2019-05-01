/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef ih_datacatalog_executor_params_H
#define ih_datacatalog_executor_params_H

/* 
 * ParameterData represents:
 * Actual parameter data serialized by the planner or executor (it's a runtime
 * decision the XAct make or the user if spawned without a transaction).  The
 * planner will generate shard specific statements.  It's possible the quals
 * will change if multiple shards are accessed.  Therefore the planner
 * populates a ParamTransferMap that bind the back end specific
 * serialization functions and options to each ParamTransfer structure.
 *
 * The ParameterData structure is the only parameter data visible to the back
 * end at execution.
 *
 * vh_param_createlist_ptm takes a ParamTransferMap and forms a ParameterList
 * for a given ShardAccess.  It may call back end specific serialization 
 * functions if  available for the target Type.  Otherwise it will call 
 * generic Type serialization functions.  This allows us to keep parameter 
 * serialization in one spot, instead of in each unique backend.  When the 
 * planner or executor eventually forms ParameterData structures, it does so
 * with optional guidance of the back end.  For example, in the Postgres world 
 * we also try to ship an OID.  We call the backend to create a ParameterData 
 * superstructure, execute the serialization functions, populating the value, 
 * size, and null fields.  We then call a function prototype provided by the 
 * back end with the following parameters: struct ParameterData*, Type,
 * void *data.  This allows the back end to fill out it's specific
 * attributes, such as the OID in Postgres.
 *
 * When the PlannedStmtShard hits the back end's executor, all it has to do
 * is iterate the ParameterList and bind the values.
 */

typedef struct ShardAccessData *ShardAccess;
typedef struct ParameterData *Parameter;
typedef struct ParameterListData *ParameterList;

/* Defined in plan/paramt.h */
typedef struct ParamTransferMapData *ParamTransferMap;

typedef void (*vh_param_dl_cb)(ParameterList pl, Parameter p, void *data);
typedef void (*vh_param_destroy_func)(Parameter p, void *data);

struct ParameterData
{
	struct
	{
		vh_param_destroy_func func;
		void *data;
	} destroy;

	ParameterList list;
	Parameter next;
	void *value;
	int32_t size;

	bool null;
	bool free;
};

ParameterList vh_param_createlist(void);
ParameterList vh_param_createlist_ptm(ParamTransferMap ptm, ShardAccess sa);

void vh_param_destroylist(ParameterList pl, vh_param_dl_cb dl_cb,
							   void *dl_cb_data);

MemoryContext vh_param_switchmctx(ParameterList pl);

void vh_param_add(ParameterList pl, Parameter pm);
void* vh_param_create(ParameterList pl, size_t sz);
void vh_param_destroy(Parameter pm);

int32_t vh_param_count(ParameterList pl);

void vh_param_it_init(ParameterList pl);
void* vh_param_it_first(ParameterList pl);
void* vh_param_it_next(ParameterList pl);
void* vh_param_it_last(ParameterList pl);

#endif

