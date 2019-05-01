/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_plan_paramt_H
#define vh_datacatalog_plan_paramt_H


/*
 * Parameter Transfer Map (PTM)
 *
 * The planner is responsible for managing query parameters between shards.
 * If possible, we don't want to send values to a back end if we know the values
 * won't be on the shard.  Instead of copying the entire NodeQuery tree and
 * slimming down the parameters for each shard, the planner generates an 
 * intermediate structure for each parameter known as the Parameter Transfer
 * Map (PTM).
 *
 * If a shard independent structure should be created, pass a null ShardAccess
 * pointer to the vh_plan_paramt_add function.  Otherwise we'll assume the
 * user wants to make the particular Node's parameters Shard specific.
 *
 * It's the caller's resonsibility to populate the ParamTransfer structure
 * returned by vh_plan_paramt_add function.
 */

typedef struct ParameterListData *ParameterList;
typedef struct ParamTransferData *ParamTransfer;
typedef struct ParamTransferMapData *ParamTransferMap;
typedef struct ShardAccessData *ShardAccess;

struct ParamTransferData
{
	Type ty_outter;
	Type ty_inner;

	union
	{
		SList slist;
		void *ptr;
		int64_t i64;
		int32_t i32;
		int16_t i16;
		int8_t i8;
		double dbl;
		float flt;
	} data;

	bool is_constant;
};

/* ParamTransferMap functions */
ParamTransferMap vh_plan_paramt_createmap(void);
void vh_plan_paramt_destroymap(ParamTransferMap ptm);

/* ParamTransferMap Iterator functions
 *
 * Iterators work by the user calling the init function with a shard access.
 * Iteration cannot occur without a shard.  Global parameters are iterated
 * in the correct order.
 */
void vh_plan_paramt_it_init(ParamTransferMap ptm, ShardAccess sa);
ParamTransfer vh_plan_paramt_it_next(ParamTransferMap ptm, uint32_t *idx);
ParamTransfer vh_plan_paramt_it_first(ParamTransferMap ptm, uint32_t *idx);
ParamTransfer vh_plan_paramt_it_last(ParamTransferMap ptm, uint32_t *idx);


/* ParamTransfer actions */
ParamTransfer vh_plan_paramt_add(ParamTransferMap ptm, Node node,
   								 ShardAccess sa);

ParameterList vh_plan_paramt_createlist(ParamTransferMap ptm, ShardAccess sa);

#endif

