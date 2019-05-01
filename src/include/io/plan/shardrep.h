/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_plan_shardrep_H
#define vh_datacatalog_plan_shardrep_H

/*
 * Shard Replacement
 *
 * vh_plan_sr_create:
 * 	1)	Adds/modifies Node objects with HeapTuple specific to the passed
 * 		Shard.  Maintains an unwind structure to undo these changes.
 * 	2)	Updates the ParamTransferMap and ParamTransfer structures with the
 * 		appropriate back end specific serialization functions
 * 	3)	Flattens the resulting NodeQuery structure in place, with complete
 * 		unwind mapping to undo the changes.
 *
 * A user may then call the back end to form a command and complete a
 * PlannedStmt.
 *
 * vh_plan_sr_restore:
 * 	1)	Restores the NodeQuery structure passed to vh_plan_sr_create to its
 * 		original state.
 */

typedef struct ShardRepData *ShardRep;

ShardRep vh_plan_sr_create(NodeQuery nq, Shard shard,
						   void *htp_shard_map,
						   ParamTransferMap ptm);
void vh_plan_sr_destroy(ShardRep sr);
void vh_plan_sr_restore(ShardRep sr);

#endif

