/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/executor/estep.h"
#include "io/nodes/NodeQueryUpdate.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQual.h"
#include "io/plan/esg.h"

/*
 * There are 15 strategies available to perform an update.  Updates are the
 * most complicated to execute due to versioning and primary keys.  The SQL 
 * language does not provide a standard, back end independent way to update 
 * multiple records at once in a single statement.
 *
 * As a result, we have three general flows to update records: single, multiple,
 * and bulk.  Multiple and bulk are further divided depending on the number of
 * columns in the primary key.
 *
 * Single, multiple, and bulk also feature three layers of versioning/locking:
 * none, back end row lock, full in memory comparison with syncronization.
 *
 * It is the responsibility of the esg_upd.c module to acknowledge all shard
 * requirements.  Since the results do not have to be collected into a single
 * list, we're not terribly concerned about shards.  Just match HeapTuplePtr
 * with the correct query for the shard.
 *
 * ASSUMPTIONS:
 * 	> "Bulk" --- more than fifty (50) records
 * 	> "Multiple" --- fifty (50) records or less
 * 	> Only one HeapTupleDef can be contained in a single UPDATE statement
 *
 * STRATEGIES: (ordered from least complex to most)
 * 	1)	One with single/many column primary key with
 * 			a)	UPDATE
 * 	2)	One with single/many column primary key with locking
 * 			a) 	SELECT ... FOR UPDATE
 * 			b)	UPDATE
 * 	3)	One with single/many column primary key with version compare
 * 			a)	SELECT ... FOR UPDATE
 * 			b)	COMPARE
 * 			c)	[CONDITIONAL] UPDATE
 * 	4)	Multiple single column primary key 
 * 			a)	DELETE from target table with IN clause
 * 			b)	BULK insert to actual table
 * 	5)	Multiple single column primary key with locking
 * 			a)	SELECT ... FOR UPDATE from target table with IN clause (DISCARD)
 * 			b)	DELETE from target table with in clause
 * 			c)	BULK insert
 * 	6)	Multiple single column primary key with version compare
 * 			a)	INDEX in memory HeapTuple on PK
 * 			b)	SELECT ... FOR UPDATE from target table with IN clause
 * 				i)	INDEX results on PK
 * 				ii)	JOIN COMPARE with step (a)
 * 			c)	DELETE from target table with JOIN compare IN clause
 * 			d)	BULK insert from the JOIN compare
 */

ExecStepGroup
vh_esg_upd_generate(ExecStepGroupOpts opts, NodeQuery nquery,
					enum ExecStepTag est_hint)
{
	return 0;
}

