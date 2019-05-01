/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_xactq_H
#define vh_datacatalog_executor_xactq_H

/*
 * Transaction module manages all interactions with the various storage engines.
 * On a commit or rollback, the module reserves the right to publish changes to
 * one or all specified storage engines for a given query.  This means the XAct
 * sub-module may receive PreparedStatement targeting a Postgres instance and
 * add additional targets to the object depending on the effected targets.  Thus
 * when a commit or rollback is called, all targets are updated simultaneously.
 *
 * There are three modes a transaction can operate:
 *	1)	Immediate - all queries are flushed immediately within their own transaction
 *		space.  No implicit commits are required from the user.  In fact, in this
 *		mode calling XActCommit() or XActRollback() has no effect on the underlying 
 *		storage engine.
 *
 *			Query Order:
 *				SELECT;
 *				UPDATE;
 *
 *			Pattern:
 *				BEGIN TRANSACTION;
 *					SELECT;
 *				END TRANSACTION;
 *				BEGIN TRANSACTION;
 *					UPDATE;
 *				END TRANSACTION;
 *
 *	2)	Serialized - this is not to be confused with some database implementations
 *		of a serialized isolation level (i.e. Postgres).  If several write queries
 *		have built up without an explicit commit and a read query arrives in the
 *		transaction, the writes will be bundled into a database level subtransaction
 *		and committed.  Then the read query will execute in real time in a higher level
 *		transaction.  When XActCommit or XActRollback is explicitly called, the top level
 *		storage engine transaction will be committed.  Thus, prior writes can be visible
 *		at the storage engine layer to reads and still be rolled back if necessary.
 *
 *			Query Order:
 *				UPDATE;
 *				INSERT;
 *				INSERT;
 *				SELECT;
 *				ROLLBACK;
 *				
 *			Pattern:
 *				BEGIN TRANSACTION;
 *					BEGIN TRANSACTION;
 *						UPDATE;
 *						INSERT;
 *						INSERT;
 *					COMMIT TRANSACTION;
 *					SELECT;				** Has visibility to any changes made by the queued writes
 *				ROLLBACK;
 *
 * To accomplish this, the top level transaction contains a parented red-black
 * tree with all of the connections used in the transaction stack.  The key
 * value is defined by the ShardInstanceId.  The value is a POD containing the
 * following:
 *	1)	standard red-black tree which holds each sub-transaction (including the
 *		top level transaction) requiring a connection to the ShardInstanceId
 *	2)	transaction state (idle, open)
 *
 * Each transaction will spawn two memory pools:
 *	1)	generic pool - used for allocating all statements, resultsets, etc.
 *		within the transactions
 *	2)	object pool - used to store any objects obtained by the queries
 *
 *	Upon transaction destruction, these pools are destroyed rather than attempting
 *	to vhfree every single allocation.  The alloc'd transaction is allocated
 *	directly in its parent's generic pool.
 */

typedef enum XActMode
{
	Immediate,
	Serialized
} XActMode;


XAct vh_xact_create(XActMode mode);
void vh_xact_destroy(XAct xact);

bool vh_xact_commit(XAct xact);
bool vh_xact_rollback(XAct xact);
bool vh_xact_node(XAct xact, NodeQuery query, ExecResult* result);

bool vh_xact_isopen(XAct xact);

HeapBufferNo vh_xact_hbno(XAct xact);

HeapTuplePtr vh_xact_createht(XAct xact, TableDefVer tdv);

#define vh_xact_gettop() ((vh_ctx())->xactTop)
#define vh_xact_getcurrent() ((vh_ctx())->xactCurrent)

#endif

