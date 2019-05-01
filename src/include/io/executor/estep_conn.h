/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_estep_conn_H
#define vh_datacatalog_executor_estep_conn_H

/*
 * vh_es_putconns_cb
 *
 * When passed to vh_es_putconns, the callback will occur at every successful
 * NodeConnection placement.  If the placement occurs from the |kvm| passed in,
 * the from_connection_catalog flag will be false.
 */
typedef void (*vh_es_putconns_cb)(void *data, PlannedStmt pstmt, 
								  PlannedStmtShard pstmtshd,
								  BackEndConnection nconn, 
								  bool from_connection_catalog);

/*
 * vh_es_putconns
 * 
 * Traverses the ExecStep attempting to find ExecSteps requiring connections.
 * 
 * |root|	required	Root ExecStep to begin traversing with
 * |cc|		required	Connection Catalog to obtain connections from
 * |kvm| 	optional	key: ShardAccess; value: NodeConnection
 * |cb|		optional
 * |cb_data|optional
 */ 
bool vh_es_putconns(ExecStep root,
		   			ConnectionCatalog cc,
	   				KeyValueMap *kvm,
	   				vh_es_putconns_cb cb,
					void *cb_data,
					bool add_to_kvm);

void vh_es_relconns(ExecStep root, ConnectionCatalog cc,
					KeyValueMap kvm_exclude);

#endif

