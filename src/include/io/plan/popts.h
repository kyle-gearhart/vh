/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_plan_popts_H
#define vh_datacatalog_plan_popts_H

typedef struct BackEndConnectionData *BackEndConnection;

typedef struct PlannerOpts
{
	HeapBufferNo hbno;
	MemoryContext mctx_result;

	/*
	 * We can pass a BackEnd connection directly to the planner to
	 * avoid having to do a shard lookup.
	 */
	BackEndConnection bec;

	/*
	 * Used to force shard(s) into the planner and avoid Beacon
	 * discovery if a beacon is available.
	 */
	SList shards;
	Shard shard;

	/*
	 * Used to force a relation fetch execution node, this will
	 * typically be populated when called from a relationfetch
	 * routine.
	 */
	HeapTuple relf_ht;
	HeapTuple *relf_htl;
	uint32_t relf_htlsz;
} PlannerOpts;


#endif

