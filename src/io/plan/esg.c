/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/executor/estep.h"
#include "io/nodes/NodeQuery.h"
#include "io/plan/esg.h"
#include "io/plan/tree.h"
#include "io/shard/Beacon.h"
#include "io/utils/kvlist.h"

/*
 * We declare the submodule launcher routines explicitly here.  They will be
 * implemented in the corresponding esg_*.c file.
 */

#define ESG_SUBMODULE_DECLARE(subm)	ExecStepGroup \
									vh_esg_## subm ##_generate(ExecStepGroupOpts, \
		   													   NodeQuery, \
	   														   enum ExecStepTag)

static void esg_discover_shards(ExecStepGroupOpts opts);

ESG_SUBMODULE_DECLARE(ddl);
ESG_SUBMODULE_DECLARE(del);
ESG_SUBMODULE_DECLARE(ins);
ESG_SUBMODULE_DECLARE(sel);
ESG_SUBMODULE_DECLARE(upd);


/*
 * vh_esg_generate
 *
 * Processes specific ExecStepGroupOpts and the passes the NodeQuery tree to
 * the appropriate submodule.
 */
ExecStepGroup
vh_esg_generate(ExecStepGroupOpts opts, NodeQuery nquery,
				enum ExecStepTag est_hint)
{
	struct PlanTreeOptsData opts_pt = { };

	vh_plan_tree(nquery, &opts->pt, &opts_pt);

	if (opts->detect_shards)
		esg_discover_shards(opts);

	switch (nquery->action)
	{
	case DDLCreateTable:
	//	return vh_esg_ddl_generate(opts, nquery, est_hint);
		return 0;

	case Delete:
		return vh_esg_del_generate(opts, nquery, est_hint);

	case Insert:
	//	return vh_esg_ins_generate(opts, nquery, est_hint);
		return 0;

	case Select:
		return vh_esg_sel_generate(opts, nquery, est_hint);

	case Update:
		return vh_esg_upd_generate(opts, nquery, est_hint);

	default:

		return 0;
	}

	return 0;
}



/*
 * COMMON ROUTINES SHARED ACROSS ALL ESG SUBMODULES
 */

ExecStepGroup
vh_esg_create(void)
{
	ExecStepGroup esg = vhmalloc(sizeof(struct ExecStepGroupData));

	memset(esg, 0, sizeof(struct ExecStepGroupData));

	return esg;
}

void
vh_esg_destroy(ExecStepGroup esg)
{
	if (esg)
		vhfree(esg);
}

void
vh_esg_addstep(ExecStepGroup esg, ExecStep es)
{
	if (esg->top)
	{
		esg->bottom->child = es;
		es->parent = esg->bottom;
		esg->bottom = es;
	}
	else
	{
		esg->top = es;
		esg->bottom = es;
	}

	esg->depth++;
}

void
vh_esg_addsibling(ExecStep tree, ExecStep sibling)
{
	sibling->parent = tree->parent;
	sibling->sibling = tree->sibling;
	tree->sibling = sibling;
}

bool
vh_esg_valid(ExecStepGroup esg)
{
	return esg && esg->depth > 0;
}

static void 
esg_discover_shards(ExecStepGroupOpts opts)
{
	PlanTree pt = opts->pt;
	SList htps = 0;
	Beacon beacon;
	KeyValueListIterator it;

	vh_kvlist_it_init(&it, pt->htp_beacons);

	while (vh_kvlist_it_next(&it, &beacon, htps))
	{
		//vh_beac_discover_htps(beacon, htps, &opts->shtm);	
	}
}


