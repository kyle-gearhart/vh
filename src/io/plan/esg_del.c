/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/shard/Shard.h"
#include "io/executor/estep.h"
#include "io/plan/esg.h"
#include "io/plan/esg_quals.h"
#include "io/plan/pstmt_funcs.h"
#include "io/nodes/NodeQueryDelete.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQual.h"
#include "io/utils/SList.h"

//
/*
 * There are 4 strategies available to perform a deletion.  Deletes are the
 * simplest operation available.
 *
 * ASSUMPTIONS:
 * 	> "Bulk" --- more than fifty (50) records
 * 	> "Multiple" --- fifty (50) records or less
 *
 * STRATEGIES:
 * 	1)	One with single/many primary key
 * 	2)	Multiple with single primary key
 * 	3)	Multiple with many primary key
 * 	4)	Bulk with single/many primary key
 */

static ExecStepGroup esg_del_bulk(ExecStepGroupOpts opts, NodeQueryDelete nqdel,
					   			  TableDef td, SList htps);
static ExecStepGroup esg_del_mul_singlepk(ExecStepGroupOpts opts, 
										  NodeQueryDelete nqdel,
				   						  TableDef td, SList htps);
static ExecStepGroup esg_del_single(ExecStepGroupOpts opts, 
									NodeQueryDelete nqdel,
									TableDef td, HeapTuplePtr htp);
static ExecStepGroup esg_del_all(ExecStepGroupOpts opts,
								 NodeQueryDelete nqdel,
								 TableDef td, Beacon beacon);

typedef struct ExecStepDiscardData *ExecStepDiscard;

/*
 * vh_esg_del_generate
 *
 * Declared exclusively in esg.c
 */
ExecStepGroup
vh_esg_del_generate(ExecStepGroupOpts opts, NodeQuery nquery,
					enum ExecStepTag est_hint)
{
	NodeQueryDelete nqdel = (NodeQueryDelete) nquery;
	SList htps;
	uint32_t htpsz;
	HeapTuplePtr *htp_head;
	TableDef td;
	TableDefVer tdv;

	if (nqdel->from)
	{
		htps = nqdel->from->htps;
		td = nqdel->from->tdv->td;

		if (htps && td)
		{
			htpsz = vh_SListIterator(htps, htp_head);
			tdv = vh_td_tdv_lead(td);

			if (htpsz == 0)
			{
				/*
				 * No HeapTuplePtr were passed, but that doesn't mean the user
				 * didn't pass a nqual that will work.  esg_del_single is smart
				 * enough to recognize this and continue to form an ExecStepGroup.
				 */

				return esg_del_single(opts, nqdel, td, 0);
			}
			else if (htpsz == 1)
			{
				return esg_del_single(opts, nqdel, td, htp_head[0]);
			}
			else if (htpsz <= 50)
			{
				/*
				 * If the primary key only has only field, just do a delete
				 * with an IN clause.  Otherwise we'll need to build a temporary
				 * table to do the deletion.
				 */
				if (tdv->key_primary.nfields == 1)
					//return esg_del_mul_singlepk(opts, nqdel, td, htps);
					return 0;
				else
					//return esg_del_bulk(opts, nqdel, td, htps);
					return 0;
			}
			else
			{
				//return esg_del_bulk(opts, nqdel, td, htps);
				return 0;
			}
		}
		else
		{
			elog(ERROR1,
				 emsg("ExecStepGroup for a delete statement found a corrupt "
					  "NodeQueryDelete tree!  Either a list of records or a "
					  "TableDef was not present"));
		}	
	}

	return 0;
}

/*
 *
 * esg_del_single
 *
 * Needs to account for table partitioning on the deletion.
 * Needs to check for quals that may cause us to go against all quals.  (i.e. NOT).
 *
 */
static ExecStepGroup 
esg_del_single(ExecStepGroupOpts opts, 
			   NodeQueryDelete nqdel,
			   TableDef td, HeapTuplePtr htp)
{
	ExecStepGroup esg;
	ExecStepDiscard esd;
	HeapTuple ht;
	int32_t qual_comp;
	HeapField *hf_head;
	uint32_t hf_sz;
	Shard shd;
	TableDefVer tdv;

	/*
	 * Make sure the HeapTuplePtr is in the quals to be deleted.  The user may
	 * specify other quals.
	 */

	tdv = vh_td_tdv_lead(td);

	if (htp && (ht = vh_htp(htp)))
	{
		shd = ht->shard;

		if (!shd)
		{
		}

		if (tdv->key_primary.nfields > 0)
		{
			/*
			 * Compare the quals against specific HeapTuple fields and if they
			 * all match up, we don't need to add any extracts.  Just form up
			 * the plan(s) which will happen after this branch closes.
			 */

			qual_comp = vh_esg_quals_comphf(nqdel->quals, ht,
											(HeapField*)&tdv->key_primary.fields[0],
											tdv->key_primary.nfields);

			if (qual_comp)
			{
				vh_esg_quals_addfromht(nqdel->quals, ht,
  									   (HeapField*)&tdv->key_primary.fields[0],
  									   tdv->key_primary.nfields,
  									   false);
			}
		}
		else
		{
			/*
			 * Every field on the tuple must become a qual, insert before any
			 * existing quals passed in.  Be careful, as the user may have
			 * already done this.
			 */

			hf_sz = vh_SListIterator(tdv->heap.fields, hf_head);
			vh_esg_quals_addfromht(nqdel->quals, ht, hf_head, hf_sz, false);
		}

		/*
		 * We're good to go, since we already know what shard this HeapTuple
		 * belongs to because the generic planner does all of the shard 
		 * discovery when HeapTuple are present.  Since there's only going to
		 * be one shard involved, we can call the very simplistic 
		 */

		esg = vh_esg_create();
		esd = vh_es_create(EST_Discard, 0);

		vh_esg_addstep(esg, (ExecStep)esd);
		
		//esd->pstmt = vh_pstmt_generate_from_query(nqdel, shd->access[0]->be);
		//esd->pstmtshd = vh_pstmtshd_generate(esd->pstmt, shd);

		return esg;
	}
	else
	{
		/*
		 * Examine the existing quals to see if we have quals that may indicate
		 * the shard to fire against.
		 */

		/*
		 * Run the delete against every shard in the beacon for the table.
		 */

		//return esg_del_all(opts, nqdel, td, td->beacon);
		return 0;
	}

	return 0;
}

