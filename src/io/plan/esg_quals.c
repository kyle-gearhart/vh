/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/plan/esg_quals.h"
#include "io/nodes/NodeField.h"
#include "io/nodes/NodeQual.h"
#include "io/utils/SList.h"


/*
 * MapHf support
 */

struct QualMapHfContext
{
	HeapField *hfs;
	pavl_table *tbl;
	uint32_t hfsz;
};

struct QualMapHfEntry
{
	HeapField hf;
	SList quals;
};

static int32_t esg_quals_maphf_comp(const struct QualMapHfEntry *a,
									const struct QualMapHfEntry *b, void *params);
static void esg_quals_maphf_recurse(Node nqual, void *qualmaphfcontext);



/*
 * ChainHtHfEQ Support
 */
struct ChainHtHfEqContext
{
	HeapField *hfs;
	HeapTuple ht;
	uint32_t hfsz;

	int32_t ret;
	bool hit[1];
};

static int32_t esg_quals_chain_hthf_eq(Node root, HeapTuple ht,
									   HeapField *hfs, uint32_t hfsz);
static void esg_quals_chain_hthf_eq_recurse(Node root, void *data);


/*
 * Pull Shard Support
 */

struct PullShardContext
{
	struct ESG_PullShardOpts *opts;
	struct ESG_PullShardRet *ret;

	struct pavl_table *skip_qual;
	SList fetch_shard;

	HeapField *hfs;
	uint32_t hfs_sz;

	bool stop_recurse;
};

static void esg_quals_pullshard_recurse(Node root, void *data);
static bool esg_quals_pullshard_beacfields(struct PullShardContext *psc,
		 								   NodeQual nqual,
		 								   NodeQualS nhf, NodeQualS nval,
		 								   HeapField hf, uint32_t beac_idx);
static int32_t esg_quals_pullshard_comp(const Shard a,
										const Shard b, void *params);



static bool esg_quals_hf_pullrefvalue(NodeQual nqual, 
									  NodeQualS *nqhf, NodeQualS *val,
									  HeapField *hf);

/*
 * Adds a HeapTuple to the qual list, expanding all primary key fields into
 * the qual.  If no primary key exists, then we simply add all fields to
 * the QualList.
 */
void
vh_esg_quals_addfromht(Node nquals, HeapTuple ht, HeapField *hfs, 
					   uint32_t nhfs, bool force)
{
}


/*
 * Takes all of the HeapField passed and finds any quals that target them.  By
 * default we insert all HeapField into the tree.  If we don't find any quals
 * referencing a HeapField we'll remove the field from the tree.
 *
 * Table entries have a HeapField as they key (and first data member), followed
 * by an SList as the value.  The pointer to the entire NodeQual structure is
 * pushed into the SList.
 */
struct pavl_table*
vh_esg_quals_maphf(Node nquals, HeapField *hfs, uint32_t hfsz)
{
	/*
	struct QualMapHfContext qmhf;
	struct QualMapHfEntry *hfentry;
	uint32_t i;


	if (nquals && hfs && hfsz)
	{
		qmhf.hfs = hfs;
		qmhf.hfsz = hfsz;
		qmhf.tbl = pavl_create((pavl_comparison_func*) esg_quals_maphf_comp,
							   0,
							   vh_MemoryContextAllocAVL());

		for (i = 0; i < hfsz; i++)
		{
			hfentry = vhmalloc(sizeof(struct QualMapHfEntry));
			hfentry->hf = hfs[i];
			hfentry->quals = 0;

			pavl_insert(qmhf.tbl, hfentry);
		}
		
		vh_nsql_visit_tree(nquals, esg_quals_maphf_recurse, &qmhf);

		return qmhf.tbl;
	}
	*/

	return 0;
}

static void
esg_quals_maphf_recurse(Node node, void *qualmaphfcontext)
{
	NodeQual nqual;
	NodeQualS nquals[2];
	uint32_t i;
	HeapField hf;

	if (node->tag == Qual)
	{
		nqual = (NodeQual) node;
		nquals[0] = &nqual->lhs;
		nquals[1] = &nqual->rhs;

		for (i = 0; i < 2; i++)
		{
			switch (nquals[i]->format)
			{
			case TableFieldRef:
			case HeapTupleFieldRef:
				{
					TableField tf = nquals[i]->ref;

					if (tf)
						hf = (HeapField)tf;
				}

				break;

			default:
				hf = 0;
				break;
			}

			if (hf)
			{
				/*
				qme_probe.hf = hf;
				qme = pavl_find(qmhf->tbl, &qme_probe);
				
				if (!qme->quals)
					qme->quals = vh_SListCreate();

				vh_SListPush(qme->quals, node);
				*/
				break;
			}
		}
	}
}


static int32_t 
esg_quals_maphf_comp(const struct QualMapHfEntry *a,
					 const struct QualMapHfEntry *b, void *params)
{
	return (a->hf < b->hf ? -1 : a->hf > b->hf);
}


/*
 * vh_esg_quals_comphf
 *
 * Checks a NodeQual tree to see if the quals contain an AND branch with all of the
 * desired fields and values.
 *
 * We need to build chains based on the parent.
 */

int32_t
vh_esg_quals_comphf(Node root, HeapTuple ht,
					HeapField *hfs, uint32_t hfsz)
{
	return 0;
}


static int32_t 
esg_quals_chain_hthf_eq(Node root, HeapTuple ht,
						HeapField *hfs, uint32_t hfsz)
{
	struct ChainHtHfEqContext *chte;
	uint32_t i;
	bool all = true;

	chte = vhmalloc(sizeof(struct ChainHtHfEqContext) +
					hfsz);
	memset(chte, 0, sizeof(struct ChainHtHfEqContext) + hfsz);

	chte->hfsz = hfsz;
	chte->hfs = hfs;
	chte->ht = ht;

	vh_nsql_visit_tree(root, 
					   esg_quals_chain_hthf_eq_recurse, chte);

	for (i = 0; i < hfsz && all; i++)
		all = chte->hit[i];

	return all;
}

/*
 * Mark a hit for the the field it matches the HeapTuple value for the field.
 */
static void 
esg_quals_chain_hthf_eq_recurse(Node root, void *data)
{
	struct ChainHtHfEqContext *ctx = data;
	uint32_t i;
	int32_t comp;
	NodeQual nqual;
	NodeQualS nqual_ref, nqual_val;
	HeapField hf;
	HeapTuple ht;
	void *hf_value;

	if (root->tag == Qual)
	{
		for (i = 0; i <= ctx->hfsz; i++)
		{
			if (i == ctx->hfsz)
			{
				/* 
				 * Reset the values if we made it to the end of the list without
				 * breaking.  Seems better than keeping a hit flag around.
				 */

				nqual_ref = 0;
				nqual_val = 0;
				hf = 0;
			}

			if (!ctx->hit[i])
			{
				nqual = (NodeQual)root;
				esg_quals_hf_pullrefvalue(nqual, &nqual_ref, &nqual_val, &hf);

				if (hf && hf == ctx->hfs[i])
				{
					if (nqual_val->format == HeapTupleFieldRef)
					{
						ht = vh_htp(nqual_val->htp);

						if (ht == ctx->ht &&
							nqual->oper == Eq)
						{
							ctx->hit[i] = true;
							break;
						}
					}
					else
					{
						hf_value = vh_ht_field(ctx->ht, hf);

						if (hf_value)
						{
						}

						if (nqual->oper == Eq)
						{
							//comp = ty->funcs.Comparison(hf_value, &nqual_val->value[0]);
							comp = 0;

							if (comp == 0)
								ctx->hit[i] = true;
						}
					}		
				}
			}
		}
	}
}


/*
 * Finds the HeapField reference NodeQualS and sets the pointer.  Checks to see
 * if the opposite is a value reference of some sort.  If it is, we'll return true
 * otherwise false.  This is helpful if both sides of the qual reference a field.
 */
static bool 
esg_quals_hf_pullrefvalue(NodeQual nqual, 
						  NodeQualS *nqhf, NodeQualS *val,
						  HeapField *hf)
{
	NodeQualS nquals[2];
	uint32_t i;
	bool is_val = false;

	nquals[0] = &nqual->lhs;
	nquals[1] = &nqual->rhs;

	*nqhf = 0;
	*val = 0;

	for (i = 0 ; i < 2; i++)
	{
		switch (nquals[i]->format)
		{
		case TableFieldRef:

			if (hf)
			{
				TableField tf = nquals[i]->ref;
				*hf = (HeapField)tf;
			}

			*nqhf = nquals[i];

			break;

		case QueryFieldRef:

			if (hf)
			{
				NodeField nf = nquals[i]->ref;
				*hf = (HeapField)nf->tf;
			}

			*nqhf = nquals[i];

			break;

		case FieldValueList:

			if (hf)
			{
				NodeField nf = nquals[i]->ref;
				*hf = (HeapField)nf->tf;
			}

			*nqhf = nquals[i];

			break;


		case HeapTupleFieldRef:
		case Constant:
		case ConstantRef:
			*val = nquals[i];
			is_val = true;
			
			break;

		default:

			*val = 0;
			is_val = false;
			break;
		}
	}

	return is_val;
}

/*
 * vh_esg_quals_pullshard
 *
 * Determines all of the relevant shards based on the quals.  We have a few
 * options.  If we want to return all shards in a beacon if we come across a
 * qual that is indeterminate (i.e. NOT), then the |xbeacon| opt will be set
 * by the caller.
 */
void
vh_esg_quals_pullshard(struct ESG_PullShardOpts *opts, Node nquals,
					   struct ESG_PullShardRet *ret)
{
	struct PullShardContext psc;
	MemoryContext mctx_work;
	TableDef td;
	TableDefVer tdv;

	mctx_work = vh_MemoryPoolCreate(vh_mctx_current(),
									8192,
									"ESG Quals Pull Shard Working Set");

	td = opts->td;
	tdv = vh_td_tdv_lead(td);
	psc.opts = opts;
	
	if (tdv)
	{
		if (tdv->key_logical.nfields)
		{
			psc.hfs = (HeapField*)&tdv->key_logical.fields[0];
			psc.hfs_sz = tdv->key_logical.nfields;
		}
		else
		{
			psc.hfs = (HeapField*)&tdv->key_primary.fields[0];
			psc.hfs_sz = tdv->key_primary.nfields;
		}
	}

	if (ret)
	{
		/*
		ret->shards = pavl_create((pavl_comparison_func*) esg_quals_pullshard_comp,
								  0,
								  vh_MemoryContextAllocAVL());
		*/
		/*
		 * We'll use esg_quals_pullshard_comp even though it casts to a Shard rather
		 * than a NodeQual.  It's just comparing the pointer and all we're doing here
		 * is making a log of all of the quals we chained forward thru.
		 */

		/*
		psc.skip_qual = pavl_create((pavl_comparison_func*) esg_quals_pullshard_comp,
									0,
									vh_MemoryContextAllocAVL_ctx(mctx_work));
		*/

		vh_nsql_visit_tree(nquals, esg_quals_pullshard_recurse, &psc);
	}

	vh_mctx_destroy(mctx_work);
}

/*
 * Needs to skip over qual chain values based on the BeaconKey.  We'll examine those
 * later.  We're just interested in finding new quals that aren't a part of a chain.
 *
 * A beacon chain is considered a chain of AND-ed quals with an Eq operator which 
 * match the BeaconKey fields.
 */
static void
esg_quals_pullshard_recurse(Node root, void *data)
{
	struct PullShardContext *psc = data;
	NodeQualS nqual_hf, nqual_val;
	NodeQual nqual;
	HeapField hf;
	uint32_t i;
	bool is_val, is_beackey, is_complete_beac_chain;

	/*
	 * Check to make sure we haven't already chained forward to this qual and
	 * found a satisfactory match.
	 */

	//if (pavl_find(psc->skip_qual, root))
	//{
	//	return;
	//}

	if (root->tag == Qual)
	{
		nqual = (NodeQual)root;

		is_val = esg_quals_hf_pullrefvalue(nqual, &nqual_hf,
				 						   &nqual_val, &hf);

		if (is_val)
		{
			/*
			 * Check if we're in the beacon key list, if we're not we don't
			 * care.
			 */

			for (i = 0, is_beackey = false; i < psc->hfs_sz; i++)
			{
				if (hf == psc->hfs[i])
				{
					is_beackey = true;
					break;
				}
			}

			if (is_beackey)
			{
				is_complete_beac_chain =
					esg_quals_pullshard_beacfields(psc,
												   nqual, 
												   nqual_hf, nqual_val, hf,
												   i);

				if (is_complete_beac_chain)
					return;
			}
		}
	}
}

/*
 * Attempt to detect a Beacon Key chain with nqual's siblings.  If we
 * find one, we'll inject all the quals in the skip_qual table.  We'll
 * also form up a HeapTuple(s) with populated fields to submit to the
 * Beacon.  We assume we'll only submit to the Beacon once during planning.
 */
static bool 
esg_quals_pullshard_beacfields(struct PullShardContext *psc,
		 					   NodeQual nqual,
		 					   NodeQualS nhf, NodeQualS nval,
		 					   HeapField hf, uint32_t beac_idx)
{
	if (nqual->oper == Eq)
	{
		/*
		 * Since we're a beacon key, let's try to chain forward thru the beacon
		 * keys.  First spin up a new HeapTuple for the TableDef in question.  
		 * Then we'll populate the fields on the HeapTuple.  If we hit all of 
		 * the BeaconKeys, we can shove the HeapTuple in the fetch_shard list.
		 *
		 * vh_esg_qual_pullshard will request the Beacon to deliver* the 
		 * shards required.
		 */

		//Type ty = hf->type;

	}
	else
	{
		/*
		 * We need to fetch all of the shards from the Beacon to
		 * query against.  We can set the stop flag on the context,
		 * because as once we pick up all the shards there's no 
		 * since in continuing to recurse down the tree with 
		 * vh_nsql_visit_tree.
		 */
	}

	return false;
}

static int32_t
esg_quals_pullshard_comp(const Shard a,
						 const Shard b, void *params)
{
	return (a < b ? -1 : a > b);
}

