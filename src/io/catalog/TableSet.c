/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/TableSet.h"
#include "io/executor/eplan.h"
#include "io/executor/exec.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/nodes/NodeJoin.h"
#include "io/utils/art.h"
#include "io/utils/kset.h"
#include "io/utils/kvlist.h"
#include "io/utils/kvmap.h"
#include "io/utils/SList.h"


#define tsr_maxrels			10
#define tsr_isroot(t)		(t && !t->parent)

struct TableSetRelData
{
	TableSet ts_owner;
	TableSetRel parent;
	TableDef td;
	String td_alias;
	SList children;							/* TableSetRel */

	TableField qual_inner[tsr_maxrels]; 	/* Child table fields */
	TableField qual_outter[tsr_maxrels];	/* Parent table fields */
	uint32_t nquals;

	uint32_t depth;
	
	String alias_path;
	String name_path;

	union
	{
		KeyValueList kvl_htps;				/* Key: Parent HTPS, Value: Child HTPS */
		SList htps;
	};

	bool fetched;
};

static void tsr_init(TableSetRel tsr, TableSet owner, TableDef td, const char* td_alias);

typedef struct BuildIdxData
{
	MemoryContext mctx;
	TableField *tfs;
	uint32_t ntfs;
	art_tree idx;
	SList unique_htps;
	unsigned char *kb;			/* Key buffer, no sense is reallocating for each HTP */
	size_t kb_len;
} *BuildIdx;

static BuildIdx tsr_buildidx_parent(TableSetRel tsr, bool collect_unique);
static bool tsr_buildidx_parent_cb(HeapTuplePtr htp_inner,
								   HeapTuplePtr htp_outter, void *cb_data);

static SList tsr_fetch_htps(TableSetRel tsr, BuildIdx bidx);
static bool tsr_nest_htps(TableSetRel tsr, BuildIdx bidx, SList htps);

struct TableSetIterData
{
	TableSet ts_owner;
	uint32_t root_idx;
	uint32_t root_count;
	HeapTuplePtr root_htp;
	KeyValueMap kvm_child_htps;		/* Key: TableSetRel; Value: SList of HTPS */
	int8_t direction;
	bool iterating;
};

static TableSetIter tsi_create(TableSet ts);
static void tsi_recurse(TableSetIter tsi, TableSetRel tsr, HeapTuplePtr parent_htp);

struct TableSetData
{
	KeySet ks_tsi;					/* Key: TableSetIter */
	TableSetRel tsr_root;
	SList tsrs;						/* Un-nested list of child relations */
	MemoryContext mctx;
	uint32_t depth;
};





/*
 * vh_ts_create
 *
 * Creates the core TableSet data structures in it's own MemoryContext, within
 * the current MemoryContext.  Callers should be care to ensure they're in the
 * right context, with a lifespan acceptable to their anticipated operations.
 */
TableSet
vh_ts_create(TableDef root, const char* root_alias)
{
	TableSet ts;
	MemoryContext mctx_old, mctx_ts;

	mctx_old = vh_mctx_current();
	mctx_ts = vh_MemoryPoolCreate(mctx_old, 8192,
								  "TableSet root context");

	vh_mctx_switch(mctx_ts);

	ts = vhmalloc(sizeof(struct TableSetData) + sizeof(struct TableSetRelData));
	ts->mctx = mctx_ts;

	ts->tsr_root = (TableSetRel)(ts + 1);
	tsr_init(ts->tsr_root, ts, root, root_alias);
	ts->tsr_root->depth = 1;
	ts->depth = 1;

	ts->ks_tsi = vh_kset_create();
	ts->tsrs = vh_SListCreate();


	mctx_ts = vh_mctx_switch(mctx_old);

	/*
	 * Let's just make sure we didn't mangle the memory context stack here
	 * by checked to see what MemoryContextSwitch returns.  If we don't
	 * get back the mctx in the newly created TableSet, we've got problems!
	 */
	assert(mctx_ts == ts->mctx);

	return ts;	
}

/*
 * vh_ts_root
 *
 * Sets the root records.  We expect the caller to provide us with a list of
 * root records to nest with.
 */
bool
vh_ts_root(TableSet ts, SList htps)
{
	if (!ts->tsr_root->fetched)
	{
		ts->tsr_root->htps = htps;
		ts->tsr_root->fetched = true;

		return true;
	}

	return false;
}

/*
 * tsr_init
 *
 * Initializes a TableSetRel, including several memory allocations so we don't
 * have to deal with it later.  We'll setup the SList of children and also
 * stand up the qual array.  It's fixed width.
 */
static void
tsr_init(TableSetRel tsr, TableSet ts, TableDef td, const char* alias)
{
	tsr->ts_owner = ts;
	tsr->td = td;
	tsr->td_alias = vh_str.Convert(alias);
	
	tsr->parent = 0;
	tsr->children = vh_SListCreate();

	tsr->nquals = 0;

	tsr->alias_path = 0;
	tsr->name_path = 0;

	tsr->kvl_htps = 0;
	tsr->htps = 0;

	tsr->fetched = false;
}

/*
 * vh_tsr_root_create
 *
 * All we'll do here is grab the TableSetRel from the root context and pass it
 * over to vh_tsr_create_child.  Consider this a convienence function.
 */
TableSetRel
vh_tsr_root_create_child(TableSet ts, TableDef td, const char* child_alias)
{
	return vh_tsr_create_child(ts->tsr_root, td, child_alias);
}

/*
 * vh_tsr_create_child
 *
 * Just create a new TableSetRel and do the proper nesting.
 */
TableSetRel
vh_tsr_create_child(TableSetRel tsr_parent, TableDef td, const char* child_alias)
{
	MemoryContext mctx_old, mctx_ts;
	TableSet ts = tsr_parent->ts_owner;
	TableSetRel tsr_child;

	mctx_old = vh_mctx_switch(ts->mctx);
	
	tsr_child = vhmalloc(sizeof(struct TableSetRelData));	
	tsr_init(tsr_child, ts, td, child_alias);
	tsr_child->parent = tsr_parent;
	tsr_child->depth = tsr_parent->depth + 1;

	if (tsr_child->depth > tsr_parent->ts_owner->depth)
		tsr_parent->ts_owner->depth = tsr_child->depth;

	vh_SListPush(tsr_parent->children, tsr_child);

	mctx_ts = vh_mctx_switch(mctx_old);

	assert(mctx_ts == ts->mctx);
	
	return tsr_child;
}

/*
 * vh_tsr_push_qual
 *
 * Pushes a qual onto a TableSetRel.
 */
bool
vh_tsr_push_qual(TableSetRel tsr, TableField tf_parent, TableField tf_child)
{
	TableDef td_child, td_parent;

	if (!tsr->parent)
	{
		elog(WARNING,
			 emsg("No parent relationship defined for the TableSetRel passed!"));
	
		return false;
	}

	td_child = tsr->td;
	td_parent = tsr->parent->td;

	if (tf_parent->tdv->td != td_parent)
	{
		elog(ERROR1,
			 emsg("The parent TableField is not from the same TableDef object that's "
				  "been defined in the TableSetRel!  Unable to map the relation."));

		return false;
	}

	if (tf_child->tdv->td != td_child)
	{
		elog(ERROR1,
			 emsg("The child TableField is not from the same TableDef object that's "
				  "been defined in the child TableSetRel!  Unable to map the relation!"));

		return false;
	}

	tsr->qual_inner[tsr->nquals] = tf_child;
	tsr->qual_outter[tsr->nquals] = tf_parent;
	tsr->nquals++;

	return true;	
}

/*
 * vh_tsr_fetch_all
 *
 * We fetch all of the relationships from the underlying source database.  This
 * can get a little tricky for one big reason.  We generally don't want to
 * sacrifice speed.  To obtain speed, we ask the executor to do most of the work
 * for us.  Instead of getting back an SList of HeapTuplePtr, we're going to get
 * back atleast one, if not multiple indexes.  
 *
 * We'll always:
 * 		> request an index on the fields defining the parent relation
 * 		> request indexes on all child relations
 *
 * We'll sometimes:
 * 		> request an index on the Primary Key (PK)
 *
 * We reserve the right to re-order relationship quals to minimize index creation.
 * The re-ordering is why we only allow AND with equality operators in 
 * relationship quals.
 *
 * PARENT
 * 	Index on PK++
 * 		CHILD
 * 			Index on PK++
 * 			Index on relationship to parent
 * 			Indexes on relationships to children
 *
 * ++ Only requested if the TableDef has multiple relations defined.  We don't
 * want the same record (as determined by the PK) stood up in multiple
 * HeapTuplePtr.  If a user decides to make a change on said record in one
 * relationship we expect that change to cascade thru to other relationships.
 *
 * Thus we work the tree recursively: from top to bottom moving right thru the
 * sibling relationships at each level.  This takes a good bit of planning to
 * get right.  We can't just be keeping indexes around.  They're expensive
 * because we generally end up duplicate copies of the key: once on the
 * HeapTuple itself and then on the indexes.
 *
 * "Attaching" child HeapTuplePtr to the parent relation is easy.  We hope the
 * parent still has an index lying around that has the same fields (and order!)
 * as the relation to the child.  We then simply intersect our two indexes, 
 * pushing values into |kvl_htps|.  Based on our underlying fetch methodology
 * (create a temporary table to join to the child data source) we won't get
 * any "naked" child records that won't have a parent.  We may have parents that
 * do not have any children.  We do a lot of work to get to our end goal of
 * attaching a child to a parent.
 *
 * We're also going to send back a whole bunch of statistics to the caller about
 * how we did.  This stack should be modular enough so that we can fetch a
 * single TableSetRel without a lot of additional hacking.
 */

bool
vh_tsr_fetch_all(TableSet ts)
{
	return false;
}

/*
 * vh_tsr_fetch
 *
 * Fetches a given relation.  May result in us walking up the parent child tree
 * to fetch parent relationships.  We really just need to build an index on the
 * parent.
 */

bool
vh_tsr_fetch(TableSetRel tsr)
{
	TableSetRel tsr_parent = tsr->parent;
	BuildIdx bidx;
	SList htps;

	if (!tsr_parent->fetched && !tsr_isroot(tsr_parent))
		vh_tsr_fetch(tsr_parent);

	if (!tsr_parent->fetched && tsr_isroot(tsr_parent))
	{
		/*
		 * Let's get out of a potentially recursive stack but kicking up an
		 * error.  The caller should know they'll want this is in a
		 * TRY...CATCH block.
		 *
		 * We've made it all the way up to the root relationship and there
		 * are no base records.
		 */

		elog(ERROR1,
			 emsg("Root relationship does not have any records.  Supply root "
				  "relation records prior to calling vh_tsr_fetch or "
				  "vh_tsr_fetch_all!"));

		return false;
	}

	/*
	 * We should probably build the parent index and also build the
	 * temporary table to JOIN against.  We don't want to pull down any
	 * "hanging chad" child records because we don't have the parent in our
	 * current working set.
	 */

	bidx = tsr_buildidx_parent(tsr, false);

	if (!bidx)
	{
	}

	/*
	 * Now we've got a unique index on the parent so we can build our query
	 * to fetch all of the records in the relationship.
	 */

	htps = tsr_fetch_htps(tsr, bidx);

	if (htps)
		tsr_nest_htps(tsr, bidx, htps);

	art_tree_destroy(&bidx->idx);

	vh_mctx_destroy(bidx->mctx);

	return true;
}

size_t
vh_tsri_ht(TableSetRel tsr, vh_tsri_ht_cb cb, void *cb_data)
{
	SList htps;
	HeapTuplePtr *htp_head, htp_parent;
	HeapTuple ht_outter, ht_inner;
	KeyValueListIterator it;
	uint32_t htp_sz, i;
	size_t counter = 0;

	if (tsr->parent)
	{
		vh_kvlist_it_init(&it, tsr->kvl_htps);

		while (vh_kvlist_it_next(&it, &htp_parent, &htps)) 
		{
			ht_outter = vh_htp(htp_parent);
			htp_sz = vh_SListIterator(htps, htp_head);

			for (i = 0; i < htp_sz; i++)
			{
				ht_inner = vh_htp(htp_head[i]);

				if (!cb(ht_inner, htp_head[i],
						ht_outter, htp_parent,
						cb_data))
				{
					return counter += i;
				}
			}

			counter += i;
		}

		return counter;
	}
	else
	{
		htp_sz = vh_SListIterator(tsr->htps, htp_head);

		for (i = 0; i < htp_sz; i++)
		{
			ht_inner = vh_htp(htp_head[i]);

			if (!cb(ht_inner, htp_head[i], 0, 0, cb_data))
				return i;
		}

		return i;
	}
}

/*
 * vh_tsri_htp
 *
 * Cycles thru all of the records in a TableSetRel and calls the cb for each
 * one of them.
 */
size_t
vh_tsri_htp(TableSetRel tsr, vh_tsri_htp_cb cb, void *cb_data)
{
	SList htps;
	HeapTuplePtr *htp_head, htp_parent;
	uint32_t htp_sz, i;
	KeyValueListIterator it;
	size_t counter = 0;

	if (tsr->parent)
	{
		vh_kvlist_it_init(&it, tsr->kvl_htps);
		
		while (vh_kvlist_it_next(&it, &htp_parent, &htps))
		{
			htp_sz = vh_SListIterator(htps, htp_head);

			for (i = 0; i < htp_sz; i++)
				if (!cb(htp_head[i], htp_parent, cb_data))
					return counter + i;

			counter += i;
		}

		return counter;
	}
	else
	{
		htp_sz = vh_SListIterator(tsr->htps, htp_head);

		for (i = 0; i < htp_sz; i++)
			if (!cb(htp_head[i], 0, cb_data))
				return i;

		return i;
	}
}

static BuildIdx
tsr_buildidx_parent(TableSetRel tsr, bool collect_unique)
{
	MemoryContext mctx_old, mctx_bidx;
	BuildIdx bidx;

	mctx_bidx = vh_MemoryPoolCreate(tsr->ts_owner->mctx, 8192,
									"tsr_buildidx_parent");
	mctx_old = vh_mctx_switch(mctx_bidx);

	bidx = vhmalloc(sizeof(struct BuildIdxData));
	bidx->tfs = &tsr->qual_outter[0];
	bidx->ntfs = tsr->nquals;
	bidx->mctx = mctx_bidx;
	bidx->kb = vhmalloc(256);
	bidx->kb_len = 256;

	art_tree_init(&bidx->idx);

	if (collect_unique)		
		bidx->unique_htps = vh_SListCreate();
	else
		bidx->unique_htps = 0;

	vh_tsri_htp(tsr->parent, tsr_buildidx_parent_cb, bidx);

	mctx_bidx = vh_mctx_switch(mctx_old);

	return bidx;	
}

/*
 * tsr_buildidx_parent_cb
 *
 * Takes a HeapTuplePtr on htp_inner and extracts the key into a contigous 
 * set of memory.  We then shove that array down to the underlying index and
 * let it find the entry.  If there's no entries for the key, we'll just store
 * the HeapTuplePtr as the value.  If an entry already exists, we should promote
 * the value to an SList if it isn't already.  In the future we may give users
 * the option to enfore a unique index on TableSetRel.  For now, just line 'em
 * up.
 */
static bool
tsr_buildidx_parent_cb(HeapTuplePtr htp_inner,
					   HeapTuplePtr htp_outter,  void *cb_data)
{
	BuildIdx bidx = cb_data;
	HeapTuple ht = vh_htp(htp_inner);
	size_t key_sz = 0;
	void* idx_val;
	SList htps;

	key_sz = vh_ht_formkey(bidx->kb, bidx->kb_len, ht,
						   (HeapField*)bidx->tfs, bidx->ntfs);

	if (key_sz)
	{
		idx_val = art_search(&bidx->idx, bidx->kb, key_sz);

		if (idx_val)
		{
			htps = idx_val;
			vh_htp_SListPush(htps, htp_inner);
		}
		else
		{	
			vh_htp_SListCreate(htps);
			vh_htp_SListPush(htps, htp_inner);

			art_insert(&bidx->idx, bidx->kb, key_sz, htps);

			if (bidx->unique_htps)
				vh_htp_SListPush(bidx->unique_htps, htp_inner);
		}
	}

	return true;
}

static SList
tsr_fetch_htps(TableSetRel tsr, BuildIdx bidx)
{
	NodeQuerySelect nqsel;
	NodeFrom nf;
	NodeJoin nj;
	uint32_t i;
	ExecResult er;

	nqsel = vh_sqlq_sel_create();
	nf = vh_sqlq_sel_from_add(nqsel, tsr->td, 0);
	nj = vh_sqlq_sel_join_add(nqsel, tsr->parent->td, 0);

	vh_sqlq_sel_from_addfields(nqsel, nf, 0);
	nj->join_table.htps = bidx->unique_htps;

	for (i = 0; i < tsr->nquals; i++)
		vh_nsql_join_qual_addtf(nj, tsr->qual_inner[i], tsr->qual_outter[i]);

	er = vh_exec_node((Node)nqsel);

	if (er)
	{
		tsr->fetched = true;

		return er->tups;
	}

	return 0;	
}

/*
 * tsr_nest_htps
 *
 * Nests an SList of HTPS using a pre-built index to identify the parent
 * relations.
 */
static bool
tsr_nest_htps(TableSetRel tsr, BuildIdx bidx, SList htps)
{
	HeapTuplePtr *htp_head, *htp_idx_head, htp, htp_idx;
	HeapTuple ht;
	uint32_t i, j, htp_sz, htp_idx_sz;
	size_t key_sz;
	SList parent_htps, nested_htps;

	if (!tsr->kvl_htps)
		tsr->kvl_htps = vh_htp_kvlist_create();

	htp_sz = vh_SListIterator(htps, htp_head);

	for (i = 0; i < htp_sz; i++)
	{
		htp = htp_head[i];
		ht = vh_htp_immutable(htp);

		key_sz = vh_ht_formkey(bidx->kb, bidx->kb_len, ht,
							   (HeapField*)&tsr->qual_inner[0], tsr->nquals);

		if (key_sz == 0)
			continue;

		parent_htps = art_search(&bidx->idx, bidx->kb, key_sz);

		if (parent_htps)
		{
			htp_idx_sz = vh_SListIterator(parent_htps, htp_idx_head);

			for (j = 0; j < htp_idx_sz; j++)
			{
				htp_idx = htp_idx_head[j];

				vh_kvlist_value(tsr->kvl_htps, &htp_idx, nested_htps); 
				vh_htp_SListPush(nested_htps, htp);
			}		
		}
	}

	return true;
}

/*
 * tsi_create
 *
 * Creates a TableSetIter stub.  The caller should set the direction and the 
 * root_idx if the direction is backwards.
 */
static TableSetIter
tsi_create(TableSet ts)
{
	MemoryContext mctx_old, mctx_ts;
	TableSetIter tsi;
	TableSetRel *tsr_head, tsr;
	uint32_t tsr_sz, i;
	SList *htps;

	mctx_old = vh_mctx_switch(ts->mctx);

	tsi = vhmalloc(sizeof(struct TableSetIterData));
	tsi->ts_owner = ts;
	tsi->root_idx = 0;
	tsi->root_htp = 0;
	tsi->iterating = false;
	tsi->kvm_child_htps = vh_htp_kvmap_create();

	tsr_sz = vh_SListIterator(ts->tsrs, tsr_head);

	for (i = 0; i < tsr_sz; i++)
	{
		tsr = tsr_head[i];
		vh_kvmap_value(tsi->kvm_child_htps, &tsr, htps);
		*htps = 0;	
	}

	vh_kset_key(ts->ks_tsi, tsi);

	mctx_ts = vh_mctx_switch(mctx_old);
	assert(mctx_ts == ts->mctx);

	return tsi;
}

TableSetIter
vh_tsi_first(TableSet ts)
{
	TableSetIter tsi = tsi_create(ts);

	tsi->direction = 1;

	return tsi;
}

TableSetIter
vh_tsi_last(TableSet ts)
{
	TableSetIter tsi = tsi_create(ts);
	TableSetRel tsr_root = ts->tsr_root;
	uint32_t htp_sz = vh_SListSize(tsr_root->htps);

	tsi->direction = -1;
	tsi->root_idx = htp_sz > 0 ? htp_sz - 1 : 0;

	return tsi;
}

/*
 * vh_tsi_next
 *
 * We get to step down the TableSetRel tree recursively and set the SList
 * in the iterator's kvm_child_htps.
 */
bool
vh_tsi_next(TableSetIter tsi)
{
	HeapTuplePtr *htp_head, htp;
	TableSetRel *tsr_head, tsr_root = tsi->ts_owner->tsr_root;
	uint32_t i, tsr_sz, htp_sz;

	if (tsi->iterating)
	{
		if (tsi->direction < 0 && tsi->root_idx == 0)
			return false;

		if (tsi->direction > 0 && tsi->root_idx == tsi->root_count - 1)
			return false;

		tsi->root_idx += tsi->direction;
	}
	else
	{
		tsi->iterating = true;
	}

	htp_sz = vh_SListIterator(tsr_root->htps, htp_head);

	if (tsi->root_idx >= htp_sz)
		return false;

	assert(tsi->root_idx < htp_sz);
	htp = htp_head[tsi->root_idx];

	tsr_sz = vh_SListIterator(tsr_root->children, tsr_head);

	for (i = 0; i < tsr_sz; i++)
		tsi_recurse(tsi, tsr_head[i], htp);

	return true;
}

bool
vh_tsi_previous(TableSetIter tsi)
{
	HeapTuplePtr *htp_head, htp;
	TableSetRel *tsr_head, tsr_root = tsi->ts_owner->tsr_root;
	uint32_t i, tsr_sz, htp_sz;

	if (tsi->iterating)
	{
		if (tsi->direction * -1 < 0 && tsi->root_idx == 0)
			return false;

		if (tsi->direction * -1 > 0 && tsi->root_idx == tsi->root_count - 1)
			return false;

		tsi->root_idx += (tsi->direction * -1);
	}
	else
	{
		tsi->iterating = true;
	}

	htp_sz = vh_SListIterator(tsr_root->htps, htp_head);
	assert(tsi->root_idx < htp_sz);
	htp = htp_head[tsi->root_idx];

	tsr_sz = vh_SListIterator(tsr_root->children, tsr_head);

	for (i = 0; i < tsr_sz; i++)
		tsi_recurse(tsi, tsr_head[i], htp);

	return true;
}

HeapTuplePtr
vh_tsi_root_htp(TableSetIter tsi)
{
	TableSet ts = tsi->ts_owner;
	TableSetRel tsr_root = ts->tsr_root;
	HeapTuplePtr *htp_head;
	uint32_t htp_sz;

	htp_sz = vh_SListIterator(tsr_root->htps, htp_head);
	assert(tsi->root_idx < htp_sz);

	return htp_head[tsi->root_idx];
}

SList
vh_tsi_child_htps(TableSetIter tsi, TableSetRel tsr)
{
	SList *htps;

	htps = vh_kvmap_find(tsi->kvm_child_htps, &tsr);

	return htps ? *htps : 0;
}

void
vh_tsi_destroy(TableSetIter tsi)
{
}

size_t
vh_tsi_root_count(TableSetIter tsi)
{
	if (!tsi->root_count)
	{
		return vh_SListSize(tsi->ts_owner->tsr_root->htps);
	}

	return tsi->root_count;
}

size_t
vh_tsi_child_count(TableSetIter tsi, TableSetRel tsr)
{
	return 0;
}

/*
 * tsi_recurse
 *
 * Recursive function call that sets the TableSetIter's kvm_child_htps KeyValueMap
 * values.  If the current relation doesn't have any children for the parent_htp
 * then we don't try to walk any further down the tree.
 */
static void
tsi_recurse(TableSetIter tsi, TableSetRel tsr, HeapTuplePtr parent_htp)
{
	SList *tsi_htps, tsr_htps;
	HeapTuplePtr *htp_head;
	TableSetRel *tsr_head;
	uint32_t i, j, htp_sz, tsr_sz;

	vh_kvmap_value(tsi->kvm_child_htps, &tsr, tsi_htps);
	tsr_htps = vh_kvlist_find(tsr->kvl_htps, &parent_htp);
	*tsi_htps = tsr_htps;

	if (tsr_htps && tsr->children)
	{
		htp_sz = vh_SListIterator(tsr_htps, htp_head);
		tsr_sz = vh_SListIterator(tsr->children, tsr_head);

		for (i = 0; i < htp_sz; i++)
			for (j = 0; j < tsr_sz; j++)
				tsi_recurse(tsi, tsr_head[j], htp_head[i]);	
	}
}


