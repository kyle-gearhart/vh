/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/TypeVar.h"
#include "io/utils/btree.h"
#include "io/utils/SList.h"


/*
 * ============================================================================
 * B Tree Structures
 * ============================================================================
 *
 * We're generally going to want to use a BTree index structure.  This is two
 * fold: it prevents us from having to copy the key and we get an ordered
 * list we can iterate at the leaf level.  It also gives us the ability to
 * cherry pick which key fields we want to work with.
 *
 * There are two primary ways of storing the keys: the first we call
 * BT_KC_VALUE.  It allows for the key value itself to be copied into the nodes.
 * This is helpful when the key value is 4 bytes or less.  Potentially time
 * series data could store an 8 byte key to speed up comparison operations.
 *
 * The more complex strategy is to use the BT_KC_HTPSLOT method, which puts 
 * refers to an index of a HeapTuplePtr array stored in root->htps.  The key 
 * becomes the index to the root->htps array.  The comparison function must 
 * obtain the HeapTuplePtr, get a HeapTuple read only lock to compare the key 
 * being passed down thru the function with that of the value of the index.  
 * The advantage of this method is that we don't have to store two copies of 
 * the key value: once in the HeapTuple and again in the index.  This 
 * dramatically simplifies the cleanup of a B Tree when items are removed 
 * from the index.
 *
 * If there are free down pointers on the page, we'll set the high bit on the
 * right most slot.  Otherwise, slots will be allocated from left to right
 * based on the number of keys.
 *
 * We can have multiple levels in a B Tree, with each key field representing its
 * own level.  The root->levels array contains the details about each level.
 * The node itself contains a leaf_level indicator to indicate if the node is a
 * leaf and what level it corresponds to.  The high bit is set if the node is a
 * leaf.
 *
 * btScanKey 
 */

#define BT_PAGESIZE			8192

typedef struct btKeyColumnData *btKeyColumn;
typedef struct btNodeData *btNode;
typedef struct btNodeItemData *btNodeItem;
typedef struct btRootData *btRoot;
typedef struct btScanKeyData *btScanKey;

#define BT_KCS_VALUE		0x01
#define BT_KCS_BINVALUE		0x02
#define BT_KCS_HTPSLOT		0x03
#define BT_KCS_HTP 			0x04

struct btKeyColumnData
{
	Type tys[VH_TAMS_MAX_DEPTH];		
	TableDefVer tdv;
	HeapField hf;
	vh_tom_comp comp;

	int16_t offset;
	
	int8_t sz;
	int8_t alignment;
	int8_t comp_strategy;
	bool byval;
	bool varlen;
	bool nulls;
};

struct btRootData
{
	MemoryContext mctx;
	
	struct btNodeData *root;
	
	SList htps;
	btKeyColumn cols;

	int32_t leaves;

	int16_t ncols;
	int16_t depth;

	int16_t node_sz;
	int16_t value_sz;
	int16_t bitmap_sz;

	bool hasnulls;
	bool varlens;
	bool unique;
};

/*
 * struct btNodeData
 *
 * We mimic a HeapPage with btNodeData, the top half of the page includes
 * an array of page item pointers.  When we insert a new key onto a page, we
 * simply shift the item pointer array right.
 */

#define bt_node_sizeofheader 		(offsetof(struct btNodeData, items[0]))
#define bt_n_flag_leaf				(0x8000)
#define bt_n_items(n)				(((n)->d_upper - bt_node_sizeofheader) / sizeof(uint16_t))

#define BT_RIGHTMOST(n)				((n)->right == 0)
#define BT_HIGHKEY					((uint16_t)1)
#define BT_DATAKEY					((uint16_t)2)
#define BT_FIRSTDATAKEY(n)			(BT_RIGHTMOST(n) ? BT_HIGHKEY : BT_DATAKEY)

struct btNodeData
{
	struct btNodeData *left, *right;

	/*
	 * 15th bit		leaf node indicator
	 */
	uint16_t flags;


	uint16_t d_freespace;
	uint16_t d_lower;		/* Bottom of the page */
	uint16_t d_upper;		/* Top of the page */
	uint16_t items[1];
};

#define bt_ni_flag_null		(0x8000)

struct btNodeItemData
{
	btNode ptr;
	/*
	 * 15th bit has nulls
	 * 0-14th bit offset to null bitmap
	 */
	uint16_t t_info;
};

struct btStackData
{
	btNode parent;
	HeapTuple ht;
	int32_t keyidx;		/* The slot we fell thru on the parent */
};

struct btScanData
{
	btRoot root;

	/*
	 * Traversal keys are formed by vh_bt_scan_first to figure out where to start
	 * the scan.  The result should only be one key per column.
	 */
	btScanKey sk_traverse;
	int32_t sk_n_traverse;

	/*
	 * Qualification keys are those passed to vh_bt_scan_first by the user.  As
	 * once we get down to the leaf page, we evaluate each item on the page with
	 * the qualifications.  If it matches, we'll insert it's offset number into 
	 * the items array.
	 */
	btScanKey sk_quals;
	int32_t sk_n_quals;

	btNode leftNode;
	btNode currNode;
	btNode rightNode;	/* Right page, if applicable */

	bool moreLeft;
	bool moreRight;

	/*
	 * Current result
	 */
	TypeVarSlot *key_tvs;
	char *value;
	char *dp;

	int32_t firstItem;
	int32_t lastItem;
	int32_t currItem;
	int32_t maxItems;

	uint16_t items[1];
};


#define bt_node_item(n, i)				(((unsigned char*)(n)) + (n)->items[(i) - 1])
#define bt_node_itemptr(n, i)			((btNodeItem)bt_node_item(n, i))
#define bt_node_downpointer(n, i)		(bt_node_itemptr(n, i)->ptr)

#define bt_node_valuepointer(r, n, i)									\
	((void*) ((n)->flags & bt_n_flag_leaf ?								\
	 		 (bt_node_item(n, i) +										\
			  sizeof(struct btNodeItemData) +							\
			  (bt_node_itemptr(n, i)->t_info & ~bt_ni_flag_null)) : 0))
		

static btNode bt_node_create(btRoot root);
static void bt_node_reset(btRoot root, btNode node);

static int32_t bt_search(struct btRootData *root,
						 struct btScanKeyData *scankeys,
						 struct btNodeData **node,
						 uint16_t *offset,
						 bool *key_match,
						 bool do_insert,
						 bool do_delete);

static int32_t bt_compare(struct btRootData *root,
						  struct btNodeData *node,
						  void **values,
						  bool *nulls,
						  void **idx_values,
						  uint16_t idx);

static int32_t bt_searchpos(struct btRootData *root,
							struct btNodeData *node,
							void **values,
							bool *nulls,
							void **idx_values,
							bool next_key);


static uint16_t bt_insert(struct btRootData *root,
						  struct btStackData *stack,
 						  struct btNodeData **node,
						  void **comp_values,
						  void **ins_value,
						  void **idx_values,
						  bool *nulls,
						  uint16_t itemoffset);

static int32_t bt_node_insert_pos(struct btRootData *root,
								  struct btNodeData *node,
								  void **values,
								  uint16_t *lengths,
								  uint16_t *paddings,
								  bool *nulls,
			 					  uint16_t itemidx,
								  uint16_t alignsz);

static int32_t bt_delete(struct btRootData *root,
						 struct btStackData *stack,
						 struct btNodeData *node,
						 uint16_t itemoffset);

static int32_t bt_delete_node(struct btRootData *root,
							  struct btStackData *stack,
							  struct btNodeData *node);

static void bt_node_deform(struct btRootData *root,
						   struct btNodeData *node,
						   uint16_t itemidx,
						   void **values,
						   bool *nulls,
						   bool detoast);

static void bt_node_deform_tvs(struct btRootData *root,
							   struct btNodeData *node,
							   uint16_t itemidx,
							   TypeVarSlot *slots);

static uint16_t bt_node_calc_req_space(struct btRootData *root,
									   void **values,
									   uint16_t *lengths,
									   uint16_t *paddings,
									   bool *nulls,
									   bool include_value);

static btNode bt_node_split(struct btRootData *root,
							struct btStackData *stack,
				  			struct btNodeData *node);

static void bt_node_insidx(struct btRootData *root,
						   struct btNodeData *src,
		   				   struct btNodeData *tgt,
	   					   uint16_t src_idx,
   						   uint16_t src_tgt);

static void bt_node_copyoff(struct btRootData *root,
							struct btNodeData *src,
							struct btNodeData *tgt,
							uint16_t src_idx,
							uint16_t src_tgt);

static struct btScanKeyData* bt_scankey_form_htp(struct btRootData *root,
												 HeapTuplePtr htp);
static struct btScanKeyData* bt_scankey_form_tvs(struct btRootData *root,
												 TypeVarSlot **datas, int32_t n_datas);

static void bt_col_make_var(struct btRootData *root, void **cols);
static void bt_col_destroy_var(struct btRootData *root, void **cols);

/*
 * Infinite Boundary Traversal
 */
static int32_t bt_traverse_leaf(struct btRootData *root,
	   							struct btNodeData **node,
				   				bool rightmost);
static struct btStackData* bt_traverse_leaf_stack(struct btRootData *root,
												  struct btNodeData **node,
												  struct btStackData **at,
												  bool rightmost);


/*
 * Scan Functions
 */
static bool bt_read_node(struct btScanData *scan, struct btNodeData *node,
						 uint16_t offset, bool forward);


/*
 * Utility Functions
 */
static void bt_print_tree_impl(struct btRootData *root,
							   struct btNodeData *node, int32_t depth);

static void
bt_print_node_keys(struct btRootData *root,
				   struct btNodeData *node);

#define BT_MAX_COLUMNS 10


static struct BinaryAMOptionData bt_binaryamopts = {
	.sourceBigEndian = false,   
	.targetBigEndian = false,
	.malloc = false
};

/*
 * ============================================================================
 * B Tree Function Implementation
 * ============================================================================
 */

btRoot
vh_bt_create(MemoryContext mctx, bool unique)
{
	btRoot root;
	MemoryContext bt_mctx;

	if (mctx)
		bt_mctx = mctx;
	else
		bt_mctx = vh_mctx_current();

	
	root = vhmalloc_ctx(bt_mctx, sizeof(struct btRootData));
	memset(root, 0, sizeof(struct btRootData));

	root->mctx = bt_mctx;
	root->node_sz = BT_PAGESIZE;
	root->unique = false;
	root->value_sz = 2000;

	return root;
}

void
vh_bt_destroy(btRoot root)
{
}

bool
vh_bt_add_column(struct btRootData *root,
	   			 TableDefVer tdv,
		   		 TableField tf,
		   		 bool allow_nulls)
{
	MemoryContext mctx_old;
	btKeyColumn col;
	size_t sz;

	mctx_old = vh_mctx_switch(root->mctx);

	if (!root)
	{
		elog(ERROR2,
				emsg("Invalid BTree root pointer [%p] passed to "
					 "vh_bt_ad_column.  Unable to add the column as "
					 "requested.",
					 root));

		return false;
	}

	if (!tdv)
	{
		elog(ERROR2,
				emsg("Invalid TableDefVer pointer [%p] passed to "
					 "vh_bt_add_column.  Unable to add the column as "
					 "requested.",
					 tdv));

		return false;
	}

	if (!tf)
	{
		elog(ERROR2,
				emsg("Invalid TableField pointer [%p] passed to "
					 "vt_bt_add_column.  Unable to add the column as "
					 "requested.",
					 tf));
		
		return false;
	}

	if (root->leaves)
	{
		elog(ERROR2,
				emsg("Unable to add additional columns to BTree [%p].  "
					 "Items have been inserted into the tree.",
					 root));

		return false;
	}

	if (root->ncols == BT_MAX_COLUMNS)
	{
		elog(ERROR2,
				emsg("The BTree[%p] has the maximum number of group by "
					 "columns (%d).  Unable to add another column.",
					 root, BT_MAX_COLUMNS));

		return false;
	}


	if (root->ncols)
	{
		sz = (root->ncols + 1) * sizeof(struct btKeyColumnData);
		root->cols = vhrealloc(root->cols, sz);
		col = &root->cols[root->ncols];	

		root->ncols++;
	}
	else
	{
		sz = sizeof(struct btKeyColumnData);
		root->cols = vhmalloc(sz);
		col = &root->cols[0];

		root->ncols = 1;
	}

	if (allow_nulls)
		root->hasnulls = true;

	root->bitmap_sz = (root->ncols / 8) + 1;

	col->nulls = allow_nulls;
	col->alignment = tf->heap.maxalign;


	if (tf->heap.hasvarlen ||
		tf->heap.type_depth > 1)
	{
		col->comp_strategy = BT_KCS_HTP;
		col->tdv = tdv;
		col->hf = &tf->heap;

		col->tys[0] = tf->heap.types[0];
		col->comp = tf->heap.types[0]->tom.comp;

		col->alignment = sizeof(int64_t);

		col->varlen = tf->heap.hasvarlen;
		col->sz = sizeof(int64_t);
		col->byval = true;
		col->varlen = false;

		root->varlens = false;
	}
	else
	{
		col->comp_strategy = BT_KCS_VALUE;
		col->tys[0] = tf->heap.types[0];
		col->comp = tf->heap.types[0]->tom.comp;
		col->hf = &tf->heap;
		col->sz = (int8_t)tf->heap.types[0]->size;

		col->varlen = false;

		if (col->sz <= sizeof(uintptr_t))
			col->byval = true;
	}

	assert(col->alignment > 0);

	vh_mctx_switch(mctx_old);

	return true;
}

bool
vh_bt_add_column_tys(btRoot root, Type *tys, bool allow_nulls)
{
	MemoryContext mctx_old;
	btKeyColumn col;
	size_t sz, width, max_align;
	int8_t depth;
	bool constructor;

	mctx_old = vh_mctx_switch(root->mctx);

	if (!root)
	{
		elog(ERROR2,
				emsg("Invalid BTree root pointer [%p] passed to "
					 "vh_bt_ad_column.  Unable to add the column as "
					 "requested.",
					 root));

		return false;
	}

	if (root->leaves)
	{
		elog(ERROR2,
				emsg("Unable to add additional columns to BTree [%p].  "
					 "Items have been inserted into the tree.",
					 root));

		return false;
	}

	if (root->ncols == BT_MAX_COLUMNS)
	{
		elog(ERROR2,
				emsg("The BTree[%p] has the maximum number of group by "
					 "columns (%d).  Unable to add another column.",
					 root, BT_MAX_COLUMNS));

		return false;
	}


	if (root->ncols)
	{
		sz = (root->ncols + 1) * sizeof(struct btKeyColumnData);
		root->cols = vhrealloc(root->cols, sz);
		col = &root->cols[root->ncols];	

		root->ncols++;
	}
	else
	{
		sz = sizeof(struct btKeyColumnData);
		root->cols = vhmalloc(sz);
		col = &root->cols[0];

		root->ncols = 1;
	}

	if (allow_nulls)
		root->hasnulls = true;

	root->bitmap_sz = (root->ncols / 8) + 1;


	vh_type_stack_properties(tys, -1, 
							 &depth, &width, &max_align,
							 &constructor);

	col->nulls = allow_nulls;
	col->alignment = max_align;


	if (depth > 1)
	{
		/*
		 * We really need a better way to handle this.
		 */
	}
	else
	{
		col->comp_strategy = BT_KCS_VALUE;

		vh_type_stack_copy(col->tys,tys);
		col->comp = tys[0]->tom.comp;
		col->hf = 0;
		col->sz = width;

		col->varlen = tys[0]->varlen;

		if (col->sz <= sizeof(uintptr_t))
			col->byval = true;
	}

	assert(col->alignment > 0);

	vh_mctx_switch(mctx_old);

	return true;
}

/*
 * vh_bt_find_htp
 *
 */
bool
vh_bt_find_htp(btRoot root, HeapTuplePtr htp, void **value)
{
	btScanKey sks;
	btNode node;
	int32_t res;
	bool match;
	uint16_t pos;

	if (!root->root)
	{
		root->root = bt_node_create(root);
		root->root->flags = bt_n_flag_leaf;
		root->leaves++;
		root->depth++;

		*value = 0;

		return false;
	}

	sks = bt_scankey_form_htp(root, htp);

	if (sks)
	{
		node = 0;
		res = bt_search(root, sks, &node, &pos, &match, false, false);

		if (res < 0)
		{
			/*
			 * There was an error we should handle
			 */

			match = false;
		}
		else if (node)
		{
			if (match)
			{
				*value = bt_node_valuepointer(root, node, pos);
			}
			else
			{
				*value = 0;
			}
		}
		else
		{
			match = false;
		}
		
		vhfree(sks);

		/*
		 * We need to unpin the HeapTuplePtr when we're done.
		 */
	}

	return match;
}

bool
vh_bt_find_tvs(btRoot root, TypeVarSlot **datas, int32_t n_datas,
			   void **value)
{
	btScanKey sks;
	btNode node = 0;
	int32_t res;
	uint16_t pos;
	bool match;

	*value = 0;

	if (!root->root)
	{
		root->root = bt_node_create(root);
		root->root->flags = bt_n_flag_leaf;
		root->leaves++;
		root->depth++;

		*value = 0;

		return false;
	}

	sks = bt_scankey_form_tvs(root, datas, n_datas);

	if (sks)
	{
		node = 0;
		res = bt_search(root, sks, &node, &pos, &match, false, false);

		if (res < 0)
		{
			match = false;
		}
		else if (node)
		{
			if (match)
			{
				*value = bt_node_valuepointer(root, node, pos);
			}
			else
			{
				*value = 0;
			}
		}
		else
		{
			/*
			 * No error here, but we didn't find anything.
			 */
			match = false;
		}

		vhfree(sks);
	}

	return match;
}

/*
 * vh_bt_insert_tvs
 *
 * Inserts with TypeVarSlot
 */
bool
vh_bt_insert_tvs(btRoot root, TypeVarSlot **datas, int32_t n_datas,
				 void **value)
{
	btScanKey sks;
	btNode node = 0;
	int32_t res;
	uint16_t pos;
	bool inserted = false, match;

	if (root->ncols != n_datas)
		return false;

	sks = bt_scankey_form_tvs(root, datas, n_datas);

	if (sks)
	{
		res = bt_search(root, sks, &node, &pos, &match, true, false);

		if (res < 0)
		{
			inserted = false;
		}
		else if (node)
		{
			*value = bt_node_valuepointer(root, node, pos);
			inserted = true;
		}
		else
		{
			inserted = false;
		}

		vhfree(sks);
	}

	return inserted;
}

bool
vh_bt_delete_tvs(btRoot root, TypeVarSlot **datas, int32_t n_datas)
{
	btScanKey sks;
	btNode node = 0;
	int32_t res = 1;
	uint16_t pos;
	bool match;

	if (root->ncols != n_datas)
		return false;

	sks = bt_scankey_form_tvs(root, datas, n_datas);

	if (sks)
	{
		res = bt_search(root, sks, &node, &pos, &match, false, true);
		res = match;

		vhfree(sks);
	}

	return res == 0;
}

bool
vh_bt_delete_htp(btRoot root, HeapTuplePtr htp)
{
	btScanKey sks;
	btNode node = 0;
	int32_t res = 1;
	uint16_t pos;
	bool match;

	sks = bt_scankey_form_htp(root, htp);

	if (sks)
	{
		res = bt_search(root, sks, &node, &pos, &match, false, true);
		res = match;

		vhfree(sks);
	}

	return res == 0;
}

/*
 * vh_bt_insert_htp
 *
 * Compares the HeapTuplePtr's TableDefVer with the TableDefVer we were
 * expecting, otherwise we may have to apply the common name principle
 * to match the fields up.
 *
 * We'll return true if there wasn't an error and the insert was succesful.
 */
bool
vh_bt_insert_htp(btRoot root, HeapTuplePtr htp,
				 void **value)
{
	btScanKey sks;
	btNode node;
	int32_t res;
	bool error, match;
	uint16_t pos;

	if (!root->root)
	{
		root->root = bt_node_create(root);
		root->root->flags = bt_n_flag_leaf;
		root->leaves++;
		root->depth++;
	}

	sks = bt_scankey_form_htp(root, htp);

	if (sks)
	{
		node = 0;
		res = bt_search(root, sks, &node, &pos, &match, true, false);

		if (res < 0)
		{
			/*
			 * There was an error we should handle
			 */

			error = true;
		}
		else if (node)
		{
			*value = bt_node_valuepointer(root, node, pos);

			error = false;
		}
		else
		{
			error = false;
		}
		
		/*
		 * Unpin the HeapTuplePtr so it can be evicted if needed, we have no
		 * more use for at the moment.
		 */

		vhfree(sks);
	}



	return !error;
}


/*
 * vh_bt_trim
 *
 * Trims @x leaf entries from the leaves.
 */
bool
vh_bt_trim(struct btRootData *root, int32_t entries, bool forward)
{
	struct btStackData *stack_head, *stack;
	btNode node, copy;
	int32_t max_keys, trimmed = 0, i, j;
	
	while (trimmed < entries)
	{
		stack_head = bt_traverse_leaf_stack(root, &node, &stack, !forward);

		max_keys = bt_n_items(node);

		if (!BT_RIGHTMOST(node))
			max_keys--;

		if (max_keys == 0)
		{
			/*
			 * We ran out of leaf entries to kill before we reached the requested
			 * number to trim.
			 */

			vhfree(stack_head);
			return false;
		}
		
		if (max_keys + trimmed <= entries)
		{
			/*
			 * Just kill the whole page.
			 */

			bt_delete_node(root, stack, node);
			trimmed += max_keys;
		}
		else
		{
			/*
			 * We get to selectively remove items off the page.
			 */

			copy = bt_node_create(root);

			if (!BT_RIGHTMOST(node))
			{
				/*
				 * We're not the right-most page so we'll need to copy over the high
				 * key, but it doesn't count against our trimmed count.
				 */

				bt_node_copyoff(root, node, copy, BT_HIGHKEY, BT_HIGHKEY);
			}

			for (i = BT_FIRSTDATAKEY(node), j = BT_FIRSTDATAKEY(node); i <= bt_n_items(node); i++)
			{
				if (forward && trimmed < entries)
				{
					trimmed++;
					continue;
				}
				
				bt_node_copyoff(root, node, copy, i, j);
				j++;

				if (!forward && 
					((entries - trimmed) <= (j - BT_FIRSTDATAKEY(node) + 1)))
				{
					trimmed = entries;
					break;
				}
			}

			copy->flags = node->flags;
			copy->left = node->left;
			copy->right = node->right;

			memcpy(node, copy, root->node_sz);

			vhfree(copy);
		}

		vhfree(stack_head);
	}

	return true;
}



/*
 * vh_bt_scan_begin
 *

 *
 * The calling order for a scan should be
 * 	1)	vh_bt_scan_begin
 * 	2)	vh_bt_scan_first
 * 	3)	vh_bt_scan_next (until it's done)
 * 	4)	vh_bt_scan_end OR vh_bt_scan_abort
 *
 * The trick here is that our scan qualifications are for more complex than the
 * simple find and insert scan keys.  We may in fact use a TypeVarOpExec on the
 * ScanKey to the qualifications as we traverse items on the page.
 *
 */
btScan
vh_bt_scan_begin(struct btRootData *root,
			 	 int32_t nskeys)
{
	btScan scan;
	size_t sz, scan_sz, traverse_sz, quals_sz, tvs_sz;
	int32_t i;
	int32_t max_values = (BT_PAGESIZE / root->value_sz);

	scan_sz = sizeof(struct btScanData) + (sizeof(uint16_t) * max_values);
	traverse_sz = sizeof(struct btScanKeyData) * root->ncols;
	quals_sz = sizeof(struct btScanKeyData) * nskeys;
	tvs_sz = sizeof(TypeVarSlot) * root->ncols;

	sz = scan_sz + traverse_sz + quals_sz + tvs_sz;
	scan = vhmalloc(sz);
	memset(scan, 0, sz);

	scan->root = root;
	scan->maxItems = max_values;

	/*
	 * Create the traversal scan keys and the qualification scan keys.
	 */
	scan->sk_traverse = (struct btScanKeyData*)(((char*)scan) + scan_sz);
	scan->sk_n_traverse = root->ncols;

	scan->sk_quals = scan->sk_traverse + root->ncols;
	scan->sk_n_quals = nskeys;

	/*
	 * Create the TypeVarSlot to store the keys in as we traverse the track.
	 */
	scan->key_tvs = (TypeVarSlot*)(scan->sk_quals + nskeys);

	for (i = 0; i < root->ncols; i++)
	{
		vh_tvs_init(&scan->key_tvs[i]);

		if (root->cols[i].varlen)
		{
			vh_tvs_store_var(&scan->key_tvs[i],
							 vh_typevar_make_tys(root->cols[i].tys),
							 VH_TVS_RA_DVAR);
		}
	}

	return scan;
}

void
vh_bt_scan_end(btScan scan)
{
	struct btRootData *root = scan->root;
	int32_t i;

	if (scan->sk_quals)
	{
		for (i = 0; i < scan->sk_n_quals; i++)
		{
			if (scan->sk_quals[i].tvope)
				vh_typevar_op_destroy(scan->sk_quals[i].tvope);
		}
	}

	if (scan->key_tvs)
	{
		for (i = 0; i < root->ncols; i++)
		{
			vh_tvs_finalize(&scan->key_tvs[i]);
		}
	}

	vhfree(scan);
}

/*
 * vh_bt_scan_first
 *
 * Starts a scan.  @skeys represents the qualifications for the scan.  Each key
 * column may have more than one qualification.
 *
 * scan_begin starts by forming a new set of ScanKeys used to traverse to the first
 * matching spot in the index.  Finding the best key to use is a bit tricky.
 * If we're scanning forward (i.e. left to right) we use the lowest key on the
 * comparator.
 *
 * We set the appropriate data on btScan to re-enter.
 */
bool
vh_bt_scan_first(struct btScanData *scan,
				 struct btScanKeyData *skeys,
				 int32_t nskeys,
				 bool forward)
{
	static const char* oper_lookup[] = { "<", "<=", "==", "!=", ">=", ">" };
	static const int32_t tvope_flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
													   VH_OP_DT_TVS, VH_OP_ID_INVALID,
													   VH_OP_DT_TVS, VH_OP_ID_INVALID);

	btRoot root = scan->root;
	btNode node = 0;
	int32_t i, col_no, res;
	TypeVarSlot slot;
	Type tys[VH_TAMS_MAX_DEPTH];
	//void *values[VH_TAMS_MAX_DEPTH];
	//bool nulls[VH_TAMS_MAX_DEPTH];
	uint16_t offset;
	int8_t oper;
	bool comp, match;

	scan->sk_n_quals = nskeys;


	if (!nskeys)
	{
		/*
		 * No scankeys, so we're going to scan the entire index.
		 */
		if (bt_traverse_leaf(scan->root, &node, !forward))
		{
			return bt_read_node(scan, node, (forward ? BT_FIRSTDATAKEY(node) : bt_n_items(node)), forward);	
		}

		return false;
	}

	/*
	 * Get the TypeVarOpExec for each ScanKey.  Then figure out which scan key
	 * is most appropriate for the initial traversal scan.
	 */
	for (i = 0; i < nskeys; i++)
	{
		col_no = skeys[i].col_no;
		oper = skeys[i].oper;

		tys[0] = root->cols[col_no].tys[0];
		tys[1] = 0;

		vh_tvs_init(&slot);
		vh_tvs_store(&slot, tys, 0);

		scan->sk_quals[i].tvope = vh_typevar_comp_init(oper_lookup[oper], tvope_flags,
													&slot, &skeys[i].tvs);
		scan->sk_quals[i].tvs = skeys[i].tvs;
		scan->sk_quals[i].col_no = col_no;
		scan->sk_quals[i].oper = oper;

		if (!scan->sk_traverse[col_no].tvope)
		{
			scan->sk_traverse[col_no] = skeys[i];
		}
		else
		{
			/*
			 * We get to compare the values.
			 */
			comp = vh_typevar_comp(oper_lookup[oper], tvope_flags,
				   				   &skeys[i].tvs, &scan->sk_traverse[col_no].tvs);

			if (comp)
			{
				vh_tvs_move(&scan->sk_traverse[col_no].tvs,
							&skeys[i].tvs);
				scan->sk_traverse[col_no].oper = skeys[i].oper;
				scan->sk_traverse[col_no].col_no = col_no;
			}
		}
	}

	/*
	 * Need to run down our traversal keys, we may have an infinite starting
	 * boundary that requires us to traverse down the left or right side.
	 */

	switch (scan->sk_traverse[col_no].oper)
	{
		case VH_BT_OPER_LT:
		case VH_BT_OPER_LTEQ:

			if (forward)
			{
				bt_traverse_leaf(root, &node, false);

				if (node && bt_read_node(scan, node, BT_FIRSTDATAKEY(node), forward))
				{
					bt_node_deform_tvs(root, node, scan->currItem, scan->key_tvs);
					scan->value = bt_node_valuepointer(root, node, scan->currItem);

					return true;	
				}

				return false;
			}

			break;

		case VH_BT_OPER_GT:
		case VH_BT_OPER_GTEQ:

			if (!forward)
			{
				bt_traverse_leaf(root, &node, true);

				if (node && bt_read_node(scan, node, bt_n_items(node), forward))
				{
					bt_node_deform_tvs(root, node, scan->currItem, scan->key_tvs);
					scan->value = bt_node_valuepointer(root, node, scan->currItem);

					return true;
				}

				return false;
			}

			break;
	}


	/*
	 * Let's find our page in the tree that we need to read.
	 */
	res = bt_search(root, scan->sk_traverse, &node, &offset, &match, false, false);

	if (res)
	{
	}

	if (node && bt_read_node(scan, node, offset, forward))
	{
		bt_node_deform_tvs(root, node, scan->currItem, scan->key_tvs);
		scan->value = bt_node_valuepointer(root, node, scan->currItem);

		return true;
	}

	return false;
}

/*
 * vh_bt_scan_get
 */
void
vh_bt_scan_get(struct btScanData *scan, TypeVarSlot **keys, void **value)
{
	*keys = scan->key_tvs;
	*value = scan->value;
}

static bool
bt_read_node(struct btScanData *scan, struct btNodeData *node,
			 uint16_t offset, bool forward)
{
	bool comp, meets_qual, more_data = false;
	TypeVarOpExec tvope;

	int32_t i, j, max, min, col_no;

	/*
	 * Reset the scan index
	 */

	if (forward)
	{
		scan->firstItem = 0;
		scan->lastItem = 0;
		scan->currItem = 0;
	}
	else
	{
		scan->firstItem = scan->maxItems - 1;
		scan->lastItem = scan->maxItems - 1;
		scan->currItem = scan->maxItems - 1;
	}

	max = bt_n_items(node);
	min = BT_FIRSTDATAKEY(node);

	for (i = offset;;)
	{
		if (forward && i > max)
			break;
		if (!forward && i < min)
			break;

		bt_node_deform_tvs(scan->root, node, i, scan->key_tvs);

		meets_qual = true;

		for (j = 0; j < scan->sk_n_quals; j++)
		{
			tvope = scan->sk_quals[j].tvope;
			col_no = scan->sk_quals[j].col_no;

			comp = vh_typevar_comp_fp(tvope,
									  &scan->key_tvs[col_no],
									  &scan->sk_quals[j].tvs);

			if (comp)
			{
				/*
				 * This ScanKey qual is good
				 */
			}
			else
			{
				meets_qual = false;
			}
		}

		if (meets_qual)
		{
			if (forward)
			{
				scan->items[scan->lastItem++] = i;
			}
			else
			{
				scan->items[scan->firstItem--] = i;
			}

			more_data = true;
		}

		i += (forward ? 1 : -1);
	}


	if (more_data)
	{
		if (forward)
		{
			scan->value = bt_node_valuepointer(scan->root, node, scan->items[scan->firstItem]);
			scan->dp = (void*)bt_node_downpointer(node, scan->items[scan->firstItem]);

			/*
			 * Read in the keys into a TypeVarSlot array on Scan.
			 */
			bt_node_deform_tvs(scan->root,
							   node,
							   scan->items[scan->firstItem],
							   scan->key_tvs);
		}
		else
		{
			scan->value = bt_node_valuepointer(scan->root, node, scan->items[scan->lastItem]);
			scan->dp = (void*)bt_node_downpointer(node, scan->items[scan->lastItem]);
			
			/*
			 * Read in the keys into a TypeVarSlot array on Scan.
			 */
			bt_node_deform_tvs(scan->root,
							   node,
							   scan->items[scan->lastItem],
							   scan->key_tvs);
		}

		scan->currNode = node;
		scan->leftNode = node->left;
		scan->rightNode = node->right;
	}

	return more_data;
}

bool
vh_bt_scan_next(struct btScanData *scan,
				bool forward)
{
	btNode node;

	if (forward)
	{
		if (++scan->currItem >= scan->lastItem)
		{
			/*
			 * read the right hand link in, populating all the items that meet
			 * the sk_quals.  The reader should return false if no keys met the
			 * qualifications.
			 */
			node = scan->rightNode;

			if (!node)
				return false;

			if (!bt_read_node(scan, node, BT_FIRSTDATAKEY(node), forward))
				return false;
		}
	}
	else
	{
		if (--scan->currItem <= scan->firstItem)
		{
			/* 
			 * read the left hand link in, populating all the items that meet
			 * the sk_quals.  The reader should return false if no keys met the
			 * qualifications.
			 */

			node = scan->leftNode;

			if (!node)
				return false;

			if (!bt_read_node(scan, node, bt_n_items(node), forward))
				return false;
		}		
	}

	/*
	 * Set the value pointer for the item
	 */
	scan->value = bt_node_valuepointer(scan->root, scan->currNode, scan->items[scan->currItem]);
	scan->dp = (void*)bt_node_downpointer(scan->currNode, scan->items[scan->currItem]);

	bt_node_deform_tvs(scan->root,
					   scan->currNode,
					   scan->items[scan->currItem],
					   scan->key_tvs);
	
	return true;
}

static struct btScanKeyData*
bt_scankey_form_htp(struct btRootData *root, HeapTuplePtr htp)
{
	btScanKey sks;
	btKeyColumn cols;
	bool sks_set = true;
	uint16_t i;

	cols = root->cols;
	sks = vhmalloc(sizeof(struct btScanKeyData) * root->ncols);

	for (i = 0; i < root->ncols; i++)
	{
		//if (cols[i].tdv != ht->htd)
		{
		}

		vh_tvs_init(&sks[i].tvs);

		switch (cols[i].comp_strategy)
		{
			case BT_KCS_VALUE:

				vh_tvs_store_htp_hf(&sks[i].tvs, htp, cols[i].hf);

				break;

			case BT_KCS_HTP:
			case BT_KCS_HTPSLOT:

				/*
				 * We don't actually want to insert the HeapTuplePtr into the
				 * SList, so we store the HeapTuplePtr instead.  The insert
				 * routines are intelligent enough to recognize this and store
				 * the HeapTuplePtr when necessary.
				 */
				
				vh_tvs_store_i64(&sks[i].tvs, htp);

				break;
		}
		
		sks[i].oper = VH_BT_OPER_EQ;
		sks[i].tvope = 0;
	}

	if (!sks_set)
	{
		vhfree(sks);

		return 0;
	}

	return sks;
}

static struct btScanKeyData*
bt_scankey_form_tvs(struct btRootData *root,
					TypeVarSlot **datas, int32_t n_datas)
{
	btScanKey sks;
	btKeyColumn cols;
	int32_t i;

	cols = root->cols;
	sks = vhmalloc(sizeof(struct btScanKeyData) * root->ncols);

	for (i = 0; i < root->ncols; i++)
	{
		vh_tvs_init(&sks[i].tvs);

		switch(cols[i].comp_strategy)
		{
			case BT_KCS_VALUE:

				vh_tvs_copy(&sks[i].tvs, datas[i]);

				break;

			default:
				elog(WARNING,
						emsg("Unsupported comp_startegy, unable to form a ScanKey "
							 "from a TypeVarSlot array."));

				vhfree(sks);

				return 0;
		}
		
		sks[i].oper = VH_BT_OPER_EQ;
		sks[i].tvope = 0;
	}

	return sks;
}

static int32_t
bt_search(struct btRootData *root,
		  struct btScanKeyData *scankeys,
		  struct btNodeData **node,
		  uint16_t *offset,
		  bool *key_match,
		  bool do_insert,
		  bool do_delete)
{
	struct btStackData *stack, *istack;
	struct btNodeData *n;
	struct btKeyColumnData *cols;
	HeapTuple ht;
	HeapTuplePtr htp;
	uint16_t ioffset;
	
	bool nulls[BT_MAX_COLUMNS];
	void *comp_values[BT_MAX_COLUMNS];
	void *ins_values[BT_MAX_COLUMNS];
	void *idx_values[BT_MAX_COLUMNS];
	uint16_t i;

	/*
	 * Transfer values out of the ScanKey and do a transform if necessary.
	 *
	 * We'll need to unpin the HeapTuple when we're done with the scan.  To
	 * do this, we've set ht == 0, so if it's non-null then we get to unpin.
	 */

	ht = 0;
	htp = 0;
	cols = root->cols;

	bt_col_make_var(root, idx_values);

	for (i = 0; i < root->ncols; i++)
	{
		switch (cols[i].comp_strategy)
		{
			case BT_KCS_VALUE:

				nulls[i] = vh_tvs_isnull(&scankeys[i].tvs);
				comp_values[i] = ins_values[i] = vh_tvs_value(&scankeys[i].tvs);

				break;

			case BT_KCS_HTP:
			case BT_KCS_HTPSLOT:

				if (htp)
				{
					assert(htp == *((HeapTuplePtr*)vh_tvs_value(&scankeys[i].tvs)));
				}
				else
				{		
					htp = *((HeapTuplePtr*)vh_tvs_value(&scankeys[i].tvs));
					ht = vh_htp(htp);

					if (!ht)
					{
						elog(ERROR1,
								emsg("Unable to pin HeapTuplePtr [%lld] to setup the B+Tree "
									 "search.  The buffer containing the HeapTuplePtr may no "
									 "longer exist or the HeapTuplePtr has been removed from "
									 "the buffer.",
									 htp));
					}
				}

				ins_values[i] = (void*)htp;
				
				comp_values[i] = vh_ht_field(ht, cols[i].hf);
				nulls[i] = vh_htf_isnull(ht, cols[i].hf);
				
				break;
		}
	}


	stack = vhmalloc(sizeof(struct btStackData) * (root->depth + 1));
	stack->parent = 0;
	stack->keyidx = -1;
	stack->ht = 0;

	istack = stack;
	
	if (*node)
		n = *node;
	else
		n = root->root;

	while (true)
	{
		ioffset = bt_searchpos(root, n, comp_values, nulls, idx_values, false);

		if (ioffset > -1)
		{
			if (n->flags & bt_n_flag_leaf)
			{
				if (do_insert)
				{
					ioffset = bt_insert(root, istack, &n, 
										comp_values, ins_values, idx_values,
										nulls, ioffset);
				}
				
				/*
				 * We're at the leaf node, so return the position we think this
				 * thing should sit at for now.
				 */
				*offset = ioffset;
				*node = n;

				if (ioffset > bt_n_items(n))
				{
					*key_match = false;
				}
				else if (!bt_compare(root, n, comp_values, nulls, idx_values, ioffset))
				{
					/*
					 * We have an exact match, let the caller know this.
					 */

					*key_match = true;

					if (do_delete)
					{
						bt_delete(root, istack, n, ioffset);
					}
				}
				else
				{
					*key_match = false;
				}

				
				break;
			}
			else
			{
				/*
				 * We need to move right, comparing the high keys.  The search algorithm
				 * doesn't always put us on the right page.
				 */

				istack++;
				istack->parent = n;
				istack->keyidx = ioffset;

				n = bt_node_downpointer(n, ioffset);

				for (;;)
				{
					if (!n->right)
						break;

					if (bt_compare(root, n, comp_values, nulls, idx_values, BT_HIGHKEY) >= 0)
					{
						n = n->right;
						istack->keyidx++;
						continue;
					}
					else
					{
						break;
					}
				}

				assert(n);

				continue;
			}
		}
		else
		{
			*key_match = false;
		}
	}

	bt_col_destroy_var(root, idx_values);
	vhfree(stack);

	return 0; 
}

/*
 * bt_compare
 *
 * Returns
 * 		<0 if values < key at idx
 * 		0 if values == key at idx
 * 		>0 if values > key at idx
 *
 */
static int32_t
bt_compare(struct btRootData *root,
		   struct btNodeData *node,
		   void **values,
		   bool *nulls,
		   void **idx_values,
		   uint16_t idx)
{
	btKeyColumn cols;
	vh_tom_comp compf;
	int32_t comp;
	uint16_t i;

	bool idx_nulls[BT_MAX_COLUMNS];

	if (!(node->flags & bt_n_flag_leaf) && idx == BT_FIRSTDATAKEY(node))
		return 1;

	/*
	 * If we've got a varlen, then we expect a TypeVar to already exist in that
	 * slot.  The easier way to do this is to change the calling convention
	 * around so that we get handed a TypeVarSlot array with this already done.
	 */

	cols = root->cols;
	bt_node_deform(root, node, idx, idx_values, idx_nulls, true);

	for (i = 0; i < root->ncols; i++)
	{
		if (nulls[i])
		{
			if (idx_nulls[i])
				comp = 0;
			else
				comp = -1;
		}
		else if (root->hasnulls && idx_nulls[i])
		{
			comp = 1;
		}
		else
		{
			compf = cols[i].comp;
			
			/*
			if (cols[i].byval && cols[i].comp_strategy == BT_KCS_VALUE)
				comp = vh_tom_firee_comp(compf,
										 idx_values[i],
										 &values[i]);
			else
			*/
				comp = vh_tom_firee_comp(compf,
										 idx_values[i],
										 values[i]);

			comp = -comp;
		}

		if (comp != 0)
			return comp;
	}

	return 0;
}

/*
 * bt_searchpos
 *
 * Searches for the position where the key should live, will set the equality
 * flag if the position returned has a matching key.
 *
 *
 * 	-1	Error
 */
static int32_t 
bt_searchpos(struct btRootData *root, 
			 struct btNodeData *node,
			 void **comp_values,
			 bool *comp_nulls,
			 void **idx_values,
			 bool next_key)
{
	int32_t comp, cmpval;
	uint16_t high, low, mid;

	cmpval = next_key ? 0 : 1;

	low = BT_FIRSTDATAKEY(node);
	high = bt_n_items(node);

	if (high < low)
		return low;

	high++;

	while (high > low)
	{
		mid = low + ((high - low) / 2);
		comp = bt_compare(root, node, comp_values, comp_nulls, idx_values, mid);

		if (comp >= cmpval)
			low = mid + 1;
		else
			high = mid;
	}

	if (node->flags & bt_n_flag_leaf)
		return low;

	assert( low > BT_FIRSTDATAKEY(node));

	return low - 1;
}


static uint16_t 
bt_insert(struct btRootData *root,
		  struct btStackData *stack,
 		  struct btNodeData **node,
 		  void **comp_values,
		  void **ins_values,
		  void **idx_values,
		  bool *nulls,
		  uint16_t itemoffset)
{
	btNode n;
	uint16_t k_sz;
	uint16_t k_lengths[BT_MAX_COLUMNS];
	uint16_t k_paddings[BT_MAX_COLUMNS];

	if (*node)
		n = *node;
	else
		n = root->root;

	k_sz = bt_node_calc_req_space(root, ins_values, k_lengths, k_paddings, nulls, true);

	while (true)
	{
		if (n->d_freespace >= k_sz + sizeof(uint16_t))
		{
			bt_node_insert_pos(root, n, ins_values, 
							   k_lengths, k_paddings, nulls,
							   itemoffset, k_sz);

			*node = n;

			return itemoffset;
		}
		else
		{
			/*
			 * Surprise! We get to split the page, but first we must check
			 * the parent page to see if there's space there.  We'll need
			 * to calculate a new key size based on the left most key, as
			 * this the one that will be inserted into the parent.
			 */

			bt_node_split(root, stack, n);

			for (;;)
			{
				if (!n->right)
					break;

				if (bt_compare(root, n, comp_values, nulls, idx_values, BT_HIGHKEY) >= 0)
				{
					n = n->right;
					continue;
				}
				else
				{
					break;
				}
			}

			itemoffset = bt_searchpos(root, n, comp_values, nulls, idx_values, false);
		}
	}
}

static btNode
bt_node_split(struct btRootData *root,
			  struct btStackData *stack,
			  struct btNodeData *node)
{
	btNodeItem item;
	btNode sibling, parent, altparent, nodecopy;
	size_t downlink_sz;
	uint16_t mid, right, i, j, pmid, ppos;


	if (node == root->root)
	{
		assert(!stack->parent);

		/*
		 * We need to split the root page, so we get to make to new nodes, the
		 * first node will be the new root and the second node will be the right
		 * hand side.
		 */

		parent = bt_node_create(root);
		sibling = bt_node_create(root);
		nodecopy = bt_node_create(root);

		/*
		 * Copy over the leaf flags, if the node is a leaf then we want its sibling
		 * to be flagged as a leaf as well.
		 */
		sibling->flags = node->flags;
		nodecopy->flags = node->flags;

		mid = bt_n_items(node) / 2;
		right = bt_n_items(node);
		mid++;


		nodecopy->right = sibling;
		nodecopy->left = node->left;
		sibling->left = node;

		bt_node_copyoff(root, node, nodecopy, mid, BT_HIGHKEY);

		if (node->right)
		{
			bt_node_copyoff(root, node->right, sibling, BT_FIRSTDATAKEY(node->right), BT_HIGHKEY);
			node->right->left = sibling;
		}

		for (i = BT_FIRSTDATAKEY(node), j = BT_FIRSTDATAKEY(nodecopy); i < mid; i++, j++)
			bt_node_copyoff(root, node, nodecopy, i, j);

		for (i = mid, j = BT_FIRSTDATAKEY(sibling); i <= right; i++, j++)
			bt_node_copyoff(root, node, sibling, i, j);

		memcpy(node, nodecopy, root->node_sz);	

		/*
		 * Copy the keys into the parent for the down pointers.
		 */	
		bt_node_copyoff(root, nodecopy, parent, 1, BT_HIGHKEY);
		bt_node_copyoff(root, nodecopy, parent, 1, BT_DATAKEY);

		/*
		 * Set the down pointers on the parent page.
		 */
		bt_node_downpointer(parent, BT_FIRSTDATAKEY(parent)) = node;
		bt_node_downpointer(parent, BT_FIRSTDATAKEY(parent) + 1) = sibling;


		root->leaves++;
		root->depth++;
		root->root = parent;

		return sibling;
	}
	else
	{
		/*
		 * This route is a little messier because we need to calculate the key
		 * sizes that we need need to promote to the parent to maintain the
		 * downlinks to the new sibling.
		 *
		 * If there's not enough room on the parent, then we'll have to split
		 * it.  But before we do, we need to figure out where our key will
		 * land so that when the parent split exits, we'll know which half to
		 * drop it in.
		 */

		parent = stack->parent;
		ppos = stack->keyidx;

		pmid = bt_n_items(parent) / 2;

		mid = bt_n_items(node) / 2;
		right = bt_n_items(node);
		mid++;

		mid = node->right ? mid + 1 : mid;

		item = bt_node_itemptr(node, right);

		downlink_sz = sizeof(struct btNodeItemData) + 
		  			  (item->t_info & ~bt_ni_flag_null) +
					  (root->hasnulls ? root->bitmap_sz : 0) +
					  (parent->flags & bt_n_flag_leaf ? root->value_sz : 0);

		if (parent->d_freespace < downlink_sz + sizeof(uint16_t))
		{
			/*
			 * It's not going to fit, so we'll have to recursively call ourselves
			 * until we find a spot this will fit.
			 */
				
			altparent = bt_node_split(root, (stack-1), parent);
		}
		else
		{
			altparent = 0;
		}

		nodecopy = bt_node_create(root);
		sibling = bt_node_create(root);
		sibling->flags = node->flags;
		nodecopy->flags = node->flags;
		
		sibling->right = node->right;
		sibling->left = node;
		nodecopy->right = sibling;
		nodecopy->left = node->left;

		bt_node_copyoff(root, node, nodecopy, mid, BT_HIGHKEY);

		if (node->right)
		{
			bt_node_copyoff(root, node->right, sibling, BT_FIRSTDATAKEY(node->right), BT_HIGHKEY);
			node->right->left = sibling;
		}

		/*
		 * Copy everything from mid to the right over to the new sibling and then
		 * free the indexes on the original node.
		 */
		for (i = BT_FIRSTDATAKEY(node), j = BT_FIRSTDATAKEY(nodecopy); i < mid; i++, j++)
			bt_node_copyoff(root, node, nodecopy, i, j);

		for (i = mid, j = BT_FIRSTDATAKEY(sibling); i <= right; i++, j++)
			bt_node_copyoff(root, node, sibling, i, j);


		/*
		 * If the parent was split, then altparent will be populated.  If so, then
		 * we'll need to figure out where the keys to our two sibling nodes now live.
		 *
		 * It's possible the LHS downlink and RHS downlink both end up on the altparent,
		 * which is the right hand parent.
		 */

		if (altparent)
		{
			if (ppos >= pmid)
			{
				/* Put both keys on the left hand parent. */

				ppos = ppos - pmid;
				bt_node_copyoff(root, sibling, altparent, BT_FIRSTDATAKEY(sibling), ppos);
				bt_node_downpointer(altparent, ppos) = node;
				bt_node_downpointer(altparent, ppos + 1) = sibling;
			}
			else
			{
				/* Put both keys on the right hand parent. */
				bt_node_copyoff(root, sibling, parent, BT_FIRSTDATAKEY(sibling), ppos);
				bt_node_downpointer(parent, ppos) = node;
				bt_node_downpointer(parent, ppos + 1) = sibling;
			}
		}
		else
		{
			/*
			 * The parent was not split and our right most key now has the
			 * wrong downpointer, because this was on the left.
			 */
			bt_node_copyoff(root, sibling, parent, BT_FIRSTDATAKEY(sibling), ppos);
			bt_node_downpointer(parent, ppos) = node;
			bt_node_downpointer(parent, ppos + 1) = sibling;
		}

		memcpy(node, nodecopy, root->node_sz);
		vhfree(nodecopy);

		if (sibling->flags & bt_n_flag_leaf)
			root->leaves++;

		return sibling;
	}

	return 0;
}

static void 
bt_node_copyoff(struct btRootData *root,
			    struct btNodeData *src,
				struct btNodeData *tgt,
				uint16_t src_off,
				uint16_t tgt_off)
{
	size_t copy_sz;
	btNodeItem item_src = bt_node_itemptr(src, src_off);
	uint16_t c;

	copy_sz = sizeof(struct btNodeItemData) + 
			  (item_src->t_info & ~bt_ni_flag_null) +
			  (tgt->flags & bt_n_flag_leaf ? 
			   (tgt->right && tgt_off == BT_HIGHKEY ? 0 : root->value_sz) : 0);

	if (tgt->d_freespace >= (uint16_t)copy_sz + sizeof(uint16_t))
	{
		/*
		 * Shift the items pointers right if necessary.
		 */

		c = bt_n_items(tgt);

		if (c && tgt_off < c)
		{
			memmove(&tgt->items[tgt_off],
					&tgt->items[tgt_off - 1],
					sizeof(uint16_t) * (bt_n_items(tgt) - tgt_off + 1));
		}
		
		tgt->d_lower -= copy_sz;
		tgt->d_upper += sizeof(uint16_t);
		tgt->d_freespace = tgt->d_lower - tgt->d_upper;
		tgt->items[tgt_off - 1] = tgt->d_lower;

		/*
		 * Copy the data from the source to the target
		 */
		memcpy(bt_node_item(tgt, tgt_off),
			   item_src,
			   copy_sz);
	}
	else
	{
		assert( 1 == 0 );
	}
}

static btNode 
bt_node_create(btRoot root)
{
	btNode node;

	node = vhmalloc_ctx(root->mctx, root->node_sz);
	memset(node, 0, root->node_sz);

	assert(((root->node_sz % sizeof(uintptr_t)) == 0));

	node->items[0] = 0;
	node->d_upper = (uintptr_t) &((btNode)0)->items[0];
	node->d_lower = root->node_sz;
	node->d_freespace = node->d_lower - node->d_upper;

	return node;
}

static void
bt_node_reset(btRoot root, btNode node)
{
	uint16_t flags = node->flags;

	memset(node, 0, root->node_sz);

	node->flags = flags;
	node->d_upper = (uintptr_t) &((btNode)0)->items[0];
	node->d_lower = root->node_sz;
	node->d_freespace = node->d_lower - node->d_upper;
}



/*
 * bt_node_deform
 *
 * Pulls values from a specific itemidx, accounts for alignment
 */
static void
bt_node_deform(struct btRootData *root,
			   struct btNodeData *node,
			   uint16_t itemoff,
			   void **values,
			   bool *nulls,
			   bool detoast)
{
	btKeyColumn cols = root->cols;
	btNodeItem item;
	unsigned char *cursor;
	int16_t i, *len;
	int32_t *htp_slot, htp_sz;
	int8_t alignments[BT_MAX_COLUMNS], padding;
	bool has_nulls, is_null, *null_flags;
	HeapTuplePtr *htp_head;
	HeapTuple ht;



	has_nulls = root->hasnulls;
	item = (btNodeItem)bt_node_itemptr(node, itemoff);
	null_flags = ((bool*)(item + 1)) + (item->t_info & ~bt_ni_flag_null);

	cursor = (unsigned char*)(item + 1);

	for (i = 0; i < root->ncols; i++)
	{
		if (has_nulls)
		{
			is_null = null_flags[0] >> (7 - i);
			nulls[i] = is_null;
		}
		else
		{
			is_null = false;
			nulls[i] = false;
		}

		if (!is_null || !has_nulls)
		{
			switch (cols[i].comp_strategy)
			{
				case BT_KCS_VALUE:

					alignments[i] = cols[i].alignment;

					if (cols[i].varlen)
					{
						if (i > 0)
						{
							if (alignments[i - 1] < sizeof(int16_t))
							{
								padding = sizeof(int16_t) - alignments[i - 1];
								cursor += padding;
							}
						}

						len = (int16_t*) cursor;

						if (sizeof(int16_t) < alignments[i])
						{
							/*
							 * The alignment of the length word isn't enough as the trailing
							 * data is of a greater alignment.  We need to pad between the 
							 * length word and the trailing data.
							 */
							
							padding = alignments[i] - sizeof(int16_t);
							cursor += padding;
						}

						/*
						 * Values is currently in binary format, so we'll have to call the
						 * set method to store it in valuess.
						 */

						vh_tam_fire_bin_set(cols[i].tys,
											&bt_binaryamopts,
											len + 1,
											values[i],
											*len,
											0);
						
						cursor = ((unsigned char*)(len + 1)) + *len;
					}
					else
					{
						if (i > 0)
						{
							if (alignments[i - 1] < alignments[i])
							{
								padding = alignments[i] - alignments[i - 1];
								cursor += padding;
							}
						}

						values[i] = cursor;
						cursor += cols[i].sz;
					}

					break;

				case BT_KCS_HTP:

					assert(cols[i].sz == sizeof(HeapTuplePtr));

					alignments[i] = cols[i].alignment;

					if (i > 0)
					{
						if (alignments[i - 1] < alignments[i])
						{
							padding = alignments[i] - alignments[i - 1];
							cursor += padding;
						}
					}

					htp_head = (HeapTuplePtr*)cursor;

					if (detoast)
					{
						if (*htp_head)
						{
							ht = vh_htp(*htp_head);

							if (!ht)
							{
								elog(ERROR1,
										emsg("Unable to obtain HeapTuplePtr [%lld] for the index "
											 "comparison.  The buffer may have been removed or the "
											 "HeapTuplePtr was removed from the buffer.  Unable to"
											 "proceed.",
											 *htp_head));
							}

							values[i] = vh_ht_field(ht, cols[i].hf);
						}
					}
					else
					{
						values[i] = cursor;
					}

					cursor += cols[i].sz;

					break;

				case BT_KCS_HTPSLOT:

					assert(cols[i].sz == sizeof(int32_t));

					if (i > 0)
					{
						if (alignments[i - 1] < alignments[i])
						{
							padding = alignments[i] - alignments[i - 1];
							cursor += padding;
						}		
					}

					htp_slot = (int32_t*)cursor;
					htp_sz = vh_SListIterator(root->htps, htp_head);

					assert(htp_sz > *htp_slot);

					/*
					 * Pull the immutable version of the HeapTuple for now.  We may
					 * provide an option in the future to set the flags that determine
					 * which version is pulled.  However, it gets complicated because
					 * if a field value changes after it's been inserted into the index
					 * then we'll have a corrupt index.  By reference allows us to cheat
					 * on the storage requirements for each key, but it does risk a 
					 * corrupt index.
					 *
					 * Play it safe.
					 */

					if (detoast)
					{
						ht = vh_htp(htp_head[*htp_slot]);
						assert(ht);

						values[i] = vh_ht_field(ht, cols[i].hf);
					}
					else
					{
						values[i] = cursor;
					}

					cursor += cols[i].sz;

					break;
			}
		}

		if (has_nulls && i > 0 && i % 7 == 0)
			null_flags++;
	}
}

/*
 * bt_node_deform_tvs
 *
 * Pulls values from a specific itemidx, accounts for alignment but uses a
 * TypeVarSlot, so we don't have to detoast.  The TypeVarSlot handles that.
 */
static void
bt_node_deform_tvs(struct btRootData *root,
	  			   struct btNodeData *node,
				   uint16_t itemoff,
				   TypeVarSlot *tvs)
{
	btKeyColumn cols = root->cols;
	btNodeItem item;
	unsigned char *cursor;
	int16_t i, *len;
	int32_t *htp_slot, htp_sz;
	int8_t alignments[BT_MAX_COLUMNS], padding;
	Type tys[VH_TAMS_MAX_DEPTH];
	bool has_nulls, is_null, *null_flags;
	HeapTuplePtr *htp_head;
	void *value;



	has_nulls = root->hasnulls;
	item = (btNodeItem)bt_node_itemptr(node, itemoff);
	null_flags = ((bool*)(item + 1)) + (item->t_info & ~bt_ni_flag_null);

	cursor = (unsigned char*)(item + 1);

	for (i = 0; i < root->ncols; i++)
	{
		tys[0] = root->cols[i].tys[0];
		tys[1] = 0;

		if (has_nulls)
		{
			is_null = null_flags[0] >> (7 - i);
		}
		else
		{
			is_null = false;
		}

		if (!is_null || !has_nulls)
		{
			switch (cols[i].comp_strategy)
			{
				case BT_KCS_VALUE:

					alignments[i] = cols[i].alignment;

					if (cols[i].varlen)
					{
						if (i > 0)
						{
							if (alignments[i - 1] < sizeof(int16_t))
							{
								padding = sizeof(int16_t) - alignments[i - 1];
								cursor += padding;
							}
						}

						len = (int16_t*) cursor;

						if (sizeof(int16_t) < alignments[i])
						{
							/*
							 * The alignment of the length word isn't enough as the trailing
							 * data is of a greater alignment.  We need to pad between the 
							 * length word and the trailing data.
							 */
							
							padding = alignments[i] - sizeof(int16_t);
							cursor += padding;
						}
						
						value = len + 1;

						vh_tam_fire_bin_set(cols[i].tys,
											&bt_binaryamopts,
											value,
											vh_tvs_value(&tvs[i]),
											*len,
											0);

						cursor = ((unsigned char*)(len + 1)) + *len;
					}
					else
					{
						if (i > 0)
						{
							if (alignments[i - 1] < alignments[i])
							{
								padding = alignments[i] - alignments[i - 1];
								cursor += padding;
							}
						}

						value = cursor;
						cursor += cols[i].sz;
					
						vh_tvs_store(&tvs[i], tys, value);
					}

					break;

				case BT_KCS_HTP:

					assert(cols[i].sz == sizeof(HeapTuplePtr));

					alignments[i] = cols[i].alignment;

					if (i > 0)
					{
						if (alignments[i - 1] < alignments[i])
						{
							padding = alignments[i] - alignments[i - 1];
							cursor += padding;
						}
					}

					htp_head = (HeapTuplePtr*)cursor;
					vh_tvs_store_htp_hf(&tvs[i], *htp_head, root->cols[i].hf);

					cursor += cols[i].sz;

					break;

				case BT_KCS_HTPSLOT:

					assert(cols[i].sz == sizeof(int32_t));

					if (i > 0)
					{
						if (alignments[i - 1] < alignments[i])
						{
							padding = alignments[i] - alignments[i - 1];
							cursor += padding;
						}		
					}

					htp_slot = (int32_t*)cursor;
					htp_sz = vh_SListIterator(root->htps, htp_head);

					assert(htp_sz > *htp_slot);

					/*
					 * Pull the immutable version of the HeapTuple for now.  We may
					 * provide an option in the future to set the flags that determine
					 * which version is pulled.  However, it gets complicated because
					 * if a field value changes after it's been inserted into the index
					 * then we'll have a corrupt index.  By reference allows us to cheat
					 * on the storage requirements for each key, but it does risk a 
					 * corrupt index.
					 *
					 * Play it safe.
					 */

					vh_tvs_store_htp_hf(&tvs[i], *htp_slot, root->cols[i].hf);

					cursor += cols[i].sz;

					break;
			}
		}

		if (has_nulls && i > 0 && i % 7 == 0)
			null_flags++;
	}
}

/*
 * bt_node_form
 *
 * Puts values at a specific itemidx, accounts for alignment
 */
static void
bt_node_form(struct btRootData *root,
			 struct btNodeData *node,
			 struct btNodeItemData *item,
			 void **values,
			 uint16_t *lengths,
			 uint16_t *paddings,
			 bool *nulls)
{
	btKeyColumn cols;
	int16_t i;
	uint16_t *lenword, val_sz = 0, align_diff;
	uint32_t htp_at, *htp_val;
	unsigned char *cursor;
	size_t tam_length, tam_cursor;
	HeapTuplePtr *htp;
	bool has_nulls = root->hasnulls, htp_inserted = false;

	cols = root->cols;
	cursor = (unsigned char*)(item + 1);

	for (i = 0; i < root->ncols; i++)
	{
		if (has_nulls && nulls[i])
		{
			item->t_info |= bt_ni_flag_null;
			continue;
		}

		switch (cols[i].comp_strategy)
		{
			case BT_KCS_VALUE:

				if (cols[i].varlen)
				{
					lenword = (uint16_t*)cursor;
					*lenword = lengths[i];

					val_sz += *lenword;

					cursor = (unsigned char*)(lenword + 1);

					/*
					 * Call the binary TAM to set the value for the key on the
					 * node.
					 */
					tam_length = lengths[i] - sizeof(uint16_t);
					tam_cursor = 0;

					vh_tam_fire_bin_get(cols[i].tys,
										&bt_binaryamopts,
										values[i],
										cursor,
										&tam_length,
										&tam_cursor);
				}
				else
				{
					if (cols[i].byval)
					{
						switch (cols[i].sz)
						{
							case sizeof(char):
								*cursor = *((char*)values[i]);
								break;

							case sizeof(int16_t):
								*((int16_t*) cursor) = *((int16_t*)values[i]);
								break;

							case sizeof(int32_t):
								*((int32_t*) cursor) = *((int32_t*)values[i]);
								break;

							case sizeof(int64_t):
								*((int64_t*) cursor) = *((int64_t*)values[i]);
								break;
						}

						cursor += cols[i].sz;
						val_sz += cols[i].sz;
					}
					else
					{
					}
				}

				cursor += paddings[i];
				val_sz += paddings[i];

				break;

			case BT_KCS_HTP:

				htp = (HeapTuplePtr*)cursor;
				*htp = (HeapTuplePtr)values[i];

				cursor += cols[i].sz + paddings[i];
				val_sz += cols[i].sz + paddings[i];

				break;

			case BT_KCS_HTPSLOT:

				/*
				 * The values array is going to have a HeapTuplePtr that we'll
				 * need to insert into the SList
				 */

				if (!htp_inserted)
				{
					if (!root->htps)
						vh_htp_SListCreate_ctx(root->htps, root->mctx);

					htp_at = vh_SListSize(root->htps);
					vh_htp_SListPush(root->htps, values[i]);

					htp_inserted = true;
				}

				htp_val = (uint32_t*)cursor;
				*htp_val = htp_at;

				cursor += cols[i].sz + paddings[i];
				val_sz += cols[i].sz + paddings[i];

				break;
		}
	}

	/*
	 * Make sure our key "ends" on a sizeof(uintptr_t) boundary.
	 *
	 * bt_node_calc_req_space handles this for us and this is where
	 * we'll put it.
	 */

	if (has_nulls)
		val_sz += root->bitmap_sz;

	if ((align_diff = val_sz % sizeof(uintptr_t)))
		val_sz += (sizeof(uintptr_t) - align_diff);

	item->t_info |= val_sz;
}

static uint16_t
bt_node_calc_req_space(struct btRootData *root,
					   void **values,
					   uint16_t *lengths,
					   uint16_t *paddings,
					   bool *nulls,
					   bool include_value)
{
	btKeyColumn cols;
	size_t val_sz, align_diff, tam_cursor;
	uint16_t sz = 0;
	int16_t i;
	uint16_t padding;
	uint8_t alignments[BT_MAX_COLUMNS];

	sz = sizeof(struct btNodeItemData);
	cols = root->cols;

	for (i = 0; i < root->ncols; i++)
	{
		if (nulls[i])
		{
			paddings[i] = 0;
			continue;
		}

		switch (cols[i].comp_strategy)
		{
			case BT_KCS_HTP:
			case BT_KCS_HTPSLOT:

				lengths[i] = cols[i].sz;
				sz += cols[i].sz;
				alignments[i] = cols[i].alignment;

				break;

			case BT_KCS_VALUE:

				if (cols[i].varlen)
				{
					val_sz = 0;

					/* 
					 * All we care about is getting the length of the binary data.
					 */
					tam_cursor = 0;

					vh_tam_fire_bin_get(cols[i].tys, 
										&bt_binaryamopts, 
										values[i], 
										&padding, 
										&val_sz, 
										&tam_cursor);

					sz += val_sz;
					sz += sizeof(uint16_t);	
				}
				else
				{
					val_sz = cols[i].sz;
				}

				alignments[i] = cols[i].alignment;				
				lengths[i] = val_sz;
				sz += val_sz;

				break;
		}

		if (i == 0)
		{
			paddings[i] = 0;
			continue;
		}

		if (alignments[i-1] < alignments[i])
		{
			padding = alignments[i] - alignments[i - 1];

			paddings[i - 1] = padding;
			paddings[i] = 0;
			sz += padding;
		}
		else
		{
			paddings[i] = 0;
		}	
	}

	if (root->hasnulls)
		sz += root->bitmap_sz;

	/*
	 * Check our alignment, this makes sure the keys and null bitmap
	 * end on an uintpr_t boundary.
	 */	
	
	if (include_value)
		sz += root->value_sz;
	
	/*
	 * Check our alignment again, to make sure that value_sz is uintptr_t
	 * aligned.
	 */
	if ((align_diff = sz % sizeof(uintptr_t)))
	{
		sz += (sizeof(uintptr_t) - align_diff);
	}


	return sz;
}

/*
 * bt_node_insert_pos
 *
 * Note: alignsz must contain the size of struct btNodeItemData
 * plus the key length plus the null bitmap if applicable plus the
 * value size for this to work correctly.  Otherwise we'll end up
 * overwriting things all over the place if we're not careful.
 */
static int32_t 
bt_node_insert_pos(struct btRootData *root,
				   struct btNodeData *node,
				   void **values,
				   uint16_t *lengths,
				   uint16_t *paddings,
 				   bool *nulls,
				   uint16_t itemoff,
				   uint16_t alignsz)
{
	btNodeItem item;
	
	assert(node->d_freespace > alignsz);
	assert((alignsz % sizeof(uintptr_t) == 0));
	assert((node->d_lower % sizeof(uintptr_t) == 0));

	/*
	 * Shift the item pointers right by one to free up the index
	 * we're going to be filling.
	 */
	if (bt_n_items(node) && (itemoff <= bt_n_items(node)))
	{
		memmove(&node->items[itemoff], 
				&node->items[itemoff - 1],
				sizeof(uint16_t) * (bt_n_items(node) - itemoff + 1));
	}


	node->d_lower -= alignsz;
	node->d_upper += sizeof(uint16_t);
	node->d_freespace = node->d_lower - node->d_upper;
	node->items[itemoff - 1] = node->d_lower;

	item = bt_node_itemptr(node, itemoff);

	/*
	 * Put the data on the node
	 */
	bt_node_form(root,
				 node,
				 item,
				 values,
				 lengths,
				 paddings,
				 nulls);

	return 0;
}

/*
 * bt_traverse_leaf
 *
 * We just want to get to the left or right most leaf node.
 */
static int32_t 
bt_traverse_leaf(struct btRootData *root,
	   			 struct btNodeData **node,
				 bool rightmost)
{
	int32_t max_keys, data_key;
	btNode n;

	n = root->root;

	while (n)
	{
		if (n->flags & bt_n_flag_leaf)
		{
			*node = n;

			while (n->right)
				n = n->right;

			return 1;
		}

		max_keys = bt_n_items(n);
		data_key = BT_FIRSTDATAKEY(n);

		n = bt_node_downpointer(n, rightmost ? max_keys : data_key);
	}

	return 0;
}

static struct btStackData*
bt_traverse_leaf_stack(struct btRootData *root,
	   				   struct btNodeData **node,
					   struct btStackData **at,
				 	   bool rightmost)
{
	struct btStackData *stack_head, *stack;
	int32_t max_keys, data_key;
	btNode n;

	if (!root->depth)
		return 0;

	n = root->root;
	stack_head = vhmalloc(sizeof(struct btStackData) * root->depth);
	stack = stack_head;
	stack->parent = 0;
	stack->keyidx = -1;
	stack++;

	while (n)
	{
		if (n->flags & bt_n_flag_leaf)
		{
			*node = n;

			while (n->right)
				n = n->right;

			return stack_head;
		}

		max_keys = bt_n_items(n);
		data_key = BT_FIRSTDATAKEY(n);

		stack->parent = n;
		stack->keyidx = rightmost ? max_keys : data_key;
		n = bt_node_downpointer(n, rightmost ? max_keys : data_key);

		*at = stack;
		stack++;
	}

	vhfree(stack_head);

	return 0;
}

static int32_t 
bt_delete(struct btRootData *root,
		  struct btStackData *stack,
		  struct btNodeData *node,
		  uint16_t itemoffset)
{
	btNode copy;
	int32_t max_keys = bt_n_items(node), i, j;

	if (max_keys == BT_FIRSTDATAKEY(node))
	{
		/*
		 * We're the last key on the page, so we need to do some extra work
		 * to remove ourselves from the sibling chain and potentially cascade up
		 * the stack to clean up the parent nodes.
		 */

		if (node->right)
			return bt_delete_node(root, stack, node);
	}
	else
	{
		copy = bt_node_create(root);
		
		for (i = BT_HIGHKEY, j = BT_HIGHKEY; i <= max_keys; i++)
		{
			if (i == itemoffset)
				continue;

			bt_node_copyoff(root, node, copy, i, j);
			j++;
		}

		copy->flags = node->flags;
		copy->left = node->left;
		copy->right = node->right;

		memcpy(node, copy, root->node_sz);

		vhfree(copy);
	}

	return 0;
}

static int32_t
bt_delete_node(struct btRootData *root,
			   struct btStackData *stack,
			   struct btNodeData *node)
{
	btNode lsib, rsib, copy;
	int32_t i, j, max;
	bool is_leaf = (node->flags & bt_n_flag_leaf) != 0, rightmost;

	rightmost = BT_RIGHTMOST(node);

	lsib = node->left;
	rsib = node->right;

	if (lsib)
		lsib->right = rsib;

	if (rsib)
		rsib->left = lsib;

	if (is_leaf)
		root->leaves--;

	vhfree(node);

	if (rightmost && lsib)
	{
		/*
		 * The left sibling will now be the right most page, so we'll
		 * have to blow the high key.  Again, the easiest way to do this
		 * is to create a new page, build it from scratch without the
		 * high key and then memcpy this newly built page into the old
		 * page @lsib.
		 */

		copy = bt_node_create(root);
		max = bt_n_items(lsib);

		for (i = BT_HIGHKEY, j = BT_DATAKEY; j <= max; i++, j++)
		{
			bt_node_copyoff(root, lsib, copy, j, i);
		}

		copy->flags = lsib->flags;
		copy->left = lsib->left;

		memcpy(lsib, copy, root->node_sz);

		vhfree(copy);
	}

	/*
	 * Remove the downlink from the parent
	 */

	return bt_delete(root, stack - 1, stack->parent, stack->keyidx);
}


void
vh_bt_print_tree(struct btRootData* root)
{
	bt_print_tree_impl(root, root->root, 0);
	printf("\n");
}

static void
bt_print_tree_impl(struct btRootData *root,
				   struct btNodeData *node, 
				   int32_t depth)
{
	uint16_t i, c, j;
	btNode dp;

	c = bt_n_items(node);

	for (j = 0; j < depth; j++)
		printf("\t");


	if (!(node->flags & bt_n_flag_leaf))
	{
		printf("[%p]\t[%d]", node, c);

		for (i = BT_FIRSTDATAKEY(node); i <= c; i++)
		{
			dp = bt_node_downpointer(node, i);
			printf("\t[%p]", dp);
		}

		printf("\n");
		for (j = 0; j < depth + 2; j++)
			printf("\t");

		printf("keys\t");
		bt_print_node_keys(root, node);
		printf("\n");

		for (i = BT_FIRSTDATAKEY(node); i <= c; i++)
		{
			dp = bt_node_downpointer(node, i);

			bt_print_tree_impl(root, dp, depth + 1);
		}
	}	
	else
	{
		printf("leaf [%p]\t[%d]", node, c);

		printf("\n");

		for (j = 0; j < depth + 2; j++)
			printf("\t");

		printf("keys\t");
		bt_print_node_keys(root, node);
		printf("\n");
	}
	
}

static void
bt_print_node_keys(struct btRootData *root,
				   struct btNodeData *node)
{
	static struct CStrAMOptionsData cstropts = { true };

	btKeyColumn cols;
	uint16_t i, j, c;
	vh_tam_cstr_get getter;
	size_t len, cur;

	char *keyval;

	void *values[BT_MAX_COLUMNS];
	bool nulls[BT_MAX_COLUMNS];

	cols = root->cols;
	c = bt_n_items(node);

	for (i = 1; i <= c; i++)
	{
		bt_node_deform(root, node, i, values, nulls, false);

		printf("{");

		for (j = 0; j < root->ncols; j++)
		{
			switch (cols[j].comp_strategy)
			{
				case BT_KCS_VALUE:

					getter = cols[j].tys[0]->tam.cstr_get;
					
					len = 0;
					cur = 0;

					keyval = getter(0, &cstropts, values[j], 0, &len, &cur, 0);

					break;

				case BT_KCS_HTP:

					getter = vh_type_int64.tam.cstr_get;

					len = 0;
					cur = 0;

					keyval = getter(0, &cstropts, values[j], 0, &len, &cur, 0);

					break;

				case BT_KCS_HTPSLOT:

					getter = vh_type_int32.tam.cstr_get;

					len = 0;
					cur = 0;

					keyval = getter(0, &cstropts, values[j], 0, &len, &cur, 0);

					break;
			}

			if (j > 0)
				printf(",");

			if (j == 0 && i == BT_HIGHKEY && node->right )
				printf("HK: ");

			if (keyval)
				printf("%s", keyval);
		}

		printf("}\t");
	}
}

static void
bt_col_make_var(struct btRootData *root, void **cols)
{
	int32_t i;

	for (i = 0; i < root->ncols; i++)
	{
		if (root->cols[i].comp_strategy == BT_KCS_VALUE &&
			root->cols[i].varlen)
		{
			cols[i] = vh_typevar_make_tys(root->cols[i].tys);
		}
		else
		{
			cols[i] = 0;
		}
	}
}

static void
bt_col_destroy_var(struct btRootData *root, void **cols)
{
	int32_t i;

	for (i = 0; i < root->ncols; i++)
	{
		if (root->cols[i].comp_strategy == BT_KCS_VALUE &&
			root->cols[i].varlen)
		{
			if (cols[i])
			{
				vh_typevar_destroy(cols[i]);
			}
		}
	}
}

