/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <stdio.h>
#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/utils/SList.h"
#include "io/utils/htbl.h"

/*
 * TableDefFuncs
 */


static TableRel tdr_rel_add_impl(TableDefVer tdv_inner,
								 TableDefVer tdv_outter,
	  							 RelationCardinality rc);

static TableRelQual tdv_rel_qual_add_impl(TableRel tr_inner, 
										  TableField tf_inner, 
					   					  TableField tf_outter);

static TableRel tdr_get_impl(TableDefVer tdv_inner,
							 TableDefVer tdv_outter);

static TableRel tdr_get_fields_impl(TableRel tr, TableRelQual *quals, uint16_t nquals);

static void td_init(TableDef table, bool with_versions);
static void tdv_init(TableDefVer tdv, bool with_name);

static bool td_tdv_it_finalize(HashTable htbl, const void *key, void *entry, void *data);

static void td_tdv_tf_finalize(HeapTupleDef htd, void *tf);

static TableField tdv_tf_add_impl(TableDef td, 
								  TableDefVer tdv, 
								  Type *tys, 
								  const char *field_name);

/*
 * TableOpFuncs
 */

static int32_t TD_CompareLK(HeapTuplePtr, HeapTuplePtr);
static int32_t TD_ComparePK(HeapTuplePtr, HeapTuplePtr);
static inline int32_t TD_CompareFields(HeapTuplePtr a, 
									   HeapTuplePtr b, 
									   TableField* tfs, 
									   uint16_t tfsz);
static inline int32_t TD_CompareFields_HT(HeapTuple a, 
				   						  HeapTuple b, 
				   						  TableField* tfs, 
			   							  uint16_t tfsz);
static TableKey TD_LK(TableDef td);
static bool TD_LKChanged(HeapTuplePtr ht);
static TableKey TD_PK(TableDef td);
static bool TD_PKChanged(HeapTuplePtr ht);
static void TD_Sort(HeapTuplePtr* hts, uint32_t htsz, TableField* tfs, uint16_t tfsz);
static void TD_SortKey(HeapTuplePtr *hts, uint32_t htsz, TableKey *tk);


/*
 * QuickSort3 Implementation
 */

#define dp_swap(items, i, j)  swap = items[i]; \
								items[i] = items[j]; \
								items[j] = swap;

struct QSC_Fields
{
	TableField *tfs;
	uint16_t ntfs;
};

typedef int32_t (*QSortComp)(void *lhs, void *rhs, void *params);

static inline int32_t QSortComp_Fields(HeapTuplePtr l,
			   						   HeapTuplePtr r,
			   						   const struct QSC_Fields * const params);
static inline int32_t QSortComp_Fields_HT(HeapTuple l,
			   							  HeapTuple r,
			   							  const struct QSC_Fields * const params);
static inline int32_t QSortComp_TableKey(HeapTuplePtr l,
			   							 HeapTuplePtr r,
			   							 TableKey *params);
static void QSort_3Pivot(void **items,
	 					 QSortComp const comp,
 						 void * const comp_params,
 						 int32_t left, 
 						 int32_t right); 

void
vh_td_copyfqname(String target, TableDef td)
{
	assert(target);
	assert(td);
	assert(td->tname);

	if (td->sname)
	{
		vh_str.AppendStr(target, td->sname);
		vh_str.Append(target, ".");
	}

	vh_str.AppendStr(target, td->tname);
}

TableField
vh_td_tf_add(TableDef td, Type *tys, const char *field_name)
{
	TableDefVer tdv;

	if (td)
		tdv = vh_td_tdv_lead(td);

	if (!tdv)
	{
		elog(ERROR1,
				emsg("Leading schema version could not be detected, unable to add field "
					"as requested."));

		return 0;
	}

	return tdv_tf_add_impl(td, tdv, tys, field_name);
}

TableField
vh_tdv_tf_add(TableDefVer tdv, Type *tys, const char *field_name)
{
	TableDef td;

	if (tdv)
		td = tdv->td;

	if (!td)
	{
		elog(ERROR1,
				emsg("Corrupt TableDefVer passed at %p",
					tdv));

		return 0;
	}

	return tdv_tf_add_impl(td, tdv, tys, field_name);
}

static TableField
tdv_tf_add_impl(TableDef td, TableDefVer tdv, Type *tys, const char *field_name)
{
	MemoryContext mctx_old, mctx_target;
	TableField tf, *tf_htbl;
	Type ty;
	int32_t ret;

	/*
	 * Go in the table catalog context otherwise put us in the current memory
	 * context.  For late-bind queries we don't want all these table fields going
	 * into the top context, where it gets much more complicated to get them
	 * out.
	 */

	if (td->tc)
		mctx_target = vh_cat_tbl_mctx(td->tc);
	else
		mctx_target = vh_mctx_current();
	
	mctx_old = vh_mctx_switch(mctx_target);

	tf = vh_tf_create();
	tf->tdv = tdv;
	tf->fname = vh_str.Convert(field_name);
	tf->db = false;
	
	/*
	 * Iterate the type stack
	 */

	ty = tys[0];

	while (ty)
	{
		vh_hf_push_type(&tf->heap, ty);
		ty = (*(++tys));
	}

	tf_htbl = vh_htbl_put(tdv->field_byname, vh_str_buffer(tf->fname), &ret);
	assert(ret == 1 || ret == 2);
	*tf_htbl = tf;

	vh_htd_add_field(&tdv->heap, &tf->heap);

	vh_mctx_switch(mctx_old);

	return tf;
}

/*
 * vh_td_tf_name
 *
 * Gets a TableField by name from the leading schema version.
 */
TableField
vh_td_tf_name(TableDef td, const char *field_name)
{
	TableDefVer tdv = vh_td_tdv_lead(td);
	TableField *tf;

	if (!td)
	{
		elog(WARNING,
				emsg("vh_td_tf_name called without a valid TableDef pointer!"));

		return 0;
	}

	if (!tdv)
	{
		elog(WARNING,
				emsg("Incomplete or corrupt TableDef a pointer %p, unable to determine "
					 "the leading schema version"));
		return 0;
	}

	tf = vh_htbl_get(tdv->field_byname, field_name);

	if (!tf)
	{
		elog(WARNING,
				emsg("Field [%s] could not be found in table %s",
					field_name,
					vh_str_buffer(td->tname)));

		return 0;
	}

	return *tf;
}

TableField
vh_td_tf_name_ver(TableDef td, const char *version_name, const char *field_name)
{
	TableDefVer tdv;
	TableField tf;

	if (td->has_versions)
	{
		tdv = vh_htbl_get(td->versions.byname, version_name);

		if (!tdv)
		{
			elog(WARNING,
					emsg("Table versions [%s] could not be found for table [%s]",
						version_name,
						vh_str_buffer(td->tname)));
		}
	}
	else
	{
		tdv = vh_td_tdv_lead(td);

		if (!tdv)
		{
			elog(WARNING,
					emsg("Table [%s] has no leading version",
						vh_str_buffer(td->tname)));
			return 0;
		}
	}

	tf = vh_htbl_get(tdv->field_byname, field_name);

	if (!tf)
	{
		elog(WARNING,
				emsg("Table [%s] has no field named [%s]",
					 vh_str_buffer(td->tname),
					 field_name));

		return 0;
	}

	return tf;
}

TableField
vh_tdv_tf_name(TableDefVer tdv, const char *field_name)
{
	TableField *tf;
	TableDef td;

	if (tdv)
	{
		td = tdv->td;

		if (td)
		{
			tf = vh_htbl_get(tdv->field_byname, field_name);

			if (!tf)
			{
				elog(WARNING,
						emsg("Table [%s] has no field named [%s]",
							td->tname ? vh_str_buffer(td->tname) : 0,
							field_name));

				return 0;
			}

			return *tf;
		}
	}

	return 0;
}

SList
vh_tdv_tf_name_filter(TableDefVer tdv, const char **field_list,
					  int32_t nfields, bool mode_exclude)
{
	SList fields;
	TableField *tf_head, tf;
	int32_t i, j, tf_sz;
	bool kill = false;

	if (!tdv)
	{
		elog(WARNING,
				emsg("Invalid TableDefVer pointer [%p] passed to "
					 "vh_tdv_tf_name_filter.  Unable to filter the "
					 "fields by name as requested.",
					 tdv));
		return 0;
	}

	/*
	 * We don't want to check for an invalid field_list or nfields,
	 * because it's possible the caller wants all fields.
	 */

	fields = vh_SListCreate();
	tf_sz = vh_SListIterator(tdv->heap.fields, tf_head);

	for (i = 0; i < tf_sz; i++)
	{
		tf = tf_head[i];
		kill = !mode_exclude;
		
		if (nfields == 0 && !mode_exclude)
			kill = false;

		for (j = 0; j < nfields; j++)
		{
			if (strcmp(vh_str_buffer(tf->fname), field_list[j]) == 0)
			{
				kill = mode_exclude;
				break;
			}
		}

		if (!kill)
			vh_SListPush(fields, tf);
	}

	return fields;
}

SList
vh_tdv_tf_filter(TableDefVer tdv, TableField *field_list,
				 int32_t nfields, bool mode_exclude)
{
	SList fields;
	TableField *tf_head, tf;
	int32_t i, j, tf_sz;
	bool kill = false;

	if (!tdv)
	{
		elog(WARNING,
				emsg("Invalid TableDefVer pointer [%p] passed to "
					 "vh_tdv_tf_name_filter.  Unable to filter the "
					 "fields by name as requested.",
					 tdv));
		return 0;
	}

	/*
	 * We don't want to check for an invalid field_list or nfields,
	 * because it's possible the caller wants all fields.
	 */

	fields = vh_SListCreate();
	tf_sz = vh_SListIterator(tdv->heap.fields, tf_head);

	for (i = 0; i < tf_sz; i++)
	{
		tf = tf_head[i];
		kill = !mode_exclude;

		if (nfields == 0 && !mode_exclude)
			kill = false;

		for (j = 0; j < nfields; j++)
		{
			if (strcmp(vh_str_buffer(tf->fname), 
					   vh_str_buffer(field_list[j]->fname)) == 0)
			{
				kill = mode_exclude;
				break;
			}
		}

		if (!kill)
			vh_SListPush(fields, tf);
	}

	return fields;
}

/*
 * vh_td_create
 *
 * Creates a new TableDef with or without versionining.  Versioning on a table
 * may be upgraded at any time, but may not be downgraded.  The TableDef
 * infrastructure has no way of knowing if there are remaining HeapTuplePtr
 * references to a given version.
 */
TableDef
vh_td_create(bool with_versions)
{
	TableDef td;
	TableDefVer tdv;

	if (with_versions)
	{
		td = vhmalloc(sizeof(TableDefData));

		if (td)
		{
			td_init(td, with_versions);
		}
	}
	else
	{
		td = vhmalloc(sizeof(TableDefData) +
					  sizeof(struct TableDefVerData));

		tdv = (TableDefVer)(td + 1);

		if (td)
		{
			td_init(td, false);
			tdv_init(tdv, false);

			td->singlever = tdv;

			tdv->td = td;
		}
	}

	return td;
}

/*
 * td_init
 *
 * Initializes a TableDef, we are careful to initialize the heap for single 
 * version mode
 */
static void
td_init(TableDef td, bool with_versions)
{
	struct HashTableOpts hopts = { };
	HashTable htbl;
	
	memset(td, 0, sizeof(struct TableDefData));

	td->has_versions = with_versions;

	if (with_versions)
	{
		hopts.key_sz = sizeof(const char*);
		hopts.value_sz = sizeof(TableField);
		hopts.func_hash = vh_htbl_hash_str;
		hopts.func_compare = vh_htbl_comp_str;
		hopts.mctx = vh_mctx_current();
		hopts.is_map = true;

		htbl = vh_htbl_create(&hopts, VH_HTBL_OPT_KEYSZ |
									  VH_HTBL_OPT_VALUESZ |
									  VH_HTBL_OPT_HASHFUNC |
									  VH_HTBL_OPT_COMPFUNC |
									  VH_HTBL_OPT_MCTX |
									  VH_HTBL_OPT_MAP );

		td->versions.leading_ver = 0;
		td->versions.byname = htbl;
	}
}


TableDefVer
vh_tdv_create(TableDef td, const char *version_name, bool make_leading)
{
	MemoryContext mctx_tgt, mctx_old;
	TableDefVer tdv, tdvl;
	size_t namelen;
	struct HashTableOpts hopts = { };
	HashTable htbl;

	if (!version_name)
	{
		elog(ERROR1,
				emsg("Unable to generate a new TableDefVer, a version name was not "
					 "supplied by the user"));

		return 0;
	}
	
	namelen = strlen(version_name);

	if (td->tc)
		mctx_tgt = vh_cat_tbl_mctx(td->tc);
	else
		mctx_tgt = vh_mctx_top();

	mctx_old = vh_mctx_switch(mctx_tgt);

	/*
	 * If we're not in version mode yet, we need to work some magic to do
	 * that.
	 */
	if (!td->has_versions)
	{
		hopts.key_sz = sizeof(const char*);
		hopts.value_sz = sizeof(TableField);
		hopts.func_hash = vh_htbl_hash_str;
		hopts.func_compare = vh_htbl_comp_str;
		hopts.mctx = vh_mctx_current();
		hopts.is_map = true;

		htbl = vh_htbl_create(&hopts, VH_HTBL_OPT_KEYSZ |
									  VH_HTBL_OPT_VALUESZ |
									  VH_HTBL_OPT_HASHFUNC |
									  VH_HTBL_OPT_COMPFUNC |
									  VH_HTBL_OPT_MCTX |
									  VH_HTBL_OPT_MAP );

		td->versions.leading_ver = td->singlever;
		td->versions.byname = htbl;

		td->has_versions = true;
	}

	tdvl = vh_td_tdv_lead(td);

	tdv = vhmalloc(sizeof(struct TableDefVerData) + namelen);
	tdv_init(tdv, true);

	strcpy(&tdv->name[0], version_name);

	if (make_leading)
	{
		if (tdvl)
		{
			tdv->ver_num = tdvl->ver_num + 1;
			tdv->ver_next = 0;
			tdv->ver_prior = tdv;
			
			tdvl->ver_next = tdv;
			td->versions.leading_ver = tdv;
		}
		else
		{
			tdv->ver_num = 1;
			tdv->ver_next = 0;
			tdv->ver_prior = 0;

			td->versions.leading_ver = tdv;
		}
	}

	vh_mctx_switch(mctx_old);

	return tdv;
}

/*
 * tdv_init
 *
 * Initializes a TableDefVer, by creating the field lookup hash table among other
 * things.
 */
static void
tdv_init(TableDefVer tdv, bool with_name)
{
	struct HashTableOpts hopts = { };

	memset(tdv, 0, sizeof(struct TableDefVerData) - 1);

	hopts.key_sz = sizeof(const char*);
	hopts.value_sz = sizeof(TableField);
	hopts.func_hash = vh_htbl_hash_str;
	hopts.func_compare = vh_htbl_comp_str;
	hopts.mctx = vh_mctx_current();
	hopts.is_map = true;
		
	tdv->field_byname = vh_htbl_create(&hopts, VH_HTBL_OPT_KEYSZ |
		   									   VH_HTBL_OPT_VALUESZ |
										   	   VH_HTBL_OPT_HASHFUNC |
											   VH_HTBL_OPT_COMPFUNC |
											   VH_HTBL_OPT_MCTX |
											   VH_HTBL_OPT_MAP );	   

	vh_htd_init(&tdv->heap);

	if (!with_name)
		tdv->name[0] = '\0';
}

void
vh_td_finalize(TableDef td)
{
	TableDefVer tdv;

	if (td->has_versions)
	{
		vh_htbl_iterate_map(td->versions.byname, 
							td_tdv_it_finalize,
							0);
	}
	else
	{
		tdv = vh_td_tdv_lead(td);
		
		if (tdv)
		{
			vh_tdv_finalize(tdv);
		}
	}

	if (td->sname)
		vh_str.Destroy(td->sname);

	if (td->tname)
		vh_str.Destroy(td->tname);
}

/*
 * td_tdv_it_finalize
 *
 * HashTable iterate function definition for finalizing the underlying
 * TableDefVer objects.
 */
static bool 
td_tdv_it_finalize(HashTable htbl, const void *key, 
				   void *entry, void *data)
{
	TableDefVer tdv = entry;

	vh_htbl_destroy(tdv->field_byname);
	vh_htd_finalize(&tdv->heap, td_tdv_tf_finalize);

	return true;
}


void
vh_tdv_finalize(TableDefVer tdv)
{
	vh_htbl_destroy(tdv->field_byname);
	vh_htd_finalize(&tdv->heap, td_tdv_tf_finalize);
}


static void
td_tdv_tf_finalize(HeapTupleDef htd, void *data)
{
	union
	{
		TableField tf;
		HeapField hf;
	} f = { .tf = data };

	if (vh_hf_is_tablefield(f.hf))
		vh_tf_finalize(f.tf);
}

TableDefVer
vh_tdv_get(TableDef td, const char *version_name)
{
	TableDefVer tdv;

	if (!version_name)
		elog(ERROR1,
				emsg("version_name not provided to vh_tdv_get"));

	if (td->has_versions)
	{
		tdv = vh_htbl_get(td->versions.byname, version_name);

		return tdv;
	}
	else
	{
		elog(WARNING,
				emsg("TableDef for table [%s] does not have schema versions",
					 vh_str_buffer(td->tname)));
	}

	return 0;
}

TableKey
vh_td_lk(TableDef td)
{
	TableKey tk_empty = { };
	TableDefVer tdv;

	tdv = vh_td_tdv_lead(td);

	if (!tdv)
	{
		elog(WARNING,
				emsg("No leading schema version is available for table [%s].  "
					 "Unable gather the logical key",
					 vh_str_buffer(td->tname)));

		return tk_empty;
	}

	return tdv->key_logical;
}

TableKey
vh_td_ver_lk(TableDef td, const char *version_name)
{
	TableKey tk_empty = { };
	TableDefVer tdv;

	tdv = vh_tdv_get(td, version_name);

	if (!tdv)
	{
		elog(WARNING,
				emsg("No schema version [%s] is available for table [%s].  "
					 "Unable gather the logical key",
					 version_name,
					 vh_str_buffer(td->tname)));

		return tk_empty;
	}

	return tdv->key_logical;
}

TableKey
vh_td_pk(TableDef td)
{
	TableKey tk_empty = { };
	TableDefVer tdv;

	tdv = vh_td_tdv_lead(td);

	if (!tdv)
	{
		elog(WARNING,
				emsg("No leading schema version is available for table [%s].  "
					 "Unable gather the primary key",
					 vh_str_buffer(td->tname)));

		return tk_empty;
	}

	return tdv->key_primary;
}

TableKey
vh_td_ver_pk(TableDef td, const char *version_name)
{
	TableKey tk_empty = { };
	TableDefVer tdv;

	tdv = vh_tdv_get(td, version_name);

	if (!tdv)
	{
		elog(WARNING,
				emsg("No schema version [%s] is available for table [%s].  "
					 "Unable gather the primary key",
					 version_name,
					 vh_str_buffer(td->tname)));

		return tk_empty;
	}

	return tdv->key_primary;
}

TableRel
vh_tdv_rel_add(TableDefVer tdv_inner,
			   TableDefVer tdv_outter,
			   RelationCardinality rc)
{
	MemoryContext mold, mtop;
	TableRel tr_inner, tr_outter;
	RelationCardinality rcop;

	if (tdv_inner && tdv_outter)
	{
		mtop = vh_mctx_top();
		mold = vh_mctx_switch(mtop);

		if ((tr_inner = vh_tdr_tdv_get(tdv_inner, tdv_outter)))
		{
			/*
			 * Set to null if we don't need to set the tr_inner->op
			 * value later on.
			 */
			tr_inner = 0;
		}
		else
		{
			tr_inner = tdr_rel_add_impl(tdv_inner, tdv_outter, rc);
			tr_inner->ht_offset = vh_htd_add_extra(&tdv_inner->heap, sizeof(uintptr_t));
		}

		/*
		 * Setup the inverse for tr_outter and inject it into
		 * td_outter table.
		 */

		if ((tr_outter = vh_tdr_tdv_get(tdv_outter, tdv_inner)))
		{
			tr_outter = 0;
		}
		else
		{
			switch (rc)
			{
			case Rel_OneToOne:
			case Rel_ManyToMany:
				rcop = rc;
				break;

			case Rel_OneToMany:
			   	rcop = Rel_ManyToOne;
	   			break;

			case Rel_ManyToOne:
	   			rcop = Rel_OneToMany;
	   			break;
			}
			
			tr_outter = tdr_rel_add_impl(tdv_outter, tdv_inner, rcop);
			tr_outter->ht_offset = vh_htd_add_extra(&tdv_outter->heap, sizeof(uintptr_t));
		}

		if (tr_outter && tr_inner)
		{
			tr_outter->op = tr_inner;
			tr_inner->op = tr_outter;
		}

		vh_mctx_switch(mold);

		return tr_inner;
	}

	return 0;
}

TableRel 
tdr_rel_add_impl(TableDefVer tdv_inner,
	 			 TableDefVer tdv_outter,
	  			 RelationCardinality rc)
{
	TableRel tr_inner, tr_orig;

	tr_inner = (TableRel) vhmalloc(sizeof(struct TableRelData));

	tr_inner->td_inner = tdv_inner;
	tr_inner->td_outter = tdv_outter;
	tr_inner->nquals = 0;
	tr_inner->card = rc;

	if (tdv_inner->nrels == 1)
	{
		tr_orig = (TableRel) tdv_inner->rels;
		tdv_inner->rels = (TableRel*) vhmalloc(sizeof(TableRel) * 2);
		tdv_inner->rels[0] = tr_orig;
		tdv_inner->rels[1] = tr_inner;
		tdv_inner->nrels++;
	}
	else if (tdv_inner->nrels > 1)
	{
		tdv_inner->rels = (TableRel*) vhrealloc(tdv_inner->rels,
											   sizeof(TableRel) *
	 										   (tdv_inner->nrels + 1));
		tdv_inner->rels[tdv_inner->nrels] = tr_inner;
		tdv_inner->nrels++;
	}
	else
	{
		tdv_inner->rels = (TableRel*) tr_inner;
		tdv_inner->nrels = 1;
	}

	return tr_inner;
}

static TableRelQual
tdv_rel_qual_add_impl(TableRel tr_inner, 
					  TableField tf_inner, 
	   				  TableField tf_outter)
{
	TableRelQual trq_inner = 0;
	uint16_t i;

	if (tr_inner->nquals > 9)
	{
		elog(ERROR1,
			 emsg("Unable to add TableDef relationship qualification: "
				  "maximum number of qualifications reached for this "
				  "table (%s-%s) pairing.",
				  vh_str_buffer(tr_inner->td_inner->td->sname),
				  vh_str_buffer(tr_inner->td_outter->td->sname)));
		return 0;
	}

	for (i = 0; i < tr_inner->nquals; i++)
	{
		if (tr_inner->quals[i]->tf_inner == tf_inner &&
			tr_inner->quals[i]->tf_outter == tf_outter)
		{
			trq_inner = tr_inner->quals[i];
			break;
		}
	}

	if (!trq_inner)
	{
		trq_inner = (TableRelQual) vhmalloc(sizeof(struct TableRelQualData));
		trq_inner->tf_inner = tf_inner;
		trq_inner->tf_outter = tf_outter;

		tr_inner->quals[tr_inner->nquals] = trq_inner;
		tr_inner->nquals++;
	}

	return trq_inner;
}

void
vh_tdr_qual_add(TableRel tr_inner, 
	 			TableField tf_inner, 
	 			TableField tf_outter)
{
	MemoryContext mold, mcur;

	if (tf_inner->tdv == tr_inner->td_inner &&
		tf_outter->tdv == tr_inner->td_outter)
	{
		mcur = vh_mctx_current();
		mold = vh_mctx_switch(mcur);

		tdv_rel_qual_add_impl(tr_inner, tf_inner, tf_outter);

		if (tr_inner->op)
			tdv_rel_qual_add_impl(tr_inner->op, tf_outter, tf_inner);

		vh_mctx_switch(mold);
	}
}

TableRel
vh_tdr_get(TableDef td_inner, TableDef td_outter)
{
	TableDefVer tdv_inner, tdv_outter;

	tdv_inner = vh_td_tdv_lead(td_inner);
	tdv_outter = vh_td_tdv_lead(td_outter);

	if (!tdv_inner)
	{
		elog(WARNING,
				emsg("Inner TableDef [%s] does not have a leading schema version",
					vh_str_buffer(td_inner->tname)));

		return 0;
	}

	if (!tdv_outter)
	{
		elog(WARNING,
				emsg("Outter TableDef [%s] does not have a leading schema version",
					vh_str_buffer(td_outter->tname)));

		return 0;
	}

	return tdr_get_impl(tdv_inner, tdv_outter);
}

TableRel
vh_tdr_get_ver(TableDef td_inner, TableDef td_outter,
			   const char *version_name)
{
	TableDefVer tdv_inner, tdv_outter;

	tdv_inner = vh_tdv_get(td_inner, version_name);
	tdv_outter = vh_tdv_get(td_outter, version_name);

	if (!tdv_inner)
	{
		tdv_inner = vh_td_tdv_lead(td_inner);

		if (!tdv_inner)
		{
			elog(WARNING,
					emsg("Inner TableDef [%s] does not have the desired schema version ["
						"%s] or a leading schema version",
						version_name,
						vh_str_buffer(td_inner->tname)));

			return 0;
		}
	}

	if (!tdv_outter)
	{
		tdv_outter = vh_td_tdv_lead(td_outter);

		if (!tdv_outter)
		{
			elog(WARNING,
					emsg("Outter TableDef [%s] does not have the desired schema version ["
						"%s] or a leading schema version",
						version_name,
						vh_str_buffer(td_outter->tname)));

			return 0;
		}
	}

	return tdr_get_impl(tdv_inner, tdv_outter);
}

TableRel
vh_tdr_tdv_get(TableDefVer tdv_inner, TableDefVer tdv_outter)
{
	return tdr_get_impl(tdv_inner, tdv_outter);
}

TableRel
tdr_get_impl(TableDefVer tdv_inner,
			 TableDefVer tdv_outter)
{
	TableRel tr_inner;
	uint32_t i;

	if (tdv_inner->nrels == 1)
	{
		tr_inner = (TableRel)tdv_inner->rels;

		if (tr_inner->td_outter == tdv_outter)
			return tr_inner;
	}
	else if (tdv_inner->nrels > 1)
	{
		for (i = 0; i < tdv_inner->nrels; i++)
		{
			tr_inner = tdv_inner->rels[i];

			if (tr_inner->td_outter == tdv_outter)
				return tr_inner;
		}
	}

	return 0;
}


void
vh_tdr_cardinality_change(TableRel tr, RelationCardinality rc)
{
	if (tr)
	{
		tr->card = rc;

		if (tr->op)
		{
			switch (rc)
			{
			case Rel_OneToOne:
			case Rel_ManyToMany:
				tr->op->card = rc;
				break;

			case Rel_ManyToOne:
				tr->op->card = Rel_OneToMany;
				break;

			case Rel_OneToMany:
				tr->op->card = Rel_ManyToOne;
				break;
			}
		}
	}
}

TableRel 
vh_tdr_get_fields(TableDef td_inner, TableDef td_outter,
				  TableRelQual *quals, uint16_t nquals)
{
	TableRel tr;
	
	tr = vh_tdr_get(td_inner, td_outter);

	if (!tr)
	{
	}

	return tdr_get_fields_impl(tr, quals, nquals);
}

TableRel 
vh_tdr_get_fields_ver(TableDef td_inner, TableDef td_outter,
					  const char *version_name,
   					  TableRelQual *quals, uint16_t nquals)
{
	TableRel tr;

	tr = vh_tdr_get(td_inner, td_outter);

	if (!tr)
	{
	}

	return tdr_get_fields_impl(tr, quals, nquals);
}

static TableRel
tdr_get_fields_impl(TableRel tr, TableRelQual *quals, uint16_t nquals)
{
	bool outter_match, inner_match;
	uint16_t i, j;
	TableRelQual qual_outter, qual_inner;

	if (tr && tr->nquals)
	{
		outter_match = true;

		for (i = 0; i < tr->nquals; i++)
		{
			qual_inner = tr->quals[i];
			inner_match = false;

			for (j = 0; j < nquals; j++)
			{
				qual_outter = quals[j];

				/*
				 * a				b	qual_inner
				 * c				d	qual_outter
				 *
				 * a = c && b = d OR
				 * a = d && b = c
				 */

				if (qual_inner->tf_inner == qual_outter->tf_inner &&
					qual_inner->tf_outter == qual_outter->tf_outter)
				{
					inner_match = true;
					break;
				}
				else if (qual_inner->tf_inner == qual_outter->tf_outter &&
						 qual_inner->tf_outter == qual_outter->tf_inner)
				{
					inner_match = true;
					break;
				}
			}

			outter_match &= inner_match;

			if (!outter_match)
				break;
		}

		if (outter_match)
			return tr;
	}

	return 0;
}

static int32_t
TD_CompareLK(HeapTuplePtr htpl,
			 HeapTuplePtr htpr)
{
	TableDefVer tdv;
	HeapTuple l = vh_htp(htpl), r = vh_htp(htpr);

	if (l->htd == r->htd)
	{
		tdv = (TableDefVer) l->htd;

		if (tdv->key_logical.nfields)
			return TD_CompareFields_HT(l,
			   						   r,
		   							   &tdv->key_logical.fields[0],
		   							   tdv->key_logical.nfields);
	}

	return 0;
}

static int32_t
TD_ComparePK(HeapTuplePtr htpl,
			 HeapTuplePtr htpr)
{
	TableDefVer tdv;
	HeapTuple l = vh_htp(htpl), r = vh_htp(htpr);

	if (l->htd == r->htd)
	{
		tdv = (TableDefVer) l->htd;

		if (tdv->key_primary.nfields)
			return TD_CompareFields_HT(l,
		   							   r,
		   							   &tdv->key_primary.fields[0],
		   							   tdv->key_primary.nfields);
	}

	return 0;
}


static inline int32_t 
TD_CompareFields(HeapTuplePtr a,
	 			 HeapTuplePtr b, 
	 			 TableField* tfs, 
	 			 uint16_t tfsz)
{
	HeapTuple hta = vh_htp(a), htb = vh_htp(b);

	return TD_CompareFields_HT(hta, htb, tfs, tfsz);
}
	
static inline int32_t 
TD_CompareFields_HT(HeapTuple a,
		   			HeapTuple b, 
				   	TableField* tfs, 
		   			uint16_t tfsz)
{
	int32_t comp, i = 0;

	do
	{
		//comp = vh_ht_CompareField(a, b, ((HeapField) tfs[i]));
		comp = 1;
		i++;
	} while (comp == 0 && i < tfsz);

	return comp;
}

static void 
TD_Sort(HeapTuplePtr* hts, 
		uint32_t htsz, 
		TableField* tfs, 
		uint16_t tfsz)
{
	static QSortComp const comp = (QSortComp) QSortComp_Fields;
	struct QSC_Fields params;
	void **values;

	values = (void**) hts;
	params.tfs = tfs;
	params.ntfs = tfsz;

	QSort_3Pivot(values, comp, &params, 0, htsz - 1);
}

static void
TD_SortKey(HeapTuplePtr *hts,
		   uint32_t htsz,
		   TableKey *htk)
{
	static QSortComp const comp = (QSortComp) QSortComp_TableKey;
	void **values;

	values = (void**) hts;

	QSort_3Pivot(values, comp, htk, 0, htsz - 1);
}

static void 
QSort_3Pivot(void **items,
			 QSortComp const comp,
			 void * const comp_params,
			 int32_t left, 
			 int32_t right)
{
	int32_t a, b, c, d, dist;
	void *p, *q, *r, *swap;

	dist = right - left;

	if (dist <= 0)
	{
		return;
	} 
	else if (dist == 1)
	{
		if (comp(items[left], items[right], comp_params) > 0)
		{
			dp_swap(items, left, right)
		}

		return;
	}

	a = b = left + 2;
	c = d = right - 1;
	p = items[left], q = items[left + 1], r = items[right];

	while (b <= c)
	{
		while (comp(items[b], q, comp_params) < 0 &&
			   b <= c)
		{
			if (comp(items[b], p, comp_params) < 0)
			{
				dp_swap(items, a, b)
				a++;
			}

			b++;
		}

		while (comp(items[c], q, comp_params) > 0 &&
			   b <= c)
		{
			if (comp(items[c], r, comp_params) > 0)
			{
				dp_swap(items, c, d)
				d--;
			}

			c--;
		}

		if (b <= c)
		{
			if (comp(items[b], r, comp_params) > 0)
			{
				if (comp(items[c], p, comp_params) < 0)
				{
					dp_swap(items, b, a)
					dp_swap(items, a, c)
					a++;
				}
				else
				{
					dp_swap(items, b, c)
				}

				dp_swap(items, c, d)
				b++, c--, d--;
			}
			else
			{
				if (comp(items[c], p, comp_params) < 0)
				{
					dp_swap(items, b, a)
					dp_swap(items, a, c)
					a++;
				}
				else
				{
					dp_swap(items, b, c)
				}

				b++, c--;
			}
		}
	}

	a--, b--, c++, d++;
	dp_swap(items, left + 1, a)
	dp_swap(items, a, b)
	a--;
	dp_swap(items, left, a)
	dp_swap(items, right, d)

	//printf("left=%d\tright=%d\ta=%d\tb=%d\tc=%d\td=%d\n",
	//	   left, right, a, b, c, d);

	QSort_3Pivot(items, comp, comp_params, left, b);
	QSort_3Pivot(items, comp, comp_params, b, right);
}


static inline int32_t 
QSortComp_Fields(HeapTuplePtr l,
				 HeapTuplePtr r,
				 const struct QSC_Fields * const params)
{
	int32_t comp, i = 0;
	HeapTuple htl = vh_htp(l), htr = vh_htp(r);

	do
	{
		//comp = vh_ht_CompareField(htl, htr, ((HeapField) params->tfs[i]));
		comp = 1;
		i++;

		if (htr && htl)
		{
		}
	} while (comp == 0 && i < params->ntfs);

	return comp;
}

static inline int32_t 
QSortComp_Fields_HT(HeapTuple l,
				   	HeapTuple r,
				   	const struct QSC_Fields * const params)
{
	int32_t comp, i = 0;

	do
	{
		//comp = vh_ht_CompareField(l, r, ((HeapField) params->tfs[i]));
		//
		comp = 1;
		i++;
	} while (comp == 0 && i < params->ntfs);

	return comp;
}

static inline 
int32_t QSortComp_TableKey(HeapTuplePtr l,
						   HeapTuplePtr r,
						   TableKey *params)
{
	return TD_CompareFields(l,
		 					r, 
		 					&params->fields[0],
		 					params->nfields);
}


void
vh_td_printfname(TableDef td)
{
	TableDefVer tdv;
	TableField *tf_head, tf;
	uint32_t tf_sz, i;

	tdv = vh_td_tdv_lead(td);

	tf_sz = vh_SListIterator(tdv->heap.fields, tf_head);
	for (i = 0; i < tf_sz; i++)
	{
		tf = tf_head[i];

		printf("\nfield %d: %s", i, vh_str_buffer(tf->fname));
	}
}

