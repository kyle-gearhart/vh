/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_TableDef_H
#define vh_datacatalog_TableDef_H

#include "io/catalog/HeapTupleDef.h"

#define VH_TABLEKEY_MAX_FIELDS		10

struct TableKey
{
	TableField fields[VH_TABLEKEY_MAX_FIELDS];
	uint16_t nfields;
};

typedef enum
{
	Rel_OneToOne,
	Rel_OneToMany,
	Rel_ManyToOne,
	Rel_ManyToMany
} RelationCardinality;

struct TableRelQualData
{
	TableField tf_inner;
	TableField tf_outter;
};

struct TableRelData
{
	TableRel op;
	TableDefVer td_inner;
	TableDefVer td_outter;
	RelationCardinality card;
	TableRelQual quals[10];
	uint32_t ht_offset;
	uint16_t nquals;
};

struct TableDefVerData
{
	HeapTupleDefData heap;

	TableDef td;	
	TableDefVer ver_prior;
	TableDefVer ver_next;

	HashTable field_byname;
	
	TableKey key_primary;
	TableKey key_logical;
	
	uint32_t reloffset;

	uint32_t nrels;	
	TableRel *rels;

	int32_t ver_num;

	char name[1];
};

struct TableDefData
{
	TableCatalog tc;
	Beacon beacon;
	String sname;
	String tname;

	union
	{
		struct
		{
			TableDefVer leading_ver;
			HashTable byname;
		} versions;

		TableDefVer singlever;
	};

	bool has_versions;
};

TableDef vh_td_create(bool with_versions);
TableDefVer vh_tdv_create(TableDef td, const char *name, bool make_leading);

void vh_td_finalize(TableDef td);
void vh_tdv_finalize(TableDefVer tdv);

TableDefVer vh_td_tdv_ver(TableDef, const char *version_name);

TableField vh_td_tf_add(TableDef td, Type *tys, const char *field_name);
TableField vh_td_tf_ver_add(TableDef td, Type *tys, const char *version_name, const char *field_name);
TableField vh_tdv_tf_add(TableDefVer tdv, Type *tys, const char *field_name);

/*
 * Field Name Resolution
 */
TableField vh_td_tf_name(TableDef td, const char *field_name);
TableField vh_td_tf_name_ver(TableDef td, const char *version_name, const char *field_name);
TableField vh_tdv_tf_name(TableDefVer tdv, const char *field_name);

SList vh_tdv_tf_name_filter(TableDefVer tdv, const char **field_list,
							int32_t nfields,
							bool mode_exclude);
SList vh_tdv_tf_filter(TableDefVer tdv, TableField *field_list,
					   int32_t nfields, bool mode_exclude);

#define vh_td_tdv_lead(td)	((td)->has_versions ? (td)->versions.leading_ver 			\
												: (td)->singlever )

#define vh_tdv_htd(tdv)		(&((tdv)->heap))

/*
 * vh_td_htd family of functions return a HeapTupleDef for a given version of a
 * TableDef.
 */
#define vh_td_htd(td)		((td)->has_versions ? &((td)->versions.leading_ver->heap) 	\
												: &((td)->singlever->heap) )

TableKey vh_td_lk(TableDef td);
TableKey vh_td_ver_lk(TableDef td, const char *version_name);

TableKey vh_td_pk(TableDef td);
TableKey vh_td_ver_pk(TableDef td, const char *version_name);

void vh_tdr_qual_add(TableRel tr, TableField tf_inner, TableField tf_outter);
TableRel vh_tdr_get(TableDef td_inner, TableDef td_outter);
TableRel vh_tdr_get_ver(TableDef td_inner, TableDef td_outter, const char *version_name);
TableRel vh_tdr_tdv_get(TableDefVer tdv_inner, TableDefVer tdv_outter);

TableRel vh_tdr_get_fields(TableDef td_inner, TableDef td_outter,
						   TableRelQual *quals, uint16_t nquals);
TableRel vh_tdr_get_fields_ver(TableDef td_inner, TableDef td_outter,
							   const char *version_name,
							   TableRelQual *quals, uint16_t nquals);

void vh_tdr_cardinality_change(TableRel tr, RelationCardinality rc);


/*
 * TableOpFuncs groups all functions which can be performed with
 * a HeapTuple against a table definition.  Often, these will call
 * the underlying HeapTupleOpFuncs methods.
 *
 * CompareLK
 * 		Compares the values of the logical key if one has been defined
 * 		for the table.  If no logical key is defined, -2 will be returned.
 * 		-1 indicates the left HeapTuple is less than the right, 0 indicates
 * 		both are equal, 1 indicate the left HeapTuple is greater than the
 * 		right.  A null value is evalulated as "less than" unless both the
 * 		left and right are null which indicates equality, 0.
 *
 * ComparePK
 * 		Compares teh valeus of the primary keyif one has been defined
 * 		for the table.  If no primary key is defined, -2 will be returned.
 *
 * CompareFields
 * 		Expects an array of TableField and a size of the array.  Is field
 * 		is compared in the two HeapTuple provided.
 *
 * FindAll
 * 		Performs a BINARY SEARCH against the HeapTuple array against the
 * 		set of TableField(s) passed in the TableField array.  The HeapTuple
 * 		passed is compared for equality against the array.  All matching
 * 		HeapTuple found in the array are inserted into the returned SList.
 * 		The boolean parameter indicates if the HeapTuple array has already
 * 		been sorted by the TableField array.
 *
 * FindOne
 *		Perofrms a BINARY SEARCH against the HeapTuple array against the
 *		set of TableField(s) passed in the TableField array.  The HeapTuple
 *		passed is compared for equality against the array.  The first matching
 *		HeapTuple found in the array is returned.  The boolean parameter
 *		indicates if the HeapTuple array has already been sorted by the
 *		TableField array.
 *
 * Sort
 * 		Sorts the HeapTuple array in place by the TableField array.  The
 * 		function attempts to select an optimal sorting algorithm based
 * 		on the TableField(s) presented and the TableDef.  Internally we
 * 		use CompareFields to sort.
 *
 * SortTKey
 * 		Sorts the HeapTuple array based on a given TableKey.
 */ 
struct TableOpFuncs
{
	int32_t (*CompareLK)(HeapTuplePtr, HeapTuplePtr);
	int32_t (*ComparePK)(HeapTuplePtr, HeapTuplePtr);
	int32_t (*CompareFields)(HeapTuplePtr, HeapTuplePtr, TableField*, uint16_t);
	SList (*FindAll)(HeapTuplePtr*, uint32_t, TableField*, uint16_t, HeapTuplePtr, bool);
	HeapTuple (*FindOne)(HeapTuplePtr*, uint32_t, TableField*, uint16_t, HeapTuplePtr, bool);
	HeapTuple (*HeapTuple)(TableDef);
	HeapTuple (*HeapTupleCtx)(TableDef, MemoryContext);
	bool (*LKChanged)(HeapTuplePtr);
	bool (*PKChanged)(HeapTuplePtr);
	void (*Sort)(HeapTuplePtr*, uint32_t, TableField*, uint16_t);
	void (*SortTKey)(HeapTuplePtr*, uint32_t, TableKey*);
};

extern const struct TableOpFuncs 	vh_tblop;

void vh_td_printfnames(TableDef td);
void vh_td_copyfqname(String target, TableDef td);


void vh_tdr_fetch_ht(HeapTuple ht, TableRel *rels, uint32_t nrels);
void vh_tdr_fetch_hts(SList hts, TableRel *rels, uint32_t nrels);

#define vh_getptrnm(htp, flgs, fname) (   \
	( { \
		HeapTuple ht = vh_htp_flags((htp), flgs); \
	  	HeapField hf = (HeapField) \
	  		(ht ? vh_tdv_tf_name((TableDefVer)ht->htd, fname) : 0); \
	  	hf ? vh_ht_field(ht, hf) : 0; \
	  } ))

#define vh_ht_getptrnm(ht, fname) ( \
	( { \
	  	HeapField hf = (HeapField) \
	  		(ht ? vh_tdv_tf_name((TableDefVer)ht->htd, fname) : 0); \
	  	hf ? vh_ht_field(ht, hf) : 0; \
	  } ))

#define vh_htp_hf_nm(htp, fname) ( 												\
	( { 																		\
	  	HeapTuple ht = vh_htp((htp));											\
	  	HeapField hf = (HeapField)												\
	  		(ht ? vh_tdv_tf_name((TableDefVer)ht->htd, fname) : 0);				\
	  	hf ? hf : 0;															\
	  } ) )

#define vh_GetStringNm(htp, fname) 		((String)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname))

/*
 * If we don't clear the null here, the compare function will leave it null if we
 * intend the field to be false.  Since HeapTuple are zeroed out and zero means false,
 * unless the null flag is set.  With a null flag, the field is null.
 */
#define vh_GetBoolNm(htp, fname)		vh_ClearNullNm((htp), (fname)), 				\
		*(bool*)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, (fname))

#define vh_GetInt16Nm(htp, fname)		(*(int16_t*)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname))
#define vh_GetInt32Nm(htp, fname)		(*(int32_t*)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname))
#define vh_GetInt64Nm(htp, fname)		(*(int64_t*)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname))

#define vh_ht_GetStringNm(ht, fname)	((String)vh_ht_getptrnm(ht, fname))
#define vh_ht_GetInt32Nm(ht, fname)		((int32_t*)vh_ht_getptrnm(ht, fname))

#define vh_GetImStringNm(htp, fname)	((const struct StringData*)vh_getptrnm(htp, 0, fname))
#define vh_GetImInt32Nm(htp, fname)		((const int32_t)(*vh_getptrnm(htp, 0, fname)))

#define vh_AsnInt32Nm(htp, fname)		vh_getvalnm(htp, fname, int32_t)
#define vh_AsnInt64Nm(htp, fname)		vh_getvalnm(htp, fname, int64_t)


#define vh_IsNullNm(htp, fname)	( \
	( { \
		HeapTuple ht = vh_htp(htp); \
	  	HeapField hf = (HeapField)(ht ? vh_tdv_tf_name((TableDefVer)ht->htd, fname) : 0); \
	  	vh_htf_isnull(ht, hf); \
	  } ))

#define vh_SetNullNm(htp, fname) ( \
	( { \
	  	HeapTuple ht = vh_htp(htp); \
	  	HeapField hf = (HeapField)(ht ? vh_tdv_tf_name((TableDefVer)ht->htd, fname) : 0); \
	  	hf ? vh_htf_setnull(ht, hf) : 0; \
	  } ))

#define vh_ClearNullNm(htp, fname) ( \
	( { \
	  	HeapTuple ht = vh_htp(htp); \
	  	HeapField hf = (HeapField)(ht ? vh_tdv_tf_name((TableDefVer)ht->htd, fname) : 0); \
	  	hf ? vh_htf_clearnull(ht, hf) : 0; \
	  } ))

#define vh_CompareFieldNm(htpl, htpr, fname) ( \
	( { \
	  	HeapTuple l = vh_htp(htpl), r = vh_htp(htpr); \
	  	HeapField hf = (HeapField)(l && r ? vh_tdv_tf_name((TableDefVer)l->htd, fname) : 0); \
		hf ? vh_ht_CompareField(l, r, hf) : 0; \
 	  } ))

#endif

