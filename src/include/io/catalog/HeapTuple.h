/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef vh_datacatalog_HeapTuple_H
#define vh_datacatalog_HeapTuple_H


/*
 * The HeapTuple structure is an in-memory representation of a instance of a 
 * HeapTupleDef.  It is variable in length based on the HeapTupleDef structure 
 * passed when vh_ht_create is called.
 *
 * The |flags| member is guaranteed to be atleast HeapTupleDef->nfields bytes 
 * long and could be longer depending on alignment padding as indicated by
 * HeapTupleDef->flagpadding.  
 *
 * Header level flags are defined in the first flags slot and use the 
 * VH_HT_FLAG definitions to set the values.  Then there are flags for each
 * field which have a different set of masks.
 */

typedef struct HeapTupleData
{
	HeapTupleDef htd;
	HeapTuplePtr tupcpy;
	Shard shard;
	uint8_t flags[1];
} HeapTupleData, *HeapTuple;

/*
 * HeapTuple Header Flags (flags[0])
 * 
 * CONSTRUCTED
 * 	All fields have had their constructors called.
 *
 * FLUSHED
 * 	The record has been flushed but not yet committed.
 *
 * ROLLEDBACK
 * 	The HeapTuple has been rolled back by an XAct and may be in an inconsistent
 * 	state.
 *
 * MUTABLE
 * 	The HeapTuple is the mutable copy.  The immutable copy is referenced by
 * 	|tupcpy|.  Likewise, when the flag isn't set the HeapTuple is the
 * 	immutable version.  If a |tupcpy| has been specified, a mutable version
 * 	has been created.
 *
 * FETCHED
 * 	Indicates the HeapTuple was fetched from a Back End.
 *
 * RELFETCHED
 * 	Indicates atleast one relationship has been fetched.
 */

#define VH_HT_FLAG_CONSTRUCTED		0x01
#define VH_HT_FLAG_FLUSHED			0x02
#define VH_HT_FLAG_ROLLEDBACK		0x04
#define VH_HT_FLAG_MUTABLE			0x08
#define VH_HT_FLAG_FETCHED			0x10
#define VH_HT_FLAG_RELFETCHED		0x20


/*
 * HeapTuple Field Flags (flags[>0])
 *
 */

#define VH_HTF_FLAG_CONSTRUCTED		0x01
#define VH_HTF_FLAG_NULL 			0x02
#define VH_HTF_FLAG_CHANGED			0x04


bool vh_ht_commit(HeapTuple ht_m, HeapTuple ht_im);
int32_t vh_ht_compare(HeapTuple lhs, HeapTuple rhs, bool track);
HeapTuple vh_ht_construct(HeapTupleDef htd, HeapTuple ht, HeapBufferNo hbno);
bool vh_ht_copy(HeapTuple source, HeapTuple target, HeapBufferNo hbno);
HeapTuple vh_ht_create(HeapTupleDef def);
void vh_ht_destruct(HeapTuple ht);




#define vh_ht_tuple(ht) 	((void*)vh_ht_tuple_ptr(ht))
#define vh_ht_tuple_ptr(ht)	(((char*)ht) + ht->htd->tupoffset)

#define vh_ht_field(ht, hf) ((vh_ht_tuple_ptr(ht)) + \
							((HeapField)hf)->offset)

/*
 * Field manipulation functions, users should always attempt to access fields
 * thru this API because it will set hint flags for which is often leveraged
 * by the back end executor to determine how to flush HeapTuples to the back 
 * end.
 */

#define vh_ht_get(ht, hf)	((void*)vh_ht_field(ht, hf))
void vh_ht_set(HeapTuple ht, HeapField hf, void *value);

/*
 * Heap Tuple Operators
 *
 * The caller should be careful to ensure a pointer to a HeapTupleData 
 * structure is passed, rather than a HeapTuplePtr.  The latter is a uint64_t
 * with specific bits providing information to the HeapBuffer on how to 
 * return the requested item.
 *
 * All macros requiring a pointer to HeapTupleData will begin with vh_ht(f)_.  
 * The counter part taking a HeapTuplePtr will have the same name, just with
 * a vh_ only prefix.  The comparison function taking a pointer to 
 * HeapTupleData is called vh_ht_Comp.  The comparison macro accepting 
 * HeapTuplePtr is vh_Comp.
 *
 * TableDef.h provides additional aliases when users only wish to pass in
 * a HeapTuplePtr with a field name.
 */

#define vh_ht_flagsval(ht)			(((HeapTuple)ht)->flags[0])
#define vh_ht_flagsptr(ht)			(((HeapTuple)ht)->flags[0])
#define vh_ht_flags(ht)				vh_ht_flagsval(ht)

/*
 * Heap Tuple Field flags
 */
#define vh_htf_flagsval(ht, hf)	((((HeapTuple)ht)->flags[(((HeapField)hf)->heapord + 1)]))
#define vh_htf_flagsptr(ht, hf)	(&vh_htf_flagsval(ht, hf))
#define vh_htf_flags(ht, hf)	vh_htf_flagsval(ht, hf)

#define vh_htf_isconstructed(ht, hf)	(vh_htf_flags(ht, hf) & VH_HTF_FLAG_CONSTRUCTED)

#define vh_htf_isnull(ht, hf)		(vh_htf_flags(ht, hf) & VH_HTF_FLAG_NULL)
#define vh_htf_clearnull(ht, hf)	(vh_htf_flags(ht, hf) &= ~VH_HTF_FLAG_NULL)
#define vh_htf_setnull(ht, hf)		(vh_htf_flags(ht, hf) |= VH_HTF_FLAG_NULL)

#define vh_htf_ischanged(ht, hf)	(vh_htf_flags(ht, hf) & VH_HTF_FLAG_CHANGED)
#define vh_htf_clearchanged(ht, hf)	(vh_htf_flags(ht, hf) &= ~VH_HTF_FLAG_NULL)
#define vh_htf_setchanged(ht, hf)	(vh_htf_flags(ht, hf) |= VH_HTF_FLAG_CHANGED)

bool vh_ht_nullbitmap(HeapTuple ht, char *bitmap, size_t sz);
#define vh_ht_nbm_isnull(nbm, idx)	((nbm)[(idx)/8] & (1 << ((idx) % 8)))
#define vh_ht_nbm_setnull(nbm, idx)	((nbm)[(idx)/8] |= (1 << ((idx) % 8)))
#define vh_ht_nbm_clearnull(nbm, idx)	((nbm)[(idx)/8] &= ~(1 << ((idx) % 8)))

#define vh_ht_nbm_size(ht)			vh_htd_nbm_size((ht)->htd)

/*
 * We've some generic functions we use for indexing to fill a memory area with
 * data from a given HeapTuple.  This is most commonly used by the Indexing
 * infrastructure to form a key of a HeapTuple.
 */

size_t vh_ht_formkey(unsigned char *buffer, size_t buffer_sz,
		  			 HeapTuple ht, HeapField *hfs, uint32_t nhfs);

size_t vh_htf_tostring(HeapTuple ht, HeapField hf, 
					   char *buffer, size_t buffer_sz);

#endif

