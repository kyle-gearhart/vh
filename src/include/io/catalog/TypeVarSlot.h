/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_TypeVarSlot_H
#define vh_catalog_TypeVarSlot_H

#include "io/catalog/Type.h"

/*
 * TypeVarSlot
 *
 */



#define vh_tvs_flags(tvs)				((tvs)->flags)

struct TypeVarSlot
{
	int32_t flags;
	TypeTag tags[VH_TAMS_MAX_DEPTH];
	
	union
	{
		void *ptr;
		HeapTuplePtr htp;
		
		char i8;
		unsigned char ui8;
		
		short i16;
		unsigned short ui16;
		
		int32_t i32;
		uint32_t ui32;
		
		int64_t i64;
		uint64_t ui64;

		float flt;
		double dbl;
	};	
};


/*
 * We have two ways a TypeVarSlot can store it's type information and we call this 
 * the Tag Method (TM).  The Tag Method is tracked in the flags member.  The 
 * lowest 4 bits are used to indentify the storage method.  Only one storage 
 * method may be used at a time!
 *
 * 		1)	With tags
 * 			VH_TVS_TM_TAGS
 * 		2)	Without tags
 * 			VH_TVS_TM_NOTAGS
 *
 * Since TVS are usually used as a wrapper for the TypeVar subsystem, we do
 * not copy the tags over to the TVS is the we're in a no tags storage method.
 * The TypeVar infrastructure has highly optimized code for extracing the tags
 * from a TypeVar, so there's no sense wasting time trying to copy data we're
 * not going to use.  So we use the TM_NOTAGS option to indicate that you've
 * got to go thru the Access Method to get the Tags.
 */

#define VH_TVS_TM_MASK					0xf0000000
#define VH_TVS_TM_TAGS					0x10000000
#define VH_TVS_TM_NOTAGS				0x20000000

#define vh_tvs_tm(tvs)					(vh_tvs_flags(tvs) & VH_TVS_TM_MASK)

/*
 * The next 6 bits (moving left) indicate the Access Method (AM).  There are
 * 5 Access Methods available:
 * 		1)	HeapTuplePtr (mutable)
 * 			VH_TVS_AM_HTPM
 * 		2)	HeapTuplePtr (immutable)
 * 			VH_TVS_AM_HTPI
 * 		3)	HeapTuple
 * 			VH_TVS_AM_HT
 * 		4)	By value
 * 			VH_TVS_AM_BYVAL
 * 		5)	TypeVar
 * 			VH_TVS_AM_TYPEVAR
 */

#define VH_TVS_AM_MASK					0x0fc00000

#define VH_TVS_AM_HT_MASK				0x0e000000
#define VH_TVS_AM_HTPM					0x08000000
#define VH_TVS_AM_HTPI					0x04000000
#define VH_TVS_AM_HT					0x02000000

#define VH_TVS_AM_PTR					0x01000000
#define VH_TVS_AM_BYVAL					0x00800000
#define VH_TVS_AM_TYPEVAR				0x00400000

#define vh_tvs_am(tvs)					(vh_tvs_flags(tvs) & VH_TVS_AM_MASK)
#define vh_tvs_am_ht(tvs)				(vh_tvs_flags(tvs) & VH_TVS_AM_HT_MASK)
#define vh_tvs_am_htp(tvs)				(vh_tvs_flags(tvs) & (0x0c000000))

/*
 * Null flag goes here
 */
#define VH_TVS_NULL						0x00200000

#define vh_tvs_isnull(tvs)				(vh_tvs_flags(tvs) & VH_TVS_NULL ? 1 : 0)
#define vh_tvs_setnull(tvs)				(vh_tvs_flags(tvs) |= VH_TVS_NULL)
#define vh_tvs_clearnull(tvs)			(vh_tvs_flags(tvs) &= ~VH_TVS_NULL)

/*
 * There are several Reset Actions (RA) that may need to occur.
 * 		1)	Reset var
 * 			VH_TVS_RA_RVAR
 * 		2)	Destroy var
 * 			VH_TVS_RA_DVAR
 * 		3)	Unpin HeapTuple
 * 			VH_TVS_RA_UNPIN
 */

#define VH_TVS_RA_MASK					0x001c0000
#define VH_TVS_RA_RVAR					0x00100000
#define VH_TVS_RA_DVAR					0x00080000
#define VH_TVS_RA_UNPIN					0x00040000

#define vh_tvs_ra(tvs)					(vh_tvs_flags(tvs) & VH_TVS_RA_MASK)

/*
 * When a HeapTuple or HeapTuplePtr Access Method (HAM) is used, we store either a 
 * field index when nulls may be used for the field or a offset amount from the 
 * start of the HeapTuple.
 * 		1)	Offset (bytes)
 * 			VH_TVS_HAM_OFF
 * 		2)	Field index
 * 			VH_TVS_HAM_FIDX
 *
 */

#define VH_TVS_HAM_MASK					0x00030000
#define VH_TVS_HAM_OFF					0x00010000
#define VH_TVS_HAM_FIDX					0x00020000

#define VH_TVS_HAM_VMASK				0x0000ffff

#define vh_tvs_ham(tvs)					(vh_tvs_flags(tvs) & VH_TVS_HAM_MASK)
#define vh_tvs_ham_value(tvs)			((uint16_t)(vh_tvs_flags(tvs) & VH_TVS_HAM_VMASK))




#define vh_tvs_flags_set(tvs, 													\
						 tm, 													\
						 am, 													\
						 ra,													\
						 ham, 													\
						 hamv) 													\
		(vh_tvs_flags(tvs) = ((tm) & VH_TVS_TM_MASK)			|				\
							 ((am) & VH_TVS_AM_MASK) 			| 				\
		 					 ((ra) & VH_TVS_RA_MASK) 			|				\
							 ((ham) & VH_TVS_HAM_MASK)	 		|				\
		 					 ((hamv) & VH_TVS_HAM_VMASK))

#define vh_tvs_init(tvs)		(vh_tvs_flags(tvs) = 0)


void vh_tvs_reset(TypeVarSlot *slot);
void vh_tvs_finalize(TypeVarSlot *slot);

void vh_tvs_copy(TypeVarSlot *target, TypeVarSlot *source);
void vh_tvs_move(TypeVarSlot *target, TypeVarSlot *source);


void vh_tvs_store(TypeVarSlot *slot, Type *tys, void *data);
#define vh_tvs_store_null(tvs)		(vh_tvs_reset(tvs), vh_tvs_setnull(tvs))

/* HeapTuple */
void vh_tvs_store_ht_hf(TypeVarSlot *slot, HeapTuple ht, HeapField hf);
void vh_tvs_store_ht_idx(TypeVarSlot *slot, HeapTuple ht, uint16_t idx);

/* HeapTuplePtr */
void vh_tvs_store_htp_name(TypeVarSlot *slot, HeapTuplePtr htp, const char *name);
void vh_tvs_store_htp_hf(TypeVarSlot *slot, HeapTuplePtr htp, HeapField hf);
void vh_tvs_store_htp_idx(TypeVarSlot *slot, HeapTuplePtr htp, uint16_t idx);

/* TypeVar */
void vh_tvs_store_var(TypeVarSlot *slot, void *typevar, int32_t flags);

/* By Value */
void vh_tvs_store_bool(TypeVarSlot *slot, bool val);
void vh_tvs_store_i8(TypeVarSlot *slot, int8_t val);
void vh_tvs_store_i16(TypeVarSlot *slot, int16_t val);
void vh_tvs_store_i32(TypeVarSlot *slot, int32_t val);
void vh_tvs_store_i64(TypeVarSlot *slot, int64_t val);

void vh_tvs_store_float(TypeVarSlot *slot, float val);
void vh_tvs_store_double(TypeVarSlot *slot, double val);
void vh_tvs_store_String(TypeVarSlot *slot, String val);

bool vh_tvs_i16(TypeVarSlot *slot, int16_t *val);
bool vh_tvs_i32(TypeVarSlot *slot, int32_t *val);
bool vh_tvs_i64(TypeVarSlot *slot, int64_t *val);
bool vh_tvs_float(TypeVarSlot *slot, float *val);
bool vh_tvs_double(TypeVarSlot *slot, double *val);


/*
 * vh_tvs_value
 *
 * Return a pointer to the data, even if the TypeVarSlot is by value.
 *
 * We try to do this inline by checking for the following Access Methods before
 * dropping out to function:
 * 		VH_TVS_AM_TYPEVAR || VH_TVS_AM_PTR
 * 		VH_TVS_AM_BYVAL
 */

void* vh_tvs_value_ext(TypeVarSlot *slot);

#define vh_tvs_value(tvs)	(													\
		((vh_tvs_am(tvs) & VH_TVS_AM_TYPEVAR) 	|| 								\
		 (vh_tvs_am(tvs) & VH_TVS_AM_PTR) ? (tvs)->ptr :						\
		 	((vh_tvs_am(tvs) & VH_TVS_AM_BYVAL) ? (void*)(&((tvs)->ptr)) :		\
			 	vh_tvs_value_ext(tvs)))											\
		)

int8_t vh_tvs_fill_tys(TypeVarSlot *slot, Type *tys);

int32_t vh_tvs_compare(const TypeVarSlot *lhs, const TypeVarSlot *rhs);

#endif 

