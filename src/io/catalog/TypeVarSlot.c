/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarSlot.h"


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

/*
 * vh_tvs_finalize
 *
 * Slightly different from vh_tvs_reset in that we always do a finalize, even
 * if the reset flag has been set.  We also move the TypeVarSlot to an
 * initialized state, so it doesn't store anything.
 */
void
vh_tvs_finalize(TypeVarSlot *slot)
{
	switch (vh_tvs_flags(slot) & VH_TVS_RA_MASK)
	{
		case VH_TVS_RA_RVAR:
		case VH_TVS_RA_DVAR:
			/*
			 * We get to destroy the TypeVar, but we need to assert our access
			 * method.
			 */

			assert(vh_tvs_am(slot) & VH_TVS_AM_TYPEVAR);
			vh_typevar_finalize(slot->ptr);

			break;

		case VH_TVS_RA_UNPIN:
			/*
			 * Make sure we're only in a HeapTuplePtr access method.  We cannot
			 * unpin a HeapTuple.
			 */

			assert((vh_tvs_am(slot) & VH_TVS_AM_HTPM) ||
				   (vh_tvs_am(slot) & VH_TVS_AM_HTPI));

			break;
	}

	/*
	 * Initialize the slot to an empty state.
	 */
	vh_tvs_init(slot);
}


/*
 * vh_tvs_reset
 *
 * Resets the TypeVarSlot.
 */
void
vh_tvs_reset(TypeVarSlot *slot)
{
	switch (vh_tvs_flags(slot) & VH_TVS_RA_MASK)
	{
		case VH_TVS_RA_RVAR:
			/*
			 * We get to reset the TypeVar, but we need to assert our access
			 * method.
			 */

			assert(vh_tvs_am(slot) & VH_TVS_AM_TYPEVAR);
			vh_typevar_reset(slot->ptr);

			break;

		case VH_TVS_RA_DVAR:
			/*
			 * We get to destroy the TypeVar, but we need to assert our access
			 * method.
			 */

			assert(vh_tvs_am(slot) & VH_TVS_AM_TYPEVAR);
			vh_typevar_finalize(slot->ptr);

			break;

		case VH_TVS_RA_UNPIN:
			/*
			 * Make sure we're only in a HeapTuplePtr access method.  We cannot
			 * unpin a HeapTuple.
			 */

			assert((vh_tvs_am(slot) & VH_TVS_AM_HTPM) ||
				   (vh_tvs_am(slot) & VH_TVS_AM_HTPI));

			break;
	}

	/*
	 * Just clear our RA, we want to keep all the other details and they'll be
	 * setup properly if a store variant is called.  It's possible that
	 * vh_tvs_reset has been called by macro vh_tvs_store_null and we definitely
	 * want to keep the other details around in that case.
	 */

	vh_tvs_flags(slot) &= ~VH_TVS_RA_MASK;
}

/*
 * vh_tvs_copy
 *
 * Call the reset on the taget and copy everything but the reset action on the
 * source.  We left the source with the reset action.
 */
void
vh_tvs_copy(TypeVarSlot *target, TypeVarSlot *source)
{
	vh_tvs_reset(target);
	memcpy(target, source, sizeof(TypeVarSlot));

	/*
	 * Clear any reset flags on the target.
	 */
	vh_tvs_flags(target) = vh_tvs_flags(target) & ~VH_TVS_RA_MASK;
}

/*
 * vh_tvs_move
 *
 * All we do here is call the reset on the target and the copy everyhing over.
 *
 * The only real exception is that we'll undo any Reset Action on the source,
 * to ensure that we don't delete anything that got moved to the target.
 */
void
vh_tvs_move(TypeVarSlot *target, TypeVarSlot *source)
{
	vh_tvs_reset(target);
	memcpy(target, source, sizeof(TypeVarSlot));
	
	/*
	 * Clear any reset actions on the source
	 */
	vh_tvs_flags(source) = vh_tvs_flags(source) & ~VH_TVS_RA_MASK;
}

void
vh_tvs_store(TypeVarSlot *slot, Type *tys, void *data)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	vh_type_stack_2tags(slot->tags, tys);
	slot->ptr = data;

	vh_tvs_flags_set(slot,					/* Slot */
  					 VH_TVS_TM_TAGS,		/* Tag Method */
			   		 VH_TVS_AM_PTR,			/* Access Method */
	   				 0,						/* Reset Method */
   					 0,						/* Heap Access Method */
	   				 0);	  				/* Heap Access Value */

}

void
vh_tvs_store_ht_hf(TypeVarSlot *slot, HeapTuple ht, HeapField hf)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	vh_type_stack_2tags(slot->tags, hf->types);
	slot->ptr = ht;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_HT,			/* Access Method */
					 0,						/* Reset Method */
					 VH_TVS_HAM_FIDX,		/* Heap Accss Method */
					 hf->dord);				/* Heap Access Value */
}

void
vh_tvs_store_ht_idx(TypeVarSlot *slot, HeapTuple ht, uint16_t idx)
{
	HeapTupleDef htd;
	HeapField hf;

	if (slot->flags)
		vh_tvs_reset(slot);

	htd = ht->htd;
	hf = vh_htd_field_by_idx(htd, idx);

	if (hf)
	{
		vh_type_stack_2tags(slot->tags, hf->types);
		slot->ptr = ht;

		vh_tvs_flags_set(slot,					/* Slot */
						 VH_TVS_TM_TAGS,		/* Tag Method */
						 VH_TVS_AM_HT,			/* Access Method */
						 0,						/* Reset Method */
						 VH_TVS_HAM_FIDX,		/* Heap Accss Method */
						 hf->dord);				/* Heap Access Value */
	}
}

void
vh_tvs_store_htp_hf(TypeVarSlot *slot, HeapTuplePtr htp, HeapField hf)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	vh_type_stack_2tags(slot->tags, hf->types);
	slot->htp = htp;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_HTPM,		/* Access Method */
					 0,						/* Reset Method */
					 VH_TVS_HAM_FIDX,		/* Heap Accss Method */
					 hf->dord);				/* Heap Access Value */
}

void
vh_tvs_store_htp_name(TypeVarSlot *slot, HeapTuplePtr htp, const char *name)
{
	HeapTupleDef htd;
	HeapTuple ht;
	HeapField hf;

	if (slot->flags)
		vh_tvs_reset(slot);

	ht = vh_htp(htp);
	htd = ht->htd;
	hf = (HeapField)vh_tdv_tf_name((TableDefVer)htd, name);
	//vh_rhtp(htp);

	if (hf)
	{
		vh_type_stack_2tags(slot->tags, hf->types);
		slot->ptr = ht;

		vh_tvs_flags_set(slot,					/* Slot */
						 VH_TVS_TM_TAGS,		/* Tag Method */
						 VH_TVS_AM_HT,			/* Access Method */
						 0,						/* Reset Method */
						 VH_TVS_HAM_FIDX,		/* Heap Accss Method */
						 hf->dord);				/* Heap Access Value */
	}
}

void
vh_tvs_store_var(TypeVarSlot *slot, void *typevar, int32_t flags)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->ptr = typevar;

	vh_typevar_fill_tags(typevar, slot->tags);

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_NOTAGS,		/* Tag Method */
					 VH_TVS_AM_TYPEVAR,		/* Access Method */
					 flags,					/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */
}

void
vh_tvs_store_i8(TypeVarSlot *slot, int8_t val)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->i8 = val;
	slot->tags[0] = vh_type_int8.id;
	slot->tags[1] = 0;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_BYVAL,		/* Access Method */
					 0,						/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */
}

void
vh_tvs_store_bool(TypeVarSlot *slot, bool val)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->i8 = val ? true : false;
	slot->tags[0] = vh_type_bool.id;
	slot->tags[1] = 0;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_BYVAL,		/* Access Method */
					 0,						/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */
}

void
vh_tvs_store_i16(TypeVarSlot *slot, int16_t val)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->i16 = val;
	slot->tags[0] = vh_type_int16.id;
	slot->tags[1] = 0;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_BYVAL,		/* Access Method */
					 0,						/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */
}

void
vh_tvs_store_i32(TypeVarSlot *slot, int32_t val)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->i32 = val;
	slot->tags[0] = vh_type_int32.id;
	slot->tags[1] = 0;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_BYVAL,		/* Access Method */
					 0,						/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */
}

void
vh_tvs_store_i64(TypeVarSlot *slot, int64_t val)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->i64 = val;
	slot->tags[0] = vh_type_int64.id;
	slot->tags[1] = 0;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_BYVAL,		/* Access Method */
					 0,						/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */
}

void
vh_tvs_store_float(TypeVarSlot *slot, float val)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->flt = val;
	slot->tags[0] = vh_type_float.id;
	slot->tags[1] = 0;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_BYVAL,		/* Access Method */
					 0,						/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */
}

void
vh_tvs_store_double(TypeVarSlot *slot, double val)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->dbl = val;
	slot->tags[0] = vh_type_dbl.id;
	slot->tags[1] = 0;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_BYVAL,		/* Access Method */
					 0,						/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */
}

void
vh_tvs_store_String(TypeVarSlot *slot, String val)
{
	if (slot->flags)
		vh_tvs_reset(slot);

	slot->ptr = val;
	slot->tags[0] = vh_type_String.id;
	slot->tags[1] = 0;

	vh_tvs_flags_set(slot,					/* Slot */
					 VH_TVS_TM_TAGS,		/* Tag Method */
					 VH_TVS_AM_PTR,			/* Access Method */
					 0,						/* Reset Method */
					 0,						/* Heap Access Method */
					 0);					/* Heap Access Value */

}

bool
vh_tvs_i16(TypeVarSlot *slot, int16_t *val)
{
	TypeVarSlot tvs = { };

	if (slot->flags & VH_TVS_TM_TAGS)
	{
		if (slot->tags[0] == vh_type_int16.id)
		{
			*val = *((int16_t*)vh_tvs_value(slot));
			return true;
		}
	}

	/*
	 * We've got to perform the operation.
	 */
	vh_tvs_store_i16(&tvs, 0);
	vh_typevar_op("=",
				  VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
					  			  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID,
								  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID),
				  &tvs,
				  slot);

	return true;
}

bool
vh_tvs_i32(TypeVarSlot *slot, int32_t *val)
{
	TypeVarSlot tvs = { };

	if (slot->flags & VH_TVS_TM_TAGS)
	{
		if (slot->tags[0] == vh_type_int32.id)
		{
			*val = *((int32_t*)vh_tvs_value(slot));
			return true;
		}
	}

	/*
	 * We've got to perform the operation.
	 */
	vh_tvs_store_i32(&tvs, 0);
	vh_typevar_op("=",
				  VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
					  			  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID,
								  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID),
				  &tvs,
				  slot);

	return true;
}

bool
vh_tvs_i64(TypeVarSlot *slot, int64_t *val)
{
	TypeVarSlot tvs = { };

	if (slot->flags & VH_TVS_TM_TAGS)
	{
		if (slot->tags[0] == vh_type_int64.id)
		{
			*val = *((int64_t*)vh_tvs_value(slot));
			return true;
		}
	}

	/*
	 * We've got to perform the operation.
	 */
	vh_tvs_store_i64(&tvs, 0);
	vh_typevar_op("=",
				  VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
					  			  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID,
								  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID),
				  &tvs,
				  slot);

	return true;
}

bool
vh_tvs_float(TypeVarSlot *slot, float *val)
{
	TypeVarSlot tvs = { };
	
	if (slot->flags & VH_TVS_TM_TAGS)
	{
		if (slot->tags[0] == vh_type_float.id)
		{
			*val = *((float*)vh_tvs_value(slot));
			return true;
		}
	}

	/*
	 * We've got to perform the operation.
	 */
	vh_tvs_store_float(&tvs, 0);
	vh_typevar_op("=",
				  VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
					  			  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID,
								  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID),
				  &tvs,
				  slot);

	return true;
}

bool
vh_tvs_double(TypeVarSlot *slot, double *val)
{
	TypeVarSlot tvs = { };
	
	if (slot->flags & VH_TVS_TM_TAGS)
	{
		if (slot->tags[0] == vh_type_dbl.id)
		{
			*val = *((double*)vh_tvs_value(slot));
			return true;
		}
	}

	/*
	 * We've got to perform the operation.
	 */
	vh_tvs_store_double(&tvs, 0);
	vh_typevar_op("=",
				  VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
					  			  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID,
								  VH_OP_DT_TVS,
								  VH_OP_ID_INVALID),
				  &tvs,
				  slot);

	return true;
}


/*
 * vh_tvs_value_ext
 *
 * We should only get here with a HeapTuple Access Method, so assert that.
 */
void*
vh_tvs_value_ext(TypeVarSlot *slot)
{
	HeapField hf;
	HeapTuple ht = 0;
	HeapTupleDef htd = 0;
	unsigned char *cur = 0;
	void *val = 0;

	assert(vh_tvs_am(slot) & VH_TVS_AM_HT_MASK);

	switch (vh_tvs_am(slot))
	{
		case VH_TVS_AM_HT:
			ht = slot->ptr;
			break;

		case VH_TVS_AM_HTPI:

			/*
			 * Be sure to set the RA_UNPIN flag.
			 */
			vh_tvs_flags(slot) |= VH_TVS_RA_UNPIN;
			break;

		case VH_TVS_AM_HTPM:
			
			/*
			 * Be sure to set the RA_UNPIN flag.
			 */
			vh_tvs_flags(slot) |= VH_TVS_RA_UNPIN;
			ht = vh_htp(slot->htp);
			break;

		default:
			elog(WARNING,
					emsg("Unexpected TypeVarSlot Access Method [%d].  Unable to "
						 "obtain the data value for the slot at [%p] with the "
						 "flags [%d]",
						 vh_tvs_am(slot),
						 slot,
						 vh_tvs_flags(slot)));

			return 0;
	}

	if (ht)
	{
		htd = ht->htd;

		switch(vh_tvs_ham(slot))
		{
			case VH_TVS_HAM_FIDX:
				hf = vh_htd_field_by_idx(htd, vh_tvs_ham_value(slot));
				val = vh_ht_field(ht, hf);

				return val;

			case VH_TVS_HAM_OFF:
				cur = (unsigned char*)ht;
				cur += vh_tvs_ham_value(slot);

				return cur;

			default:
				elog(WARNING,
						emsg("Unexpected TypeVarSlot Heap Access Method [%d].  "
							 "Unable to obtain the data value for the slot at "
							 "[%p] with the flags [%d].",
							 vh_tvs_ham(slot),
							 slot,
							 vh_tvs_flags(slot)));
		}
	}

	return 0;
}

/*
 * vh_tvs_fill_tys
 *
 * Fills a TypeStack from a slot.  There's two scenarios we could wind up in
 * depending on how the TypeVarSlot was "stored".  If we're coming from a TypeVar
 * we won't waste time copying the tags over, so we'll just go directly to the
 * TypeVar from here to get the stack.
 *
 * Otherwise, we'll need to lookup the tags on the @slot in the type catalog.
 *
 * Returns the depth or 0 if there was an error.
 */
int8_t
vh_tvs_fill_tys(TypeVarSlot *slot, Type *tys)
{
	int8_t i;
	TypeTag tag;

	if (slot->flags & VH_TVS_TM_TAGS)
	{
		for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
		{
			tag = slot->tags[i];

			if (!tag)
			{
				tys[i] = 0;
				break;
			}

			tys[i] = vh_type_tag(tag);

			if (!tys[i])
			{
				elog(WARNING,
						emsg("TypeTag %hu could not be resolved from the Type Catalog.  "
							 "The TypeVarSlot at [%p] may be corrupt.",
							 tag,
							 slot));

				return 0;
			}
		}

		return i;
	}

	/*
	 * Make sure we're a TypeVar, as that's the only way we can get here
	 * without TypeTags if the caller is using the full TypeVarSlot
	 * interface.
	 */

	assert(slot->flags & VH_TVS_AM_TYPEVAR);

	i = vh_typevar_fill_stack(slot->ptr, tys);

	return i;
}

int32_t
vh_tvs_compare(const TypeVarSlot *lhs, const TypeVarSlot *rhs)
{
	int32_t comp;

	comp = vh_typevar_comp_impl(VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
												VH_OP_DT_TVS,
												VH_OP_ID_INVALID,
												VH_OP_DT_TVS,
												VH_OP_ID_INVALID),
								lhs, rhs);

	return comp;
}

