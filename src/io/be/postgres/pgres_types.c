/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/be/postgres/Impl.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/Type.h"
#include "io/catalog/types/Array.h"
#include "io/catalog/types/Date.h"
#include "io/catalog/types/DateTime.h"
#include "io/catalog/types/Range.h"


#define POSTGRES_EPOCH_JDATE		2451545
#define UNIX_EPOCH_JDATE			2440588


static void* pgres_ty_array_tam_get(struct TamBinGetStack *tamstack, 
									const BinaryAMOptions bopts,
									const void *source, void *target,
									size_t *length, size_t *cursor);
static void* pgres_ty_array_tam_set(struct TamBinSetStack *tamstack, 
		 							const BinaryAMOptions bopts,
		 							const void *source, void *target,
			 						size_t length, size_t cursor);

static void* pgres_ty_date_tam_get(struct TamBinGetStack *tamstack, 
								   const BinaryAMOptions bopts,
		 						   const void *source, void *target,
		 						   size_t *length, size_t *cursor);
static void* pgres_ty_date_tam_set(struct TamBinSetStack *tamstack, 
		 						   const BinaryAMOptions bopts,
		 						   const void *source, void *target,
		 						   size_t length, size_t cursor);

static void* pgres_ty_datetime_tam_get(struct TamBinGetStack *tamstack, 
									   const BinaryAMOptions bopts,
									   const void *source, void *target,
									   size_t *length, size_t *cursor);
static void* pgres_ty_datetime_tam_set(struct TamBinSetStack *tamstack, 
									   const BinaryAMOptions bopts,
									   const void *source, void *target,
									   size_t length, size_t cursor);

static void* pgres_ty_range_tam_get(struct TamBinGetStack *tamstack, 
									const BinaryAMOptions bopts,
		 							const void *source, void *target,
		 							size_t *length, size_t *cursor);
static void* pgres_ty_range_tam_set(struct TamBinSetStack *tamstack, 
		 							const BinaryAMOptions bopts,
		 							const void *source, void *target,
		 							size_t length, size_t cursor);

/*
 * TAM Override Registration for:
 * 	>	Array
 * 	>	Date
 * 	>	DateTime
 * 	>	Range
 */
void
vh_pgres_ty_array_register(BackEnd be)
{
	struct TypeAMFuncs tam_funcs = { };

	tam_funcs.bin_get = pgres_ty_array_tam_get;
	tam_funcs.bin_set = pgres_ty_array_tam_set;

	vh_be_type_setam(be, &vh_type_Array, tam_funcs);
}

void
vh_pgres_ty_date_register(BackEnd be)
{
	struct TypeAMFuncs tam_funcs = { };

	tam_funcs.bin_get = pgres_ty_date_tam_get;
	tam_funcs.bin_set = pgres_ty_date_tam_set;

	vh_be_type_setam(be, &vh_type_Date, tam_funcs);
}

void
vh_pgres_ty_datetime_register(BackEnd be)
{
	struct TypeAMFuncs tam_funcs = { };
	
	tam_funcs.bin_get = pgres_ty_datetime_tam_get;
	tam_funcs.bin_set = pgres_ty_datetime_tam_set;

	vh_be_type_setam(be, &vh_type_DateTime, tam_funcs);
}

void
vh_pgres_ty_range_register(BackEnd be)
{
	struct TypeAMFuncs tam_funcs = { };

	tam_funcs.bin_get = pgres_ty_range_tam_get;
	tam_funcs.bin_set = pgres_ty_range_tam_set;

	vh_be_type_setam(be, &vh_type_Range, tam_funcs);
}

/*
 * Postgres Array Types
 *
 * Outbound wire format for Postgres:
 * 	>	int32_t		Number of dimensions
 * 	>	int32_t		Set to 1 if contains nulls
 * 	>	Oid			underlying type OID
 * 
 * For each dimension, Postgres expects the following:
 * 	>	int32_t		Number of items
 * 	>	int32_t		Lower boundary	(always 0)
 *
 * For each element, Postgres expects teh following:
 * >	int32_t		Word length (-1 indicates nulls)
 * >	char*		Element binary data
 *
 * To put the elements on the wire, we simply drop to the lowest level
 * array which contains non-array Inner Type and then iterate the data set.
 */
static void* 
pgres_ty_array_tam_get(struct TamBinGetStack *tamstack, 
					   const BinaryAMOptions bopts,
					   const void *source, void *target,
					   size_t *length, size_t *cursor)
{
	
	const struct ArrayData *array[VH_TAMS_MAX_DEPTH], *array_i[VH_TAMS_MAX_DEPTH];
	Type ty_inner;
	int32_t array_idx[VH_TAMS_MAX_DEPTH];
	int32_t array_nelems[VH_TAMS_MAX_DEPTH];
	int32_t ndimensions, nelems;
	unsigned char *buffer, *buffer_end, *buffer_cursor;
	size_t required_len = 0, cart_nelems, buffer_len;
	uint8_t ndimensions_i = 1;
	bool increment;

	if (length)
		buffer_len = *length;

	/*
	 * Build up the arrays we're going to scan across by using the TamStack.
	 */
	array[0] = source;
	array_i[0] = array[0];
	array_idx[0] = 0;
	cart_nelems = vh_ty_array_nelems(array[0]);
	ty_inner = tamstack->types[array[0]->ndimensions - 1];
	ndimensions = array[0]->ndimensions;

	while (ndimensions_i < array[0]->ndimensions)
	{
		if (tamstack->types[ndimensions_i - 1] == &vh_type_Array)
		{
			array[ndimensions_i] = array[ndimensions_i - 1]->buffer;
			array_nelems[ndimensions_i] = vh_ty_array_nelems(array[ndimensions_i]);
			array_idx[ndimensions_i] = 0;
			cart_nelems *= array_nelems[ndimensions_i];
		}

		ndimensions_i++;
	}

	/*
	 * We need to iterate each array to find out how many child elements actually
	 * exist.
	 */

	do
	{
		/*
		 * Walk down the dimensions until we reach the lowest dimension.  Then we
		 * can gather the number of elements.  We work this from left to right.
		 *
		 * Later we'll advance the array_idx by one for each dimension.
		 */

		/*
		 * Reset the lowest level idx
		 */
		array_idx[ndimensions - 1] = 0;

		ndimensions_i = 1;
		while (ndimensions_i < ndimensions)
		{
			if (array_idx[ndimensions_i] < array_nelems[ndimensions_i])
			{
				array_i[ndimensions_i] = vh_ty_array_elemat(array[ndimensions_i],
															array_idx[ndimensions_i]);
				ndimensions_i++;
			}
		}

		/*
		 * Check to see if we actually made it to the bottom.
		 */
		assert(array_i[ndimensions - 1]->ty_inner != &vh_type_Array);
		nelems = vh_ty_array_nelems(array_i[ndimensions - 1]);
		
		/*
		 * We've walked all the way down the array to grab widths and number
		 * of elements from.
		 */

		if (ty_inner->varlen)
		{
			//required_len += vh_ty_array_elemwidth(array_i[ndimensions - 1]);
		}
		else
		{
			required_len += nelems *
						  	array_i[ndimensions - 1]->ty_inner->size;
		}

		/*
		 * We've iterated everything at the lowest level so advance the 
		 * array_idx to say so.  This triggers the abort scenario in the event
		 * there's only one dimension.
		 */
		array_idx[ndimensions - 1] = array_nelems[ndimensions - 1];

		ndimensions_i--;
		increment = true;
		while (ndimensions_i > 0)
		{
			if (increment)
				array_idx[ndimensions_i]++;

			if (array_idx[ndimensions_i] == array_nelems[ndimensions_i])
				increment = true;
			else
				increment = false;

			ndimensions_i--;
		}

		if (array_idx[0] >= array_nelems[0])
			break;

	} while (true);


	/*
	 * Now we have a calculated length but we haven't added any wordlengths.
	 * This is simple, just multiply sizeof(int32_t) by cart_nelems.
	 *
	 * Then we add in the ndimension information plus the top level array
	 * header and we've got a good set of lengths.
	 */
	
	required_len += sizeof(int32_t) * cart_nelems;	/* length words */
	required_len += sizeof(int32_t) * 2 * ndimensions;	/* dimensions */
	required_len += sizeof(int32_t) * 3;			/* header chunk */
	
	/*
	 * Set our length pointer, we now know the exact number of bytes it's
	 * going to send this Array (potentially array of arrays) across the
	 * wire.
	 */

	*length = required_len;

	/*
	 * Check to see if we just came here for the length
	 */
	if (!bopts->malloc && buffer_len == 0)
	{
		return 0;
	}

	/*
	 * Pipe this mess out on the wire.  We've got two options: we can malloc
	 * this thing up to the length specifid by size or we can just fill the
	 * cursor thru.  By this point we've got a lot of meta data about the
	 * dataset thanks to the exhaustive length calculation.
	 *
	 * When we're using a cursor, we'll want to put entire 8 byte boundaries
	 * on the wire to make it easier to re-enter.  We'll simply keep a counter
	 * of the number of bytes processed.  When that amount equals or exceeds
	 * the cursor then we can start filling target again.  If the caller forces
	 * us to re-enter it should be smart enough to send us a buffer big enough
	 * to accomodate the remaining bytes.
	 */

	if (bopts->malloc)
	{
		if (buffer_len < required_len)
		{
			elog(ERROR2,
				 emsg("Insufficient length provided to the Postgres TAM routine "
					  "for Array types!  The source data type requires %d bytes "
					  "to serialize for Postgres but only %d were given!",
					  required_len,
					  buffer_len));
		}

		buffer = vhmalloc(buffer_len == 0 ? required_len : buffer_len);
		buffer_cursor = buffer;
		buffer_end = buffer_cursor + (buffer_len == 0 ? required_len : buffer_len);
	}
	else
	{
		buffer = target;
		buffer_cursor = buffer;
		buffer_end = buffer_cursor + buffer_len;
	}

	while (buffer_cursor < buffer_end)
	{
	}

	return 0;
}

static void* 
pgres_ty_array_tam_set(struct TamBinSetStack *tamstack, 
					   const BinaryAMOptions bopts,
					   const void *source, void *target,
					   size_t length, size_t cursor)
{
	return 0;
}


/*
 * pgres_ty_date_tam_get
 *
 * We just need to apply the Postgres EPOCH for dates which is January 1,
 * 2000.
 */
static void* 
pgres_ty_date_tam_get(struct TamBinGetStack *tamstack, 
					  const BinaryAMOptions bopts,
					  const void *source, void *target,
					  size_t *length, size_t *cursor)
{
	Date jd = *((const Date*)source);

	jd -=  POSTGRES_EPOCH_JDATE;

	target = vh_type_int32.tam.bin_get(tamstack, bopts, &jd, target, length, cursor);

	if (length)
		assert(*length == sizeof(int32_t));

	return target;
}

/*
 * pgres_ty_date_tam_set
 *
 * Postgres is going to send us back a julian date with an epoch of January 1,
 * 2000.  Just remove the epoch.
 */
static void*
pgres_ty_date_tam_set(struct TamBinSetStack *tamstack, 
		 			  const BinaryAMOptions bopts,
		 			  const void *source, void *target,
	 				  size_t length, size_t cursor)
{
	Date *jd = target;

	vh_type_int32.tam.bin_set(tamstack, bopts, source, jd, length, cursor);

	if (*jd < 0)
		*jd -= POSTGRES_EPOCH_JDATE;
	else
		*jd += POSTGRES_EPOCH_JDATE;

	return 0;
}

static void* 
pgres_ty_datetime_tam_get(struct TamBinGetStack *tamstack, 
						  const BinaryAMOptions bopts,
						  const void *source, void *target,
						  size_t *length, size_t *cursor)
{
	DateTime dt = *((const DateTime*)source);

	dt -= (POSTGRES_EPOCH_JDATE * USECS_PER_DAY);

	target = vh_type_int64.tam.bin_get(tamstack, bopts, &dt, target, length, cursor);

	if (length)
		assert(*length == sizeof(int64_t));

	return target;
}

static void* 
pgres_ty_datetime_tam_set(struct TamBinSetStack *tamstack, 
						  const BinaryAMOptions bopts,
						  const void *source, void *target,
						  size_t length, size_t cursor)
{
	DateTime *dt = target;

	vh_type_int64.tam.bin_set(tamstack, bopts, source, dt, length, cursor);

	if (*dt < 0)
		*dt -= POSTGRES_EPOCH_JDATE * USECS_PER_DAY;
	else
		*dt += POSTGRES_EPOCH_JDATE * USECS_PER_DAY;

	return 0;
}

/*
 * Postgres Range Definitions
 *
 * In VH.IO, we've defined a range as a fixed variable length so long as the
 * inner type is not of variable length.  Thus the layout in memory is going
 * to look something like this:
 * 		UPPER value (size of ty_inner->size)
 * 		LOWER value (size of ty_inner->size)
 * 		FLAGS value (1 byte)
 *
 * If the ty_inner is a variable length type, then the upper and lower values
 * are pointers.  The flags value remains 1 bytes in size and is at the tail
 * end of the data set.
 *
 * Postgres sends the one byte flag first, plus a network ordered length word
 * (int32_t) followed by the inner type's binary format for Postgres.
 *
 * Only non-infinity boundaries are put in the buffer.  Thus if both boundaries
 * are infinity, then the length of the buffer is 1.  Otherwise the length of
 * buffer is calcalated as:
 * 	>	1 byte flags
 * 	>	4 bytes plus inner type size per boundary
 *
 * 	If the inner type is 4 bytes in size and both boundaries aren't infinity, 
 * 	then the total size of the buffer sent to Postgres should be:
 * 		1 byte flags + 
 * 		4 byte length word + 4 byte inner type +		(LOWER)
 * 		4 byte length word + 4 byte inner type			(UPPER)
 * 		========================================
 * 		13 bytes
 *
 * 	If the inner type is 4 bytes in size and the lower boundary is infinity,
 * 	then the total size of the buffer sent to Postgres should be:
 * 		1 byte flags +
 * 		4 byte length word + 4 byte inner type 			(UPPER)
 * 		========================================
 * 		9 bytes
 *
 * 	Variable length inner types have similar properties, however the legnth
 * 	word plays a much more critical role.
 */

static void* 
pgres_ty_range_tam_get(struct TamBinGetStack *tamstack, 
					   const BinaryAMOptions bopts,
					   const void *source, void *target,
					   size_t *length, size_t *cursor)
{
	const vh_tam_bin_get wordlen_get = vh_type_int32.tam.bin_get;
	
	struct BinaryAMOptionData mod_bopts = { 
		.malloc = false, 
		.sourceBigEndian = bopts->sourceBigEndian,
		.targetBigEndian = bopts->targetBigEndian };

	size_t boundary_lower_len = 0, boundary_upper_len = 0,
		   required_len = 1, buffer_len = 0, boundary_cursor = 0,
		   boundary_moved_by = 0, boundary_lower_offset = 0,
		   boundary_member_sz;
	int32_t wordlen_lower = 0, wordlen_upper = 0, *wordlen_cursor;
	unsigned char *i = (unsigned char*)source, *lower_boundary, *upper_boundary;
	uint8_t flags;
	unsigned char *buffer = 0, *buffer_cursor, *buffer_end;

	vh_tam_bin_get_check(bopts, length, cursor);


	/*
	 * Setup some pointers for our lower and upper boundaries.  Dereference the
	 * flags into a local variable.
	 */

	boundary_member_sz = vh_ty_range_boundary_member_sz(tamstack->types);
	
	lower_boundary = i;
	upper_boundary = i + boundary_member_sz;
	flags = *(i + (boundary_member_sz * 2));

	/* 
	 * We know this thing is going to be atleast one byte long for the flags
	 * alone.  We use vh_tams_length_from_get to use the binary get TAM to
	 * grab a length for the remaining TAM stack.  If the bounder isn't set
	 * to infinity, take the length and add it to the required_len plus the
	 * size of the Postgres length word (int32_t) and wordlen_lower and 
	 * wordlen_upper.
	 *
	 * Go ahead and convert the wordlens to the on the wire format Postgres
	 * expects.  We use vh_tam_firee_bin_get to fire with an empty TAM stack.
	 */
	buffer_len = sizeof(int32_t);
	if (!(flags & VH_TY_RANGE_LInfinity))
	{
		boundary_lower_len = vh_tams_length_from_get(tamstack->types,
													 (TamGetUnion*)tamstack->funcs,
													 TAM_Binary, lower_boundary);
		vh_tam_firee_bin_get(wordlen_get, &mod_bopts, 
							 &boundary_lower_len, &wordlen_lower,
							 &buffer_len, 0);
		assert(buffer_len == sizeof(int32_t));

		required_len += boundary_lower_len + sizeof(int32_t);
	}
	if (!(flags & VH_TY_RANGE_UInfinity))
	{
		boundary_upper_len = vh_tams_length_from_get(tamstack->types,
													 (TamGetUnion*)tamstack->funcs,
													 TAM_Binary, upper_boundary);
		vh_tam_firee_bin_get(wordlen_get, &mod_bopts,
							 &boundary_upper_len, &wordlen_upper,
							 &buffer_len, 0);
		assert(buffer_len == sizeof(int32_t));

		required_len += boundary_upper_len + sizeof(int32_t);
	}

	/*
	 * Check to see if we just came here for the length
	 */
	if (!bopts->malloc && length && *length == 0)
	{
		*length = required_len;
		return 0;
	}

	if (bopts->malloc)
	{
		if (length && *length > 0 && required_len > *length)
		{
			elog(ERROR2,
				 emsg("Insufficient space specified.  Requires %d bytes but "
					  "only %d are allowed!",
					  required_len,
					  *length));
		}

		buffer = buffer_cursor = vhmalloc(required_len);
	}
	else if (length && *length >= required_len)
	{
		buffer = buffer_cursor = target;
	}
	
	if (buffer)
	{
		buffer_end = buffer + required_len;
		*buffer_cursor = flags, buffer_cursor++;

		if (!(flags & VH_TY_RANGE_LInfinity))
		{
			wordlen_cursor = (int32_t*)buffer_cursor;
			buffer_cursor = (unsigned char*) (wordlen_cursor + 1);
			buffer_len = buffer_end - buffer_cursor;
			
			*wordlen_cursor = wordlen_lower;

			vh_tam_firen_bin_get(tamstack, &mod_bopts,
								 lower_boundary, buffer_cursor,
								 &buffer_len, &boundary_cursor);

			assert(buffer_len == boundary_lower_len);

			buffer_cursor += boundary_cursor;
		}

		if (!(flags & VH_TY_RANGE_UInfinity))
		{
			wordlen_cursor = (int32_t*)buffer_cursor;
			buffer_cursor = (unsigned char*) (wordlen_cursor + 1);
			buffer_len = buffer_end - buffer_cursor;

			*wordlen_cursor = wordlen_upper;

			vh_tam_firen_bin_get(tamstack, &mod_bopts,
								 upper_boundary, buffer_cursor,
								 &buffer_len, &boundary_cursor);

			assert(buffer_len == boundary_upper_len);

			buffer_cursor += boundary_cursor;
		}

		*length = required_len;
		*cursor = required_len;

		return buffer;
	}
	else
	{
		buffer = buffer_cursor = target;
		boundary_lower_offset = boundary_lower_offset ? 
								( sizeof(uint8_t) +
								  sizeof(int32_t) +
								  boundary_lower_len ) : 0;
		
		if (length && cursor)
		{
			/*
			 * We need to figure out where we're at.  It's going to be
			 * one of five places:
			 * 	1)	Start: cursor = 0
			 * 	2)	Thru the flags
			 * 	3)	Partially thru the first boundary
			 * 	4)	Thru the first boundary
			 * 	5) 	Partially thru the second boundary
			 */

			while (*cursor < *length)
			{
				buffer_len = *length - *cursor;

				if (*cursor == 0)
				{
					/*
					 * Put the flags on on the wire
					 */
					*buffer_cursor = flags, buffer_cursor++, (*cursor)++;
				}
				else if (*cursor == 1)
				{
					assert(boundary_lower_len || boundary_upper_len);

					if (buffer_len > sizeof(int32_t))
					{
						wordlen_cursor = (int32_t*)buffer_cursor;
						*wordlen_cursor = wordlen_lower ? wordlen_lower : wordlen_upper;
						buffer_cursor = (unsigned char*)(wordlen_cursor + 1);
						*cursor += sizeof(int32_t);
						continue;
					}
					else
					{
						break;
					}
				} 
				else if (boundary_lower_len && *cursor > 1 && (*cursor - 5) < boundary_lower_len)
				{
					/*
					 * We got a non-infinity lower boundary and the cursor is within
					 * it.
					 */

					boundary_cursor = boundary_lower_offset - *cursor;
					boundary_moved_by = boundary_cursor;

					vh_tam_firen_bin_get(tamstack, &mod_bopts,
										 lower_boundary,
										 buffer_cursor,
										 &buffer_len, &boundary_cursor);

					boundary_moved_by = boundary_cursor - boundary_moved_by;
					*cursor += boundary_moved_by;
					buffer_cursor += boundary_moved_by;
					continue;
				}
				else if (boundary_lower_len && *cursor == boundary_lower_offset)
				{
					/*
					 * We need to put the upper wordlen on the wire.
					 */

					assert(boundary_lower_len && boundary_upper_len);
					
					if (buffer_len > sizeof(int32_t))
					{
						wordlen_cursor = (int32_t*)buffer_cursor;
						*wordlen_cursor = wordlen_upper;
						buffer_cursor = (unsigned char*)(wordlen_cursor + 1);
						*cursor += sizeof(int32_t);
						continue;
					}
					else
					{
						break;
					}
				}
				else if (boundary_upper_len && *cursor > boundary_lower_offset + sizeof(int32_t))
				{
					/*
					 * We can put the upper boundary on the wire
					 */
					boundary_cursor = required_len - *cursor;
					boundary_moved_by = boundary_cursor;

					vh_tam_firen_bin_get(tamstack, &mod_bopts,
									 	 upper_boundary,
										 buffer_cursor,
										 &buffer_len, &boundary_cursor);
				
					boundary_moved_by = boundary_cursor - boundary_moved_by;
					*cursor += boundary_moved_by;
					buffer_cursor += boundary_moved_by;
					continue;
				}
			}

			*length = required_len;
		}
	}

	return 0;	
}

/*
 * It's a hell of a lot easier to pull a range type off the wire from Postgres
 * and true it up to the VH.IO target.  The VH.IO range type has a variable
 * length header, so all we need to do is check the inner type and roll.
 */
static void* 
pgres_ty_range_tam_set(struct TamBinSetStack *tamstack, 
					   const BinaryAMOptions bopts,
					   const void *source, void *target,
					   size_t length, size_t cursor)
{
	const vh_tam_bin_set wordlen_set = vh_type_int32.tam.bin_set;
	
	struct BinaryAMOptionData mod_bopts = { 
		.malloc = false, 
		.sourceBigEndian = bopts->sourceBigEndian,
		.targetBigEndian = bopts->targetBigEndian };

	size_t boundary_member_sz, vhio_target_sz, buffer_sz;
	unsigned char *source_upper, *source_lower, source_flags,
				  *target_upper, *target_lower, *target_flags, *i;
	int32_t source_upper_len = 0, source_lower_len = 0;
	void *malloc_target = 0;

	boundary_member_sz = vh_ty_range_boundary_member_sz(tamstack->types);
	vhio_target_sz = (boundary_member_sz * 2) + sizeof(RangeFlags);

	i = (unsigned char*) source;
	source_flags = *((uint8_t*) i);
	source_lower = i + 1;

	buffer_sz = sizeof(int32_t);
	if (!(source_flags & VH_TY_RANGE_LInfinity))
	{
		vh_tam_firee_bin_set(wordlen_set, &mod_bopts,
							 source_lower, &source_lower_len,
							 buffer_sz, 0);
		source_lower += source_lower_len;
	}

	/*
	 * We've got to account for the lower boundary length word if it 
	 * wasn't infinity.  THe only way source_lower_len is getting set to a
	 * value at all is if the lower boundary isn't infinity.
	 */	
	if (!(source_flags & VH_TY_RANGE_UInfinity))
	{
		source_upper = source_lower + 
			( source_lower_len ? sizeof(uint32_t) + source_lower_len : 0 );

		vh_tam_firee_bin_set(wordlen_set, &mod_bopts,
							 source_upper, &source_upper_len,
							 buffer_sz, 0);
	}

	if (bopts->malloc)
	{
		target = vhmalloc(vhio_target_sz);
		malloc_target = target;
	}
	
	target_lower = target;
	target_upper = target + boundary_member_sz;
	target_flags = target_upper + boundary_member_sz;

	*target_flags = source_flags;

	if (source_lower_len)
		vh_tam_firen_bin_set(tamstack, &mod_bopts,
							 source_lower, target_lower,
							 source_lower_len, 0);

	if (source_upper_len)
		vh_tam_firen_bin_set(tamstack, &mod_bopts,
							 source_upper, target_upper,
							 source_upper_len, 0);

	return malloc_target;
}

