/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <ctype.h>
#include <math.h>

#include "vh.h"
#include "io/catalog/Type.h"


#define VH_STR_SET_OOL(str) 	(str->varlen.size |= VH_STR_FLAG_OOL)
#define VH_STR_CLEAR_OOL(str)	(str->varlen.size &= ~VH_STR_FLAG_OOL)

#define VH_STR_ISINLINE(str)	(!VH_STR_IS_OOL(str))
#define VH_STR_SETINLINE(str)	VH_STR_CLEAR_OOL(str)
#define VH_STR_CLEARINLINE(str)	VH_STR_SET_OOL(str)

#define VH_STR_GROWTHPOWER(val) ((int)(log((double)(val))/log(2.0)))
#define VH_STR_GROWSZ(val) ( 1 <<(VH_STR_GROWTHPOWER(val) + 1))


static void AssignStr(String, String);
static void Assign(String, const char*);
static void AssignN(String, const char*, size_t);
static String Convert(const char*);
static String ConvertN(const char*, size_t);
static void Construct(String);
static String ConstructStr(String);

static String Create(void);
static String CreateCtx(MemoryContext);
static void Destroy(String);

static void AppendStr(String, String);
static inline void Append(String, const char*);
static inline void AppendN(String, const char*, size_t);
static int32_t CompareStr(const StringData*, const StringData*);
static int32_t CompareStrN(const StringData*, const StringData*, size_t);
static int32_t Compare(const StringData*, const char*);
static int32_t CompareN(const StringData*, const char*, size_t);

static void Resize(String, size_t);
static size_t Size(String);

static void ToLower(String);

struct StringFuncs const vh_str = 
{
	.AssignStr = AssignStr,
	.Assign = Assign,
	.AssignN = AssignN,
	.Convert = Convert,
	.ConvertN = ConvertN,
	.ConstructStr = ConstructStr,
	.Create = Create,
	.CreateCtx = CreateCtx,
	.Destroy = Destroy,
	.AppendStr = AppendStr,
	.Append = Append,
	.AppendN = AppendN,
	.CompareStr = CompareStr,
	.CompareStrN = CompareStrN,
	.Compare = Compare,
	.CompareN = CompareN,
	.Resize = Resize,
	.Size = Size,
	.ToLower = ToLower
};


static void* string_tam_bin_get(struct TamBinGetStack *tamstack, 
								const BinaryAMOptions bopts, 
								const void*, void*, 
								size_t *length, size_t *cursor);
static void* string_tam_bin_set(struct TamBinSetStack *tamstack, 
								const BinaryAMOptions bopts,
		 						const void *source, void*,
		 						size_t length, size_t cursor);
static size_t string_tam_bin_length(Type type, const void* source);


#define string_tam_cstr_literal		(0x1234u)
static void* string_tam_cstr_fmt(Type ty, const char **patterns, 
								 int32_t n_patterns);
static char* string_tam_cstr_get(struct TamCStrGetStack *tamstack,
								 CStrAMOptions copts,
								 const void *source, char *target,
								 size_t *length, size_t *cursor,
								 void *format);
static void* string_tam_cstr_set(struct TamCStrSetStack *tamstack,
								 CStrAMOptions copts,
								 const char *source, void *target,
								 size_t length, size_t cursor,
								 void *format);

static void string_tam_mset_get(struct TamGenStack *tamstack, 
								void*, void*);
static void string_tam_mset_set(struct TamGenStack *tamstack,
								void*, void*);

static int32_t string_tom_compare(struct TomCompStack *tomstack,
			   					  const void *lhs, const void *rhs);
static void string_tom_construct(struct TomConstructStack *tomstack,
								 void *target, HeapBufferNo hbno);
static void string_tom_destruct(struct TomDestructStack *tomstack,
								void *target);

struct TypeData const vh_type_String =
{
	.id = 100,
	.name = "String",
	.size = sizeof(StringData),
	.alignment = sizeof(uintptr_t),
	.varlen = true,
	.varlen_has_header = true,
	.construct_forhtd = false,
	.tam =
		{
			.bin_get = string_tam_bin_get,
			.bin_set = string_tam_bin_set,
			.bin_length = string_tam_bin_length,

			.cstr_fmt = string_tam_cstr_fmt,
			.cstr_get = string_tam_cstr_get,
			.cstr_set = string_tam_cstr_set,

			.memset_get = string_tam_mset_get,
			.memset_set = string_tam_mset_set
		},
	.tom = {
		.comp = string_tom_compare,
		.construct = string_tom_construct,
		.destruct = string_tom_destruct
	}
};



static void 
AssignStr(String target, String source)
{
	MemoryContext mctx;
	size_t sz, len;

	mctx = target->varlen.hbno ? vh_hb_memoryctx(target->varlen.hbno) :
								 vh_mctx_current();

	if (VH_STR_ISINLINE(target))
	{
		if (VH_STR_ISINLINE(source))
		{
			/*
			 * Inline target and source
			 */
			target->varlen.size = source->varlen.size;
			len = vh_strlen(source) + 1;

			memcpy(&target->inline_buffer[0],
				   &source->inline_buffer[0],
				   len);
			return;
		}
		else
		{
			/*
			 * Inline target and out of line source
			 */
			if (vh_strlen(source) + 1 <= VH_STR_INLINE_BUFFER)
			{
				/*
				 * Since the target is already inline and the source
				 * will fit in the inline buffer, just do the copy
				 * and make sure the inline flag is set on the target.
				 */
				target->varlen.size = source->varlen.size;
				VH_STR_SETINLINE(target);

				memcpy(vh_str_buffer(target),
					   vh_str_buffer(source),
					   vh_strlen(source) + 1);
				return;
			}
			else
			{
				/*
				 * We need to move from an inline target to an
				 * out of line target.
				 */
				sz = VH_STR_GROWSZ(vh_strlen(source) + 1);
				target->buffer = (char*) vhmalloc_ctx(mctx, sz);
				target->capacity = sz;
				target->varlen.size = source->varlen.size;

				memcpy(target->buffer,
					   source->buffer,
					   vh_strlen(source) + 1);
				return;
			}
		}
	}
	else
	{
		if (VH_STR_ISINLINE(source))
		{
			/*
			 * Out of line target and inline source
			 */
			if (target->capacity > (vh_strlen(source) + 1))
			{
				/*
				 * The target already had an allocated buffer large
				 * enough, so instead of wasting resources free-ing
				 * it and moving the source inline on the target,
				 * just do the copy and clear the inline flag on the
				 * target since it will get copied as inline from the
				 * source on the assignment to the target.
				 */
				target->varlen.size = source->varlen.size;	
				VH_STR_CLEARINLINE(target);
				
				memcpy(target->buffer,
					   &source->inline_buffer[0],
					   vh_strlen(source) + 1);
				return;
			}
			else
			{
				/*
				 * Since the allocated buffer wasn't large enough on
				 * the target, just move the whole thing inline on
				 * the target.  If it fits inline on the source, it
				 * will fit inline on the target, since the width of
				 * the inline buffer is a compile time constant.
				 */
				vhfree(target->buffer);
				target->buffer = &target->inline_buffer[0];
				target->varlen.size = source->varlen.size;
				memcpy(target->buffer,
					   source->buffer,
					   vh_strlen(source) + 1);
				return;
			}
		}
		else
		{
			/*
			 * Out of line target and source
			 */
			if (target->capacity >= (vh_strlen(source) + 1))
			{
				target->varlen.size = source->varlen.size;

				memcpy(target->buffer,
					   source->buffer,
					   vh_strlen(source) + 1);
				return;
			}
			else
			{
				sz = VH_STR_GROWSZ(vh_strlen(source) + 1);
				target->buffer = (char*) vhrealloc(target->buffer, sz);
				target->capacity = sz;
				target->varlen.size = source->varlen.size;

				memcpy(target->buffer,
					   source->buffer,
					   vh_strlen(source) + 1);
				return;
			}
		}
	}
}

static inline void Assign(String target, const char* source)
{
	return AssignN(target, source, strlen(source));
}

/*
 * AssignN
 *
 * This is a HOT path, so we need to be making very good decisions in here.
 *
 * When loading records from a backend this is usually our performance hog.
 * There's some alignment challenges in here that prevent this thing from
 * really moving quickly.
 */
static inline void AssignN(String target, const char* source, size_t len)
{
	MemoryContext mctx;
	size_t sz, i, required_sz;
	char *word_buff;

	required_sz = len + 1;
	mctx = target->varlen.hbno ? vh_hb_memoryctx(target->varlen.hbno) :
								 vh_mctx_current();
	
	if (VH_STR_ISINLINE(target))
	{
		if (required_sz <= VH_STR_INLINE_BUFFER)
		{
			/*
			 * Since the target is already inline and the source
			 * will fit in the inline buffer, just do the copy
			 * and make sure the inline flag is set on the target.
			 */

			target->varlen.size = len;
			VH_STR_SETINLINE(target);

			/*
			 * This is a small copy, don't use memcpy.
			 */
			for (i = 0; i < len; i++)
				target->inline_buffer[i] = source[i];

			vh_str_buffer(target)[len] = '\0';

			return;
		}
		else
		{
			/*
			 * We need to move from an inline target to an
			 * out of line target.
			 */
			sz = VH_STR_GROWSZ(len + 1);
			word_buff = vhmalloc_ctx(mctx, sz);

			target->buffer = word_buff;
			target->varlen.size = len;
			VH_STR_SET_OOL(target);
			target->capacity = sz;
			
			memcpy(word_buff,
				   source,
				   len);
			
			word_buff[len] = '\0';

			return;
		}
	}
	else
	{
		if (target->capacity >= required_sz)
		{
			/*
			 * The target already had an allocated buffer large
			 * enough, so instead of wasting resources free-ing
			 * it and moving the source inline on the target,
			 * just do the copy and clear the inline flag on the
			 * target since it will get copied as inline from the
			 * source on the assignment to the target.  Since we don't
			 * shrink string buffers, we know we've got enough space.
			 */
			target->varlen.size = len;	
			VH_STR_SET_OOL(target);

			if (len < 255)
			{
				for (i = 0; i < len; i++)
					target->buffer[i] = source[i];
			}
			else
			{
				memcpy(target->buffer,
					   source,
					   len);
			}
			
			target->buffer[len] = '\0';
			return;
		}
		else
		{
			/*
			 * Since the allocated buffer wasn't large enough on
			 * the target, just move the whole thing inline on
			 * the target.  If it fits inline on the source, it
			 * will fit inline on the target, since the width of
			 * the inline buffer is a compile time constant.
			 */
			sz = VH_STR_GROWSZ(len + 1);
			target->buffer = vhrealloc(target->buffer, sz);
			target->varlen.size = len;
			target->capacity = sz;
			VH_STR_SET_OOL(target);
			
			memcpy(target->buffer,
				   source,
				   len);

			target->buffer[len] = '\0';

			return;
		}	
	}
}

static String Convert(const char* source)
{
	return ConvertN(source, strlen(source));
}
static String ConvertN(const char* source, size_t len)
{
	String str;
	size_t sz;

	str = (String) vhmalloc(sizeof(StringData));
	str->varlen.hbno = 0;

	if (len + 1 < VH_STR_INLINE_BUFFER)
	{
		str->varlen.size = len;
		VH_STR_SETINLINE(str);
		
		memcpy(vh_str_buffer(str),
			   source,
			   len);
		vh_str_buffer(str)[len] = '\0';
	}
	else
	{
		sz = VH_STR_GROWSZ(len + 1);
		str->buffer = (char*) vhmalloc(sz);
		str->capacity = sz;
		str->varlen.size = len;
		VH_STR_SET_OOL(str);

		memcpy(str->buffer,
			   source,
			   len);
		vh_str_buffer(str)[len] = '\0';
	}

	return str;
}

/*
 * We should always assume for any function being used by the Type system
 * as the constructor that it doesn't always have to hand over zero-ed
 * out memory.  We should take care to make sure the bare minimum data
 * member are set up to avoid the program from crashing.
 */
static void Construct(String str)
{
	str->varlen.size = 0;
	memset(&str->inline_buffer[0], 0, VH_STR_INLINE_BUFFER);
	VH_STR_SETINLINE(str);
}

static String ConstructStr(String source)
{
	String str;
	size_t sz;

	str = (String) vhmalloc(sizeof(StringData));
	str->varlen.hbno = 0;

	if (VH_STR_ISINLINE(source))
	{
		str->varlen.size = source->varlen.size;

		memcpy(&str->inline_buffer[0],
			   &source->inline_buffer[0],
			   vh_strlen(source) + 1);
	}
	else
	{
		sz = VH_STR_GROWSZ(vh_strlen(source) + 1);
		str->buffer = (char*) vhmalloc(sz);
		str->capacity = sz;
		str->varlen.size = source->varlen.size;

		memcpy(str->buffer,
			   source->buffer,
			   vh_strlen(source) + 1);
	}

	return str;
}

static String Create(void)
{
	String str;

	str = (String) vhmalloc(sizeof(StringData));
	str->varlen.hbno = 0;
	str->varlen.size = 0;

	VH_STR_SETINLINE(str);

	return str;
}

static String CreateCtx(MemoryContext ctx)
{
	String str;

	str = (String) vhmalloc_ctx(ctx, sizeof(StringData));
	str->varlen.size = 0;
	VH_STR_SETINLINE(str);

	return str;
}

static void Destroy(String str)
{
	if (!VH_STR_ISINLINE(str))
	{
		vhfree(str->buffer);
		str->buffer = 0;
	}
	else
	{
		memset(&str->inline_buffer[0], 0, VH_STR_INLINE_BUFFER);
	}

	str->varlen.size = 0;
}

static void AppendStr(String target, String source)
{
	MemoryContext mctx;
	size_t sz, nlen;
	char *buffer;

	nlen = vh_strlen(target) + vh_strlen(source);
	mctx = target->varlen.hbno ? vh_hb_memoryctx(target->varlen.hbno) :
		  						 vh_mctx_current();

	if (VH_STR_ISINLINE(target))
	{
		if ((nlen + 1) <=
			VH_STR_INLINE_BUFFER)
		{
			memcpy(&target->inline_buffer[vh_strlen(target)],
				   vh_str_buffer(source),
				   vh_strlen(source) + 1);

			target->varlen.size = nlen;
			VH_STR_SETINLINE(target);
			return;
		}
		else
		{
			/*
			 * Target inline buffer not large enough, move out
			 * of line in a buffer large enough accomodate both
			 * String objects.
			 */
			sz = VH_STR_GROWSZ(nlen + 1);
			buffer = (char*) vhmalloc_ctx(mctx, sz);

			memcpy(buffer,
				   &target->inline_buffer[0],
				   vh_strlen(target));

			target->buffer = buffer;

			memcpy(&target->buffer[vh_strlen(target)],
				   vh_str_buffer(source),
				   vh_strlen(source) + 1);

			target->capacity = sz;
			target->varlen.size = nlen;

			VH_STR_SET_OOL(target);
			return;
		}

	}
	else
	{
		if (target->capacity > (nlen + 1))
		{
			/*
			 * The target already had an allocated buffer large
			 * enough.  Just copy and set the new size.
			 */			
			

			memcpy(&(vh_str_buffer(target)[vh_strlen(target)]),
				   vh_str_buffer(source),
				   vh_strlen(source) + 1);

			target->varlen.size = nlen;
			VH_STR_SET_OOL(target);
			return;
		}
		else
		{
			/*
			 * Target out of line buffer needs to grow
			 */
			sz = VH_STR_GROWSZ(nlen + 1);
			target->buffer = (char*) vhrealloc(target->buffer, sz);
			target->capacity = sz;

			memcpy(&target->buffer[vh_strlen(target)],
				   vh_str_buffer(source),
				   vh_strlen(source) + 1);

			target->varlen.size = nlen;
			VH_STR_SET_OOL(target);
			return;
		}
	}
}

static void Append(String target, const char* source)
{
	return AppendN(target, source, strlen(source));
}

static void
AppendN(String target, const char* source, size_t len)
{
	MemoryContext mctx;
	size_t sz, nlen;
	char *buffer;

	nlen = vh_strlen(target) + len;
	mctx = target->varlen.hbno ? vh_hb_memoryctx(target->varlen.hbno) :
								 vh_mctx_current();

	if (VH_STR_ISINLINE(target))
	{
		if ((nlen + 1) <=
			VH_STR_INLINE_BUFFER)
		{
			memcpy(&vh_str_buffer(target)[vh_strlen(target)],
				   source,
				   len + 1);

			target->varlen.size = nlen;
			VH_STR_SETINLINE(target);
			return;
		}
		else
		{
			/*
			 * Target inline buffer not large enough, move out
			 * of line in a buffer large enough accomodate both
			 * String objects
			 */
			sz = VH_STR_GROWSZ(nlen + 1);
			buffer = (char*) vhmalloc_ctx(mctx, sz);

			memcpy(buffer,
				   &target->inline_buffer[0],
				   vh_strlen(target));

			target->buffer = buffer;

			memcpy(&target->buffer[vh_strlen(target)],
				   source,
				   len + 1);

			target->capacity = sz;
			target->varlen.size = nlen;
			VH_STR_SET_OOL(target);
		}

	}
	else
	{
		if (target->capacity > (nlen + 1))
		{
			/*
			 * The target already had an allocated buffer large
			 * enough.  Just copy and set the new size.
			 */			
			

			memcpy(&target->buffer[vh_strlen(target)],
				   source,
				   len + 1);

			target->varlen.size = nlen;
			VH_STR_SET_OOL(target);
			return;
		}
		else
		{
			/*
			 * Target out of line buffer needs to grow
			 */
			sz = VH_STR_GROWSZ(nlen + 1);
			target->buffer = (char*) vhrealloc(target->buffer, sz);
			target->capacity = sz;

			memcpy(&target->buffer[vh_strlen(target)],
				   source,
				   len + 1);

			target->varlen.size = nlen;
			VH_STR_SET_OOL(target);
			return;
		}
	}
}

static int32_t CompareStr(const StringData* lhs, const StringData* rhs)
{
	return strcmp(vh_str_buffer(lhs), vh_str_buffer(rhs));
}

static int32_t CompareStrN(const StringData* lhs, const StringData* rhs, size_t len)
{
	return strncmp(vh_str_buffer(lhs),
				   vh_str_buffer(rhs),
				   len);
}

static int32_t Compare(const StringData* lhs, const char* rhs)
{
	size_t len;

	len = strlen(rhs);

	return strncmp(vh_str_buffer(lhs),
				   rhs,
				   (vh_strlen(lhs) < len) ? vh_strlen(lhs) : len);
}

static int32_t CompareN(const StringData* lhs, const char* rhs, size_t n)
{
	return strncmp(vh_str_buffer(lhs),
				   rhs,
				   n);
}

static void
ToLower(String str)
{
	size_t len = vh_strlen(str), i;
	char *buffer = vh_str_buffer(str);

	for (i = 0; i < len; i++)
		buffer[i] = tolower(buffer[i]);
}

static void
ToUpper(String str)
{
	size_t len = vh_strlen(str), i;
	char *buffer = vh_str_buffer(str);

	for (i = 0; i < len; i++)
		buffer[i] = toupper(buffer[i]);
}

/*
 * Resize
 *
 * For now, don't worry about shrinking.
 */
static void Resize(String target, size_t sz)
{
	MemoryContext mctx;
	size_t new_sz;
	char *buffer;

	if (target)
	{
		mctx = target->varlen.hbno ? vh_hb_memoryctx(target->varlen.hbno) :
									 vh_mctx_current();

		if (VH_STR_IS_OOL(target))
		{
			if (target->capacity < sz)
			{
				/*
				 * Target out of line buffer needs to grow
				 */
				new_sz = VH_STR_GROWSZ(sz);
				target->buffer = (char*) vhrealloc(target->buffer, new_sz);
				target->capacity = new_sz;
				VH_STR_SET_OOL(target);
			}
		}
		else
		{
			if (sz > VH_STR_INLINE_BUFFER)
			{
				/*
				 * Target inline buffer not large enough, move out
				 * of line in a buffer large enough accomodate both
				 * String objects.
				 */
				new_sz = VH_STR_GROWSZ(sz + 1);
				buffer = (char*) vhmalloc_ctx(mctx, new_sz);

				memcpy(buffer,
					   &target->inline_buffer[0],
					   vh_strlen(target));

				target->buffer = buffer;

				target->capacity = new_sz;

				VH_STR_SET_OOL(target);
			}
		}
	}
}

static size_t Size(String str)
{
	return vh_strlen(str);
}

static void* 
string_tam_bin_get(struct TamBinGetStack *tamstack, 
				   const BinaryAMOptions bopts, 
				   const void *src, void *tgt, 
				   size_t *length, size_t *cursor)
{
	const struct StringData* source = src;
	String target = tgt;
	size_t source_len = vh_strlen(source), position = 0, 
		   copy_len = source_len, min_buffer;
	void *buffer;
	char **pptarget = (char**)target;

	if (cursor)
	{
		position = *cursor;
		copy_len = source_len - position;
	}

	if (bopts->malloc)
	{
		if (!length)
		{
			elog(ERROR2,
				 emsg("vh_tam_bin_get for String has been called without a "
					  "non-null length parameter!  Please see the rules "
					  "contained in catalog/Type.h and adjust the calling "
					  "convention!"));
		}

		if (*length)
		{
			/* 
			 * A non-null, non-zero length was provided by the caller, so we
			 * should only allocate |length| bytes and set the cursor
			 */
			min_buffer = *length < copy_len ? *length : copy_len;
		}
		else
		{
			min_buffer = copy_len;
		}
		
		buffer = vhmalloc(min_buffer);
		memcpy(buffer, vh_str_buffer(source) + position, min_buffer);

		if (cursor)
			*cursor = position + min_buffer;

		if (target)
			*pptarget = buffer;

		*length = source_len;

		return buffer;
	}
	else
	{
		if (!cursor)
		{
			elog(ERROR2,
				 emsg("vh_tam_bin_get for String has been called without a "
					  "cursor.  When not authorizing malloc, a cursor should "
					  "be supplied by the caller!"));

			return 0;
		}

		if (target)
		{
			if (length && *length)
			{
				min_buffer = *length < copy_len ? *length : copy_len;
				memcpy(target, vh_str_buffer(source) + position, min_buffer);
				
				*length = source_len;
				*cursor = min_buffer + position;
			}
			else if (length)
			{
				*length = source_len;
			}
		}
		else
		{
			elog(ERROR2,
				 emsg("vh_tam_bin_get for String has been called without a "
					  "non-null target pointer pointer parameter!  Please see "
					  "the rules contained in catalog/Type.h and adjust the "
					  "calling convention!"));
		}
	}

	return 0;
}

static void* 
string_tam_bin_set(struct TamBinSetStack *tamstack, 
				   const BinaryAMOptions bopts,
				   const void *src, void* tgt,
				   size_t length, size_t cursor)
{
	const char *source = src;
	String target = tgt;
	String *pptarget = (String*)target;
	String tbuffer;

	if (bopts->malloc)
	{
		if (cursor)
		{
			elog(ERROR2, 
				 emsg("vh_tam_bin_set for String has been called with a "
					  "cursor and malloc.  Please see the rules contained "
					  "in catalog/Type.h and adjust the calling convention!"));

			return 0;
		}

		if (length)
		{
			tbuffer = ConvertN(source, length);

			if (pptarget)
				*pptarget = tbuffer;

			return tbuffer;
		}
		else
		{
			elog(ERROR2,
				 emsg("vh_tam_bin_set for String has been called without a "
					  "length and malloc.  Please see the rules contained "
					  "in catalog/Type.h and adjust the calling convention!"));
			return 0;
		}

	}
	else
	{
		if (cursor)
		{
			AppendN(target, source, length);
		}
		else
		{
			AssignN(target, source, length);
		}
	}

	return 0;
}

static size_t 
string_tam_bin_length(Type type, const void* src)
{
	const struct StringData *source = src;
	return vh_strlen(source) + 1;
}

static void 
string_tam_mset_get(struct TamGenStack *tamstack, 
					void* src, void *tgt)
{
	String source = src;
	String target = tgt;

	if (tamstack->copy_varlendat)
		target->varlen.hbno = source->varlen.hbno;

	AssignStr(source, target);	
}

static void 
string_tam_mset_set(struct TamGenStack *tamstack, 
					void *src, void *tgt)
{
	String source = src;
	String target = tgt;

	if (tamstack->copy_varlendat)
		target->varlen.hbno = source->varlen.hbno;

	AssignStr(target, source);
}

static int32_t 
string_tom_compare(struct TomCompStack *tomstack,
				   const void *lhs, const void *rhs)
{
	const StringData *l = lhs;
	const StringData *r = rhs;

	vh_tom_assert_bottom(tomstack);

	return CompareStr(l, r);
}

static void 
string_tom_construct(struct TomConstructStack *tomstack,
					 void *t, HeapBufferNo hbno)
{
	String target = t;

	vh_tom_assert_bottom(tomstack);

	target->varlen.hbno = hbno;
}

static void 
string_tom_destruct(struct TomDestructStack *tomstack,
					void *t)
{
	String target = t;

	vh_tom_assert_bottom(tomstack);

	Destroy(target);
}

/*
 * We only support one formatter for Strings, they're either a literal or
 * they are not.
 */
static void* 
string_tam_cstr_fmt(Type ty, const char **patterns, 
					int32_t n_patterns)
{
	const char *pat;

	if (n_patterns)
	{
		pat = patterns[0];

		if (strcmp(pat, "\"") == 0)
		{
			return (void*)string_tam_cstr_literal;
		}
	}

	return 0;
}

static char* 
string_tam_cstr_get(struct TamCStrGetStack *tamstack,
					CStrAMOptions copts,
					const void *source, char *target,
					size_t *length, size_t *cursor,
					void *format)
{
	const struct StringData *val = source;
	char *tgt = 0, *c;
	size_t len = vh_strlen(val), alloc_sz;
	bool quote = ((uintptr_t)format == (uintptr_t)string_tam_cstr_literal);

	alloc_sz = quote ? len + 2 : len;

	if (copts->malloc)
	{
		tgt = vhmalloc(alloc_sz);

		if (quote)
		{
			tgt[0] = '\"';
			c = tgt + 1;
		}
		else
		{
			c = tgt;
		}

		memcpy(c, vh_str_buffer(val), len);

		if (quote)
		{
			c[len] = '\"';
			c[len + 1] = '\0';
		}

		if (length)
			*length = alloc_sz;

		if (cursor)
			*cursor = alloc_sz;

		return tgt;
	}

	if (!length)
		elog(ERROR1,
			 emsg("A length must be passed when vh_tam_cstr_get is called "
				  "without an option for malloc."));

	if (*length > alloc_sz)
	{
		if (quote)
		{
			target[0] = '\"';
			c = target + 1;
		}
		else
		{
			c = target;
		}

		memcpy(c, vh_str_buffer(val), len);

		if (quote)
		{
			c[len] = '\"';
			c[len + 1] = '\0'; 
		}

		if (cursor)
			*cursor = alloc_sz;
	}
	else
	{
		if (cursor)
			*cursor = 0;
	}

	*length = alloc_sz;

	return 0;
}

static void* 
string_tam_cstr_set(struct TamCStrSetStack *tamstack,
					CStrAMOptions copts,
					const char *source, void *target,
					size_t length, size_t cursor,
					void *format)
{
	String val = target;

	if (copts->malloc)
	{
		val = vh_str.ConvertN(source, length);

		return val;
	}

	if (cursor == 0)
		vh_str.AssignN(val, source, length);
	else
		vh_str.Append(val, source);

	return 0;
}

void
vh_str_init(String str)
{
	Construct(str);
	str->varlen.hbno = 0;	
}

void
vh_str_finalize(String str)
{
	if (VH_STR_IS_OOL(str))
	{
		vhfree(str->buffer);
		str->buffer = 0;
		str->varlen.size = 0;

		VH_STR_SETINLINE(str);
	}
}

char*
vh_cstrdup(const char *str)
{
	size_t len;
	char *buf = 0;

	if (str)
	{
		len = strlen(str);
		buf = vhmalloc(len + 1);
		strcpy(buf, str);
	}

	return buf;
}

