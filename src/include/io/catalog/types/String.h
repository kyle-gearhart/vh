/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_type_String_H
#define vh_datacatalog_type_String_H

#include "io/catalog/types/varlen.h"

#if VHB_SIZEOF_VOID == 8

/* 64 bit systems */
#define VH_STR_FLAG_OOL 		0x8000000000000000ULL
#define VH_STR_MASK_OOL			0x7fffffffffffffffULL

#elif VHB_SIZEOF_VOID == 4

/* 32 bit systems */
#define VH_STR_FLAG_OOL			0x80000000u
#define VH_STR_MASK_OOL			0x7fffffffu
#endif

#define VH_STR_INLINE_BUFFER 	16
#define VH_STR_IS_OOL(str)		((str)->varlen.size & VH_STR_FLAG_OOL)
#define vh_strlen(str) 			((str)->varlen.size & VH_STR_MASK_OOL)


/*
 * Use the non-padded variable length structure.  This gives us six
 * bytes of space which would normally be padded to align on an eight
 * byte boundary.  We use these six bytes of pad to store the string
 * in line if it's less than VH_STR_INLINE_BUFFER.
 *
 * The upper bit in |size| indicates if the value is stored inline
 * or out of line.
 *
 * 	Size		Type			Total	%4	%8
 * 	6 bytes		- vhvarlenm		6		2	6
 * 	10 bytes++	- char			16		0	0
 * 	8 bytes		- char*			24		0	0
 * 	4 bytes		- uint32_t		28		0	4
 * 	==========================================
 * 	28 bytes total
 *
 * 	++ Includes 7 bytes of usable padding for vhvarlenm, plus 3
 * 	additional bytes to align |buffer| on an 8 byte boundary.
 */

typedef struct StringData
{
	struct vhvarlenmpad varlen;

	union
	{
		struct 
		{
			uint64_t capacity;
			char *buffer;
		};

		char inline_buffer[VH_STR_INLINE_BUFFER];
	};
} StringData, *String;

#define vh_str_buffer(str)		(VH_STR_IS_OOL((str)) ? (str)->buffer : &((str)->inline_buffer[0]))

#define vh_str_capacity(str)	(VH_STR_IS_OOL((str)) ? (str)->capacity : VH_STR_INLINE_BUFFER)

typedef struct StringFuncs
{
	/*
	 * Assignment Operators, Conversion Constructors,
	 * Copy Constructors
	 */

	void (*AssignStr)(String, String);
	void (*Assign)(String, const char*);
	void (*AssignN)(String, const char*, size_t);
	String (*Convert)(const char*);
	String (*ConvertN)(const char*, size_t);
	String (*ConstructStr)(String);

	/*
	 * Factory Create and Free
	 */
	String (*Create)(void);
	String (*CreateCtx)(MemoryContext);
	void (*Destroy)(String);

	/*
	 * Manipulation
	 */
	void (*AppendStr)(String, String);
	void (*Append)(String, const char*);
	void (*AppendN)(String, const char*, size_t);

	int32_t (*CompareStr)(const StringData*, const StringData*);
	int32_t (*CompareStrN)(const StringData*, const StringData*, size_t);
	int32_t (*Compare)(const StringData*, const char*);
	int32_t (*CompareN)(const StringData*, const char*, size_t);

	/*
	 * Inspection
	 */

	void (*Resize)(String, size_t);
	size_t (*Size)(String);

	void (*ToLower)(String);
} StringFuncs;

extern struct StringFuncs const vh_str;

void vh_str_init(String str);
void vh_str_finalize(String str);

#define vh_strappd(str, cstr) vh_str.Append(str, cstr)
#define vh_strappds(str1, str2) vh_str.AppendStr(str1, str2)
#define vh_strconv(cstr) vh_str.Convert(cstr)
#define vh_strconvn(cstr, n) vh_str.ConvertN(cstr, n)

char* vh_cstrdup(const char *str);

#endif

