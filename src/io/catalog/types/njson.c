/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/buffer/htpcache.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/catalog/sp/sptd.h"
#include "io/catalog/types/njson.h"
#include "io/utils/htbl.h"
#include "io/utils/SList.h"

#define JSON_TYPE_ARRAY							(0x00u)
#define JSON_TYPE_OBJECT						(0x10u)
#define JSON_TYPE_PAIR							(0x20u)
#define JSON_TYPE_PAIR_HTP_REF					(0x30u)
#define JSON_TYPE_PAIR_HTPKEY_REF				(0x40u)
#define JSON_TYPE_PAIR_HTPVAL_REF				(0x50u)
#define JSON_TYPE_VALUE							(0x60u)
#define JSON_TYPE_VALUE_HTP_REF					(0x70u)

typedef uint8_t JsonFlags;
typedef uint8_t JsonTagInfo;
typedef void *Json;

/*
 * ============================================================================
 * JSON In Memory Data Structures
 * ============================================================================
 *
 * We use a few different structures to represent the various elements of a JSON
 * document.  The first data member must always be a JsonFlags named flags.
 *
 * Since we want these to be a type available for a HeapTuple, we must also
 * include a varlenm structure in the "header" of a each Json type.  This makes
 * our declarations a little messy, because we want to use the pad space as much
 * as possible in vhvarlenm.  Since it's 9 bytes wide, it'll get aligned on a
 * 8 byte boundary, leaving 7 bytes of padding.
 *
 * JsonObjArr
 * 		Represents objects and arrays in memory.
 * JsonValue
 * 		Represents a single value.  Tags indicate the underlying type.  The size
 * 		of the JsonValue allocation depends on the type being stored on it.
 * JsonPair
 * 		Similar to JsonValue, but with a StringData component to indicate the
 * 		name of the item.
 * JsonPairRef
 * 		Both the name and value reference a HeapTuple
 * JsonPairKeyRef
 * 		The name references a HeapField
 * JsonValueRef
 * 		The data value references a HeapTuple field.  The name is contained in 
 * 		the	StringData structure.
 *
 * The 4 four bits of a JsonFlags member will be the type identifier.  If the
 * 3rd highest bit is set on a JsonPair, JsonPairKeyRef, or JsonValue then
 * the value contained is a pointer to a JsonArray or JsonObject.  This allows
 * for us to nest the JSON structures per the specification.
 *
 * The upper 4 bits of a JsonTagInfo member is the alignment padding units
 * (measured by sizeof(TypeTag)).  The lower 4 bits is the type stack depth.
 * Thus the offset to the data can be computed by adding the upper 4 bits 
 * and the lower 4 bits together and multiplying the result by sizeof(TypeTag).
 */

typedef struct JsonObjArrData *JsonObjArr;
typedef struct JsonPairData *JsonPair;
typedef struct JsonPairRefData *JsonPairRef;
typedef struct JsonPairKeyRefData *JsonPairKeyRef;
typedef struct JsonPairValueRefData *JsonPairValueRef;
typedef struct JsonValueData *JsonValue;
typedef struct JsonValueRefData *JsonValueRef;

struct JsonObjArrData
{
	JsonFlags flags;							/* 1 byte */

	int32_t size;
	int32_t capacity;							/* 4 bytes */

	Json *values;
};

struct JsonPairData
{
	JsonFlags flags;
	JsonTagInfo tag_info;
	
	/*
	 * Pair name will always be a string, we won't allow a number unless it is
	 * a string.
	 */
	StringData name;
};

#define json_jp_sz						(sizeof(struct JsonPairData))
#define json_jp_value(j)				(((unsigned char*)(j)) + 				\
										 json_jp_sz +							\
										 json_ti_offsetbytes(j))
#define json_jp_json(j)					((Json*)json_jp_value(j))

struct JsonPairRefData
{
	JsonFlags flags;
	uint16_t field_idx;
	HeapTuplePtr htp;
};

struct JsonPairKeyRefData
{
	JsonFlags flags;
	JsonTagInfo tag_info;
	HeapField hf;
};

#define json_jpkr_sz 			(sizeof(JsonFlags) +							\
								 sizeof(JsonTagInfo) +							\
								 sizeof(HeapField))
#define json_jpkr_value(j)		(((unsigned char*)(j)) +						\
								 json_jpkr_sz +									\
								 json_ti_offsetbytes(j))
#define json_jpkr_json(j)		((Json*)json_jpkr_value(j))

struct JsonPairValueRefData
{
	JsonFlags flags;	
	uint16_t field_idx;

	StringData name;
	HeapTuplePtr htp;
};

#define json_jpvr_sz 					(sizeof(struct JsonPairValueRefData))

struct JsonValueData
{
	JsonFlags flags;
	JsonTagInfo tag_info;
};

#define json_jv_sz 				(sizeof(JsonFlags) + sizeof(JsonTagInfo))
#define json_jv_value(j)		(((unsigned char*)(j)) +						\
								  json_jv_sz +									\
								  json_ti_offsetbytes(j))
#define json_jv_json(j)			((Json*)json_jv_value(j))

struct JsonValueRefData
{
	JsonFlags flags;
	JsonTagInfo tag_info;
	uint16_t field_idx;

	HeapTuplePtr htp;
};


/*
 * ============================================================================
 * JsonFlags Macros
 * ============================================================================
 */

#define json_flags(j)							(((JsonFlags*)(j))[0])
#define json_flags_mask_type					(0xf0u)
#define json_isa(j, t)							((json_flags(j) & json_flags_mask_type) == (t))

/*
 * ObjArr indicates the JsonPair, JsonPairKeyRef, or JsonValue object doesn't
 * contain a TypeVar for the value, but rather a pointer to a JsonObjArr.
 * This allows for nesting of documents to occur.
 */
#define json_flags_objarr						(0x08u)
#define json_flags_null							(0x04u)

#define json_isnull(j)							(json_flags(j) & json_flags_null)



/*
 * ============================================================================
 * JsonTagInfo Macros
 * ============================================================================
 */

#define json_ti_get_alignfactor(j)				(((j)->tag_info & (0xf0u)) >> 4)
#define json_ti_get_depth(j)					((j)->tag_info & (0x0fu))

#define json_ti_set_alignfactor(j, af)			((j)->tag_info = ((JsonTagInfo)(af)) << 4)
#define json_ti_set_depth(j, td)				((j)->tag_info = (JsonTagInfo)(td))
#define json_ti_set(j, af, td)					((j)->tag_info = (((JsonTagInfo)(af)) << 4) | 	\
												((JsonTagInfo)(td)))

#define json_ti_offsetfactor(j)					(json_ti_get_alignfactor(j) + 	\
												 json_ti_get_depth(j))
#define json_ti_offsetbytes(j)					(json_ti_offsetfactor(j) * sizeof(TypeTag))



/*
 * ============================================================================
 * Helper Structures
 * ============================================================================
 */

typedef struct JsonStackData *JsonStack;

struct JsonStackData
{
	Json json;

	JsonStack parent;
	JsonStack child;
};

#define JSON_ACCESS_ARRAY						0x01
#define JSON_ACCESS_KEYNAME						0x02

struct JsonAccessData
{
	Json json;

	union
	{
		int32_t idx;
		const char *key_name;
	};

	int32_t flags;
};

/*
 * ============================================================================
 * Helper Functions
 * ============================================================================
 */
static HeapField json_htp_resolve_field_name(HeapTuplePtr htp, 
											 const char *field_name);


/*
 * ============================================================================
 * Tree Helper Functions
 * ============================================================================
 */

static Json json_tree_traverse(Json root, int32_t flags, int32_t params,
	   						   struct JsonAccessData **path, va_list args);

/*
 * ============================================================================
 * Array/Object Helper Functions
 * ============================================================================
 */

static Json json_make_objarr(void);

static Json json_objarr_pop(Json jobjarr);
static void json_objarr_push(Json jobjarr, Json jval);

static void json_objarr_destroy(Json jval);
static void json_objarr_finalize(Json jval);

static HashTable json_obj_key_hash(JsonObjArr jobj, HtpCache htpc, 
								   bool ignore_dup);

static const char* json_pair_key_name(Json pair, HtpCache htpc);


/*
 * ============================================================================
 * Stringify Functions
 * ============================================================================
 */
static void json_stringify(HashTable htbl, Json jval, String str);

/* Hash Table Functions (key: TypeTag; value: formatter) */
static HashTable json_stringify_create_fmt_htbl(void);
static void json_stringify_destroy_fmt_htbl(HashTable htbl);
static bool json_stringify_destroy_formats(HashTable htbl, const void *key,
	   									   void *entry, void *data);	

static size_t json_stringify_data(HashTable formatters, Type *tys, int32_t ty_depth,
								  void *data,
			  					  char *buffer, size_t buffer_sz,
		  						  void* (*extend_buffer)(void *h, size_t c,size_t s),
		  						  void *buffer_head, size_t buffer_cursor);

static void* json_stringify_extend_str_buff(void *buffer_head, 
											size_t cursor, size_t space);


/* JSON Default String Formatters */
struct JsonStringifyFormats
{
	TypeTag id;
	const char *fmt;
};

static struct JsonStringifyFormats stringify_fmts[] = {
	{ 36, "\"" }, 								/* Numeric */ 
	{ 100, "\"" },								/* String */
	{ 110, "\"yyyy-MM-dd\"" },					/* Date */
	{ 120, "\"yyyy-MM-dd'T'kk:mm:ss\"" },		/* DateTime */
	{ 0, 0 }
};

static const char* json_stringify_get_fmt(Type ty);



/*
 * ============================================================================
 * HeapTuple Accelerators
 * ============================================================================
 */

static int32_t json_ht(Json json, SList fields,
					   HtpCache htpc,
					   HeapTuplePtr htp, HeapTuple ht);
static int32_t ht_json(HeapTuplePtr htp, HeapTuple ht, 
					   SList fields, bool with_ref, Json *json);

/*
 * ============================================================================
 * Public Interface: ISA
 * ============================================================================
 */

bool
vh_json_isa_obj(Json j)
{
	return json_isa(j, JSON_TYPE_OBJECT); 
}

bool
vh_json_isa_array(Json j)
{
	return json_isa(j, JSON_TYPE_ARRAY);
}

bool
vh_json_isa_pair(Json j)
{
	return json_isa(j, JSON_TYPE_PAIR) || 
		   json_isa(j, JSON_TYPE_PAIR_HTP_REF) ||
		   json_isa(j, JSON_TYPE_PAIR_HTPKEY_REF) ||
		   json_isa(j, JSON_TYPE_PAIR_HTPVAL_REF);
}

bool
vh_json_isa_value(Json j)
{
	return json_isa(j, JSON_TYPE_VALUE) ||
		   json_isa(j, JSON_TYPE_VALUE_HTP_REF);
}



/*
 * ============================================================================
 * Public Interface: MAKE Routines
 * ============================================================================
 */

Json
vh_json_make_array(void)
{
	JsonObjArr joa;

	joa = json_make_objarr();
	joa->flags = JSON_TYPE_ARRAY;

	return joa;
}

Json
vh_json_make_object(void)
{
	JsonObjArr joa;

	joa = json_make_objarr();
	joa->flags = JSON_TYPE_OBJECT;

	return joa;
}



/*
 * ============================================================================
 * JSON Make Pair
 * ============================================================================
 */

Json
vh_json_make_pair(Type *tys, int32_t ty_depth, const char *name)
{
	TypeVar val;
	JsonPair jp;
	int32_t align_factor;
	void *data_at;
	bool is_typevar;

	if (!name)
	{
		elog(WARNING,
				emsg("Invalid JSON Pair key name string pointer [%p] passed to "
					 "vh_json_make_pair.  JSON RFC 4627 requires each object key "
					 "value pair have string key name.",
					 name));

		return 0;
	}

	jp = vh_typevar_create(tys, ty_depth, &align_factor,		/* Type stack, depth, factor */
						   json_jp_sz, 0, 0,					/* Header, footer sz & f. align */
						   &data_at, 0);						/* Data at, footer at */
	
	jp->flags = JSON_TYPE_PAIR;
	json_ti_set(jp, align_factor, ty_depth);	
	vh_str_init(&jp->name);

	/*
	 * Copy over the name into the new JSON object.
	 */	
	vh_str.Assign(&jp->name, name);

	/*
	 * Do a quick check to make sure vh_typevar_create and vh_json_typevar
	 * agree where the underlying data type lies.
	 */
	val = vh_json_typevar(jp, &is_typevar);
	assert(is_typevar);
	assert(data_at);
	assert(val == data_at);

	return jp;
}

Json
vh_json_make_pair_htp_nm(HeapTuplePtr htp, const char *field_name)
{
	HeapField hf;

	hf = json_htp_resolve_field_name(htp, field_name);

	if (hf)
		return vh_json_make_pair_htp_hf(htp, hf);

	if (field_name)
	{
		elog(WARNING,
				emsg("The HeapTuplePtr [%llu] does not have a field named "
					 "[%s].  Unable to create the JSON pair referencing the "
					 "field [%s].",
					 htp,
					 field_name,
					 field_name));
	}
	else
	{
		elog(WARNING,
				emsg("Invalid field name pointer [%p] provided to "
					 "vh_json_make_pair_htp_nm.  Unable to proceed.",
					 field_name));
	}

	return 0;
}

Json
vh_json_make_pair_htp_hf(HeapTuplePtr htp, HeapField hf)
{
	JsonPairRef jpr;

	if (!htp)
	{
		elog(WARNING,
				emsg("Invalid HeapTuplePtr [%llu] passed to "
					 "vh_json_make_pair_htp_hf, unable to create a JSON value.",
					 htp));
		return 0;
	}

	if (!hf)
	{
		elog(WARNING,
				emsg("Invalid HeapField pointer [%p] passed to "
					 "vh_json_make_pair_htp_hf, unable to create a JSON value.",
					 hf));

		return 0;
	}

	jpr = vhmalloc(sizeof(struct JsonPairRefData));
	jpr->flags = JSON_TYPE_PAIR_HTP_REF;
	jpr->field_idx = hf->dord;
	jpr->htp = htp;

	return jpr;
}

Json
vh_json_make_pair_krh_nm(HeapTuplePtr htp, const char *field_name)
{
	HeapField hf;

	hf = json_htp_resolve_field_name(htp, field_name);

	if (hf)
		return vh_json_make_pair_krh_hf(htp, hf);

	if (field_name)
	{
		elog(WARNING,
				emsg("The HeapTuplePtr [%llu] does not have a field named "
					 "[%s].  Unable to create the JSON pair referencing the "
					 "value on the HeapTuple.",
					 htp,
					 field_name));
	}
	else
	{
		elog(WARNING,
				emsg("Invalid field name pointer [%p] provided to "
					 "vh_json_make_pair_krh_nm.  Unable to proceed.",
					 field_name));
	}

	return 0;
}

Json
vh_json_make_pair_krh_hf(HeapTuplePtr htp, HeapField hf)
{
	JsonPairKeyRef jpkr;
	int32_t align_factor;
	void *data, *datal;
	bool is_typevar;

	if (!htp)
	{
		elog(WARNING,
				emsg("Invalid HeapTuplePtr [%llu] passed to "
					 "vh_json_make_pair_krh_hf, unable to create a JSON value.",
					 htp));
		return 0;
	}

	if (!hf)
	{
		elog(WARNING,
				emsg("Invalid HeapField pointer [%p] passed to "
					 "vh_json_make_pair_krh_hf, unable to create a JSON value.",
					 hf));

		return 0;
	}

	jpkr = vh_typevar_create(&hf->types[0], hf->type_depth, &align_factor,
							 json_jpkr_sz, 0, 0,
							 &data, 0);

	jpkr->flags = JSON_TYPE_PAIR_HTPKEY_REF;
	json_ti_set(jpkr, align_factor, hf->type_depth);

	jpkr->hf = hf;

	/*
	 * Make sure the pointer we calculate for the value is the same that
	 * vh_typevar_create thinks it should be.  Simple assert.
	 */
	datal = vh_json_typevar(jpkr, &is_typevar);
	assert(is_typevar);
	assert(data);
	assert(data == datal);

	return jpkr;
}

Json
vh_json_make_pair_vrh_nm(HeapTuplePtr htp, 
						 const char *htp_field, const char *field_name)
{
	HeapField hf;

	hf = json_htp_resolve_field_name(htp, htp_field);

	if (hf)
		return vh_json_make_pair_vrh_hf(htp, hf, field_name);

	if (field_name)
	{
		elog(WARNING,
				emsg("The HeapTuplePtr [%llu] does not have a field named "
					 "[%s].  Unable to create the JSON pair referencing the "
					 "value on the HeapTuple.",
					 htp,
					 field_name));
	}
	else
	{
		elog(WARNING,
				emsg("Invalid field name pointer [%p] provided to "
					 "vh_json_make_pair_vrh_nm.  Unable to proceed.",
					 field_name));
	}

	return 0;
}

Json
vh_json_make_pair_vrh_hf(HeapTuplePtr htp, HeapField hf,
						 const char *key_name)
{
	JsonPairValueRef jpvr;

	if (!htp)
	{
		elog(WARNING,
				emsg("Invalid HeapTuplePtr [%llu] passed to "
					 "vh_json_make_pair_vrh_hf, unable to create a JSON Pair.",
					 htp));
		return 0;
	}

	if (!hf)
	{
		elog(WARNING,
				emsg("Invalid HeapField pointer [%p] passed to "
					 "vh_json_make_pair_vrh_hf, unable to create a JSON Pair.",
					 hf));

		return 0;
	}

	jpvr = vhmalloc(json_jpvr_sz);
	jpvr->flags = JSON_TYPE_PAIR_HTPVAL_REF;
	jpvr->field_idx = hf->dord;
	jpvr->htp = htp;

	vh_str_init(&jpvr->name);
	vh_str.Assign(&jpvr->name, key_name);

	return jpvr;
}

Json
vh_json_make_pair_obj(const char *key_name)
{
	JsonPair jp;
	Json obj;
	Json *valat;

	if (!key_name)
	{
		elog(WARNING,
				emsg("Invalid key name pointer [%p] passed to "
					 "vh_json_make_pair_obj, unable to create a JSON Pair.",
					 key_name));

		return 0;
	}

	jp = vhmalloc(sizeof(struct JsonPairData) + sizeof(uintptr_t));
	jp->flags = JSON_TYPE_PAIR | json_flags_objarr;
	json_ti_set(jp, 0, 0);

	vh_str_init(&jp->name);
	vh_str.Append(&jp->name, key_name);

	valat = json_jp_json(jp);
	obj = vh_json_make_object();
	*valat = obj;

	return jp;
}

Json
vh_json_make_pair_arr(const char *key_name)
{
	JsonPair jp;
	Json arr;
	Json *valat;

	if (!key_name)
	{
		elog(WARNING,
				emsg("Invalid key name pointer [%p] passed to "
					 "vh_json_make_pair_arr, unable to create a JSON Pair.",
					 key_name));

		return 0;
	}

	jp = vhmalloc(sizeof(struct JsonPairData) + sizeof(uintptr_t));
	jp->flags = JSON_TYPE_PAIR | json_flags_objarr;
	json_ti_set(jp, 0, 0);

	vh_str_init(&jp->name);
	vh_str.Assign(&jp->name, key_name);

	valat = json_jp_json(jp);
	arr = vh_json_make_array();
	*valat = arr;

	return jp;
}

/*
 * vh_json_make_pair_objarr
 *
 * This one is a little tricky and out of the norm, but it's due to our BISON
 * parser.  Basically we create a pair that stores the JSON Array or Object
 * passed in jarr_or_obj.
 */
Json
vh_json_make_pair_objarr(const char *key_name, Json jarr_or_obj)
{
	JsonPair jp;
	Json *valat;

	if (!key_name)
	{
		elog(WARNING,
				emsg("Invalid key name pointer [%p] passed to "
					 "vh_json_make_pair_objarr, unable to create a JSON Pair.",
					 key_name));

		return 0;
	}

	if (!jarr_or_obj)
	{
		elog(WARNING,
				emsg("Invalid JSON Array or Object pointer [%p] passed to "
					 "vh_json_make_pair_objarr, unable to create a JSON Pair.",
					 jarr_or_obj));
	}

	if (!(((json_flags(jarr_or_obj) & json_flags_mask_type) == JSON_TYPE_ARRAY) ||
		json_flags(jarr_or_obj) & JSON_TYPE_OBJECT))
	{
		elog(WARNING,
				emsg("The JSON element passed as jarr_or_obj [%p] is not a JSON "
					 "Array or Object.  Unable to generate a new pair for key "
					 "%s",
					 jarr_or_obj,
					 key_name));

		return 0;
	}

	jp = vhmalloc(sizeof(struct JsonPairData) + sizeof(uintptr_t));
	jp->flags = JSON_TYPE_PAIR | json_flags_objarr;
	json_ti_set(jp, 0, 0);

	vh_str_init(&jp->name);
	vh_str.Assign(&jp->name, key_name);

	valat = json_jp_json(jp);
	*valat = jarr_or_obj;

	return jp;
}

Json
vh_json_make_pair_bool(const char *name, bool value)
{
	Type tys[] = { &vh_type_bool, 0 };
	Json jval;
	bool *v, is_typevar;

	jval = vh_json_make_pair(tys, 1, name);
	v = vh_json_typevar(jval, &is_typevar);

	if (!is_typevar && jval)
	{
		vh_json_destroy(jval);
	}

	*v = value;

	return jval;
}

Json
vh_json_make_pair_cstr(const char *name, const char *value)
{
	Type tys[] = { &vh_type_String, 0 };
	Json jval;
	String v;
	bool is_typevar;

	jval = vh_json_make_pair(tys, 1, name);
	v = vh_json_typevar(jval, &is_typevar);

	if (!is_typevar && jval)
	{
		vh_json_destroy(jval);
	}

	vh_str.Assign(v, value);

	return jval;
}

Json
vh_json_make_pair_i32(const char *name, int32_t value)
{
	Type tys[] = { &vh_type_int32, 0 };
	Json jval;
	int32_t *v;
	bool is_typevar;

	jval = vh_json_make_pair(tys, 1, name);
	v = vh_json_typevar(jval, &is_typevar);

	if (!is_typevar && jval)
	{
		vh_json_destroy(jval);
	}

	*v = value;

	return jval;
}

Json
vh_json_make_pair_null(const char *name)
{
	JsonPair jp;

	jp = vhmalloc(sizeof(struct JsonPairData));
	jp->flags = JSON_TYPE_PAIR | json_flags_null;
	vh_str_init(&jp->name);

	vh_str.Assign(&jp->name, name);

	return jp;
}

Json
vh_json_make_pair_tvs(const char *name, TypeVarSlot *slot)
{
	return 0;
}

/*
 * ============================================================================
 * JSON Make Value
 * ============================================================================
 */
Json
vh_json_make_value(Type *tys, int32_t ty_depth)
{
	JsonValue jv;
	int32_t align_factor;

	jv = vh_typevar_create(tys, 							/* Type stack */
						   ty_depth, 						/* Tag count */
						   &align_factor, 					/* Tag align factor */
						   sizeof(struct JsonValueData), 	/* Header size */
						   0, 								/* Footer size */
						   0,								/* Footer alignment */
						   0, 								/* Data At */
						   0);								/* Footer At */

	jv->flags = JSON_TYPE_VALUE;
	json_ti_set(jv, align_factor, ty_depth);

	return jv;
}

Json
vh_json_make_value_null(void)
{
	JsonValue jv;

	jv = vhmalloc(sizeof(struct JsonValueData));
	jv->flags = JSON_TYPE_VALUE | json_flags_null;

	return jv;	
}

Json
vh_json_make_value_htp_nm(HeapTuplePtr htp, const char *field_name)
{
	HeapField hf;

	hf = json_htp_resolve_field_name(htp, field_name);

	if (hf)
		return vh_json_make_value_htp_hf(htp, hf);

	if (field_name)
	{
		elog(WARNING,
				emsg("The HeapTuplePtr [%llu] does not have a field named "
					 "[%s].  Unable to create the JSON value referencing the "
					 "field [%s].",
					 htp,
					 field_name,
					 field_name));
	}
	else
	{
		elog(WARNING,
				emsg("Invalid field name pointer [%p] provided to "
					 "vh_json_make_value_htp_nm.  Unable to proceed.",
					 field_name));
	}

	return 0;
}

Json
vh_json_make_value_htp_hf(HeapTuplePtr htp, HeapField hf)
{
	JsonValueRef jvr = 0;

	if (!htp)
	{
		elog(WARNING,
				emsg("Invalid HeapTuplePtr [%llu] passed to "
					 "vh_json_make_value_htp_hf, unable to create a JSON value.",
					 htp));
		return 0;
	}

	if (!hf)
	{
		elog(WARNING,
				emsg("Invalid HeapField pointer [%p] passed to "
					 "vh_json_make_value_htp_hf, unable to create a JSON value.",
					 hf));

		return 0;
	}

	jvr = vhmalloc(sizeof(struct JsonValueRefData));
	jvr->flags = JSON_TYPE_VALUE_HTP_REF;
	jvr->tag_info = 0;

	jvr->htp = htp;
	jvr->field_idx = hf->dord;

	return jvr;
}



/*
 * ============================================================================
 * Public Interface: Value Routine
 * ============================================================================
 *
 * vh_json_objarr
 *
 * We only want to return a Json pointer here to be consistent with our naming
 * conventions.  If hte JSON actually stores a TypeVar, we'll set the flag
 * if provided and return null.
 */
void*
vh_json_objarr(Json jval, bool *is_objarr)
{
	JsonPair jp = 0;
	JsonPairRef jpr = 0;
	JsonPairKeyRef jpkr = 0;
	JsonValue jv = 0;
	bool objarr;
	void **pp;

	if (!jval)
	{
		elog(WARNING,
				emsg("Invalid Json pointer [%p] passed to vh_json_objarr.  "
					 "Unable to derive the underlying JSON type and return a "
					 "JSON Array or Object pointer.",
					 jval));

		return 0;
	}

	objarr = (json_flags(jval) & json_flags_objarr);

	if (is_objarr)
		*is_objarr = objarr;

	switch (json_flags(jval) & json_flags_mask_type)
	{
		case JSON_TYPE_ARRAY:
			elog(WARNING,
					emsg("Json value at address [%p] is an Array, cannot return "
						 "it as a value.  Use the vh_json_array_ family of functions "
						 "to manipulate the Array.",
						 jval));

			return 0;

		case JSON_TYPE_OBJECT:
			elog(WARNING,
					emsg("Json value at address [%p] is an Object, cannot return "
						 "it as a value.  Use the vh_json_obj_ family of functions "
						 "to manipulate the Object.",
						 jval));

			return 0;

		case JSON_TYPE_PAIR:

			jp = jval;

			if (!objarr)
				return 0;

			pp = json_jp_json(jp);

			return *pp;

		case JSON_TYPE_PAIR_HTP_REF:
	
			jpr = jval;

			elog(WARNING,
					emsg("Json value at address [%p] references a HeapTuplePtr field.  "
						 "The value is not an Array or Object.",
						 jpr,
						 jpr->htp));

			return 0;

		case JSON_TYPE_PAIR_HTPKEY_REF:

			jpkr = jval;

			if (!objarr)
				return 0;

			pp = json_jpkr_json(jpkr);

			return *pp;

		case JSON_TYPE_PAIR_HTPVAL_REF:

			elog(WARNING,
					emsg("Json value at address [%p] references a HeapTuplePtr value.  "
						 "Unable to return a JSON Array or Object.",
						 jpr,
						 jpr->htp));

			return 0;

		case JSON_TYPE_VALUE:

			/*
			 * This one is super easy.
			 */
			jv = jval;

			if (!objarr)
				return 0;

			pp = json_jv_json(jv);

			return *pp;

	}

	return 0;
}

/*
 * vh_json_typevar
 *
 * We only want to return a TypeVar here to be consistent with our naming 
 * conventions.  If the JSON actually stores a pointer to another JSON Array
 * or Object, we'll set the flag if it's provided and return null.
 */

void*
vh_json_typevar(Json jval, bool *is_typevar)
{
	JsonPair jp;
	JsonPairRef jpr;
	JsonPairKeyRef jpkr;
	JsonPairValueRef jpvr;
	JsonValue jv;
	bool typevar;

	if (!jval)
	{
		elog(WARNING,
				emsg("Invalid Json pointer [%p] passed to vh_json_value.  "
					 "Unable to derive the underlying JSON type and return a "
					 "value pointer",
					 jval));

		return 0;
	}

	/*
	 * Set our flag to indicate the value returned is a nest JSON object
	 * or array.
	 */

	typevar = !(json_flags(jval) & json_flags_objarr);

	if (is_typevar)
		*is_typevar = typevar;

	switch (json_flags(jval) & json_flags_mask_type)
	{
		case JSON_TYPE_ARRAY:
			elog(WARNING,
					emsg("Json value at address [%p] is an Array, cannot return "
						 "it's value.  Use the vh_json_array_ family of functions "
						 "to manipulate the Array.",
						 jval));

			return 0;

		case JSON_TYPE_OBJECT:
			elog(WARNING,
					emsg("Json value at address [%p] is an Object, cannot return "
						 "it's value.  Use the vh_json_obj_ family of functions "
						 "to manipulate the Object.",
						 jval));

			return 0;

		case JSON_TYPE_PAIR:

			jp = jval;

			if (!typevar)
				return 0;

			if (json_isnull(jp))
				return 0;

			return json_jp_value(jp);

		case JSON_TYPE_PAIR_HTP_REF:
			jpr = jval;

			elog(WARNING,
					emsg("Json value at address [%p] references a HeapTuplePtr field.  "
						 "To change the value, use an assignment operator against the "
						 "HeapTuplePtr at [%llu]",
						 jpr,
						 jpr->htp));

			return 0;

		case JSON_TYPE_PAIR_HTPKEY_REF:

			jpkr = jval;

			if (!typevar)
				return 0;

			return json_jpkr_value(jpkr);

		case JSON_TYPE_PAIR_HTPVAL_REF:

			jpvr = jval;

			elog(WARNING,
					emsg("Json value at address [%p] references a HeapTuplePtr value.  "
						 "To change the value, use an assignment operator against the "
						 "HeapTuplePtr at [%llu].",
						 jpvr,
						 jpvr->htp));

			return 0;

		case JSON_TYPE_VALUE:

			/*
			 * This one is super easy.
			 */
			jv = jval;

			if (!typevar)
				return 0;

			if (json_isnull(jv))
				return 0;

			return json_jv_value(jv);

	}

	return 0;
}

/*
 * vh_json_htp
 *
 * Returns the HeapTuplePtr referenced by the object
 */
HeapTuplePtr
vh_json_htp(Json jval)
{
	JsonPairRef jpr;
	JsonPairValueRef jpvr;
	JsonValueRef jvr;

	if (!jval)
	{
		elog(WARNING,
				emsg("Invalid Json pointer [%p] passed to vh_json_htp.  "
					 "Unable to derive the underlying JSON type and return a "
					 "HeapTuplePtr.",
					 jval));

		return 0;
	}

	switch (json_flags(jval) & json_flags_mask_type)
	{
		case JSON_TYPE_PAIR_HTP_REF:
			jpr = jval;
			return jpr->htp;

		case JSON_TYPE_PAIR_HTPKEY_REF:
			return 0;

		case JSON_TYPE_PAIR_HTPVAL_REF:
			jpvr = jval;
			return jpvr->htp;

		case JSON_TYPE_VALUE_HTP_REF:
			jvr = jval;
			return jvr->htp;

		default:
			return 0;
	}	
	
	return 0;
}



/*
 * ============================================================================
 * Public Interface: Cleanup Routines
 * ============================================================================
 */

void
vh_json_finalize(Json jval)
{
	JsonPair jp;
	JsonPairValueRef jpvr;
	TypeVar var;
	bool is_typevar, is_objarr, is_null, possible_value = true;

	if (jval)
	{
		switch (json_flags(jval) & json_flags_mask_type)
		{
			case JSON_TYPE_ARRAY:
			case JSON_TYPE_OBJECT:
				json_objarr_finalize(jval);
				possible_value = false;
				break;

			case JSON_TYPE_PAIR:
				jp = jval;
				vh_str_finalize(&jp->name);
				break;

			case JSON_TYPE_PAIR_HTPVAL_REF:
				jpvr = jval;
				vh_str_finalize(&jpvr->name);
				possible_value = false;
				break;

			case JSON_TYPE_PAIR_HTP_REF:
				possible_value = false;
				break;

			case JSON_TYPE_PAIR_HTPKEY_REF:
				/*
				 * We don't have to release the key but we should let it check
				 * for a value and release it.
				 */

				break;
		}

		/*
		 * This is on the Value side of the house.
		 */
		if (possible_value)
		{
			is_typevar = !(json_flags(jval) & json_flags_objarr);
			is_null = json_isnull(jval);

			if (is_typevar && !is_null)
			{
				var = vh_json_typevar(jval, &is_typevar);
				
				if (var && is_typevar)
				{
					vh_typevar_finalize(var);
				}
			}
			else if (!is_typevar)
			{
				assert(!is_null);
				var = vh_json_objarr(jval, &is_objarr);

				if (var && is_objarr)
				{
					json_objarr_finalize(var);
					vhfree(var);
				}
			}
		}
	}
}

void
vh_json_free(Json jval)
{
	if (jval)
		vhfree(jval);
}

void
vh_json_destroy(Json jval)
{
	JsonPair jp;
	JsonPairValueRef jpvr;
	TypeVar var;
	bool is_typevar, is_objarr, is_null, possible_value = true;

	if (jval)
	{
		switch (json_flags(jval) & json_flags_mask_type)
		{
			case JSON_TYPE_ARRAY:
			case JSON_TYPE_OBJECT:
				json_objarr_destroy(jval);
				possible_value = false;
				break;

			case JSON_TYPE_PAIR:
				jp = jval;
				vh_str_finalize(&jp->name);
				break;

			case JSON_TYPE_PAIR_HTPVAL_REF:
				jpvr = jval;
				vh_str_finalize(&jpvr->name);
				possible_value = false;
				break;

			case JSON_TYPE_PAIR_HTP_REF:
				possible_value = false;
				break;

			case JSON_TYPE_PAIR_HTPKEY_REF:
				/* 
				 * JSON_TYPE_PAIR_HTPKEY_REF
				 *
				 * Has a possible value but we don't need to release the
				 * key.
				 */

				break;
		}

		/*
		 * This is on the Value side of the house.
		 */
		if (possible_value)
		{
			is_typevar = !(json_flags(jval) & json_flags_objarr);
			is_null = json_isnull(jval);

			if (is_typevar && !is_null)
			{
				var = vh_json_typevar(jval, &is_typevar);
				
				if (var && is_typevar)
				{
					vh_typevar_finalize(var);
				}
			}
			else if (!is_typevar)
			{
				assert(!is_null);
				var = vh_json_objarr(jval, &is_objarr);

				if (var && is_objarr)
				{
					json_objarr_destroy(var);
				}
			}
		}

		vhfree(jval);
	}
}



/*
 * ============================================================================
 * JSON Array Functions
 * ============================================================================
 */
int32_t
vh_json_arr_count(Json jval)
{
	JsonObjArr joa;
	bool isa_array;

	if (!jval)
	{
		elog(WARNING, 
				emsg("Invalid JSON Array pointer [%p] passed to "
					 "vh_json_arr_count.  Unable to determine the count "
					 "of array entries.",
					 jval));

		return -1;
	}

	isa_array = (json_flags(jval) & json_flags_mask_type) == JSON_TYPE_ARRAY;

	if (isa_array)
	{
		joa = jval;

		return joa->size;
	}

	elog(WARNING,
			emsg("JSON element at pointer [%p] is not a JSON Array.  Unable"
				 " to provide the count of array entries.",
				 jval));

	return -2;
}

Json
vh_json_arr_atidx(Json jval, int32_t idx)
{
	JsonObjArr joa;
	bool isa_array;

	if (!jval)
	{
		elog(WARNING,
				emsg("Invalid JSON Array pointer [%p] passed to "
					 "vh_json_arr_atidx.  Unable to return the JSON "
					 "element at index %d",
					 jval,
					 idx));
	}

	isa_array = (json_flags(jval) & json_flags_mask_type) == JSON_TYPE_ARRAY;

	if (isa_array)
	{
		joa = jval;

		if (idx < 0 ||
			idx >= joa->size)
		{
			elog(ERROR1,
					emsg("Index %d is out of bounds for the JSON Array at [%p]!"
						 "  Only %d items exist.",
						 idx,
						 jval,
						 joa->size));

			return 0;
		}

		return joa->values[idx];
	}

	elog(WARNING,
			emsg("JSON element at [%p] is not a JSON Array.  Unable to return "
				 "the JSON element at index %d",
				 jval,
				 idx));

	return 0;
}

bool
vh_json_arr_push(Json jarray, Json jval)
{
	bool isa_array, isa_array_member;

	if (!jarray)
	{
		elog(WARNING,
				emsg("Invalid JSON Array pointer at [%p].  Unable to push the element "
					 "[%p] into an invalid JSON Array pointer.",
					 jarray,
					 jval));

		return false;
	}

	if (!jval)
	{
		elog(WARNING,
				emsg("Invalid JSON Array Member pointer at [%p].  Unable to push the "
					 "element into the JSON Array at [%p]",
					 jval,
					 jarray));

		return false;
	}

	isa_array = (json_flags(jarray) & json_flags_mask_type) == JSON_TYPE_ARRAY;

	if (!isa_array)
	{
		elog(ERROR1,
				emsg("Expected a JSON Array at [%p].  Unable to push the JSON element "
					 "into the target.",
					 jarray));

		return false;
	}

	/*
	 * We can push everything but Pair into a JSON array, so check to make sure
	 * that's what we're trying to do!
	 */
	isa_array_member = ((json_flags(jval) & json_flags_mask_type) == JSON_TYPE_ARRAY) ||
					   (json_flags(jval) & JSON_TYPE_OBJECT) ||
					   (json_flags(jval) & JSON_TYPE_VALUE) ||
					   (json_flags(jval) & JSON_TYPE_VALUE_HTP_REF);

	if (!isa_array_member)
	{
		elog(ERROR1,
				emsg("JSON Pairs cannot be inserted into a JSON Array."));

		return false;
	}

	json_objarr_push(jarray, jval);

	return true;
}

/*
 * ============================================================================
 * JSON Object Functions
 * ============================================================================
 */

/*
 * vh_json_obj_key_count
 *
 * Returns the number of keys, including duplicates, in an object.
 */
int32_t
vh_json_obj_key_count(Json jval)
{
	JsonObjArr jobj;
	bool isa_obj;

	isa_obj = vh_json_isa_obj(jval);

	if (!isa_obj)
	{
		elog(WARNING,
				emsg("JSON element pointed to by [%p] is not a JSON Object.  "
					 "Unable to get the key count",
					 jval));

		return -1;
	}

	jobj = jval;

	return jobj->size;
}

Json
vh_json_obj_key_byname(Json jval, const char *key)
{
	Json sval = 0;
	HtpCache htpc;
	const char *pair_key;
	int32_t i;
	JsonObjArr jobj;
	bool isa_obj;

	if (!jval)
	{
		elog(WARNING, emsg("Invalid Json element pointer [%p] passed to "
					 	   "vh_json_obj_key_name.", jval));

		return 0;
	}

	isa_obj = vh_json_isa_obj(jval);

	if (!isa_obj)
	{
		elog(WARNING,
				emsg("JSON element at [%p] is not an object.  Unable to "
					 "search for the requested key name [%s].",
					 jval, key));

		return 0;
	}

	jobj = jval;
	htpc = vh_htpc_create();

	for (i = 0; i < jobj->size; i++)
	{
		pair_key = json_pair_key_name(jobj->values[i], htpc);

		if (!strcmp(pair_key, key))
		{
			sval = jobj->values[i];
			break;
		}
	}

	vh_htpc_destroy(htpc);

	return sval;
}

/*
 * vh_json_obj_key_names
 *
 * Returns an SList of const char* key names for every pair on the JSON Object.
 */
SList
vh_json_obj_key_names(Json jval, bool ignore_duplicates)
{
	SList names;
	JsonObjArr jobj;
	HtpCache htpc;
	const char *key;
	int32_t i;
	bool isa_obj;

	if (!jval)
	{
		elog(WARNING,
				emsg("Invalid Json element pointer [%p] passed to "
					 "vh_json_obj_key_names.  Unable to gather the key names.",
					 jval));

		return 0;
	}

	isa_obj = vh_json_isa_obj(jval);

	if (!isa_obj)
	{
		elog(WARNING,
				emsg("JSON element at [%p] is not an object.  Unable to "
					 "list all the key names.",
					 jval));

		return 0;
	}

	jobj = jval;
	htpc = vh_htpc_create();
	names = vh_SListCreate();

	for (i = 0; i < jobj->size; i++)
	{
		key = json_pair_key_name(jobj->values[i], htpc);

		if (key)
		{
			vh_SListPush(names, (void*)key);
		}	
	}

	/*
	 * Clean up our HeapTuple cache
	 */

	vh_htpc_destroy(htpc);

	return names;
}

/*
 * vh_json_obj_key_hash
 *
 * Create a HashTable with the key of a field name (const char*) and the value
 * being the pair for that key name.
 *
 * If there are duplicates, the last key name key will be stored if the
 * ignore_duplicate flag is set.
 *
 * This is just a stub that eventually calls into vh_json_obj_key_hash_htpc.
 */
HashTable
vh_json_obj_key_hash(Json jval, bool ignore_dup)
{
	HashTable htbl;
	HtpCache htpc;
	JsonObjArr jobj;
	bool isa_obj;


	if (!jval)
	{
		elog(WARNING,
				emsg("Invalid JSON element pointer [%p] passed to "
					 "vh_json_obj_key_hash.  Unable to insert the keys "
					 "into a new Hash table as requested.",
					 jval));

		return 0;
	}

	isa_obj = vh_json_isa_obj(jval);

	if (!isa_obj)
	{
		elog(WARNING,
				emsg("JSON element at [%p] is not a JSON Object.  Unable to "
					 "insert the keys into a new hash table as requested.",
					 jval));

		return 0;
	}

	jobj = jval;
	htpc = vh_htpc_create();

	/*
	 * Call our hashing function
	 */

	htbl = json_obj_key_hash(jobj, htpc, ignore_dup);

	/*
	 * Clean up our local HtpCache
	 */

	vh_htpc_destroy(htpc);

	return htbl;
}

HashTable
vh_json_obj_key_hash_htpc(Json jval, HtpCache htpc, bool ignore_dup)
{
	HashTable htbl;
	JsonObjArr jobj;
	bool isa_obj;


	if (!jval)
	{
		elog(WARNING,
				emsg("Invalid JSON element pointer [%p] passed to "
					 "vh_json_obj_key_hash_htpc.  Unable to insert the keys "
					 "into a new Hash table as requested.",
					 jval));

		return 0;
	}

	if (!htpc)
	{
		elog(WARNING,
				emsg("Invalid HtpCache pointer [%p] passed to "
					 "vh_json_obj_key_hash_htpc.  Unable to insert the keys "
					 "into a new hash table as requested.  Consider using "
					 "vh_json_obj_key_hash when a HtpCache is not available.",
					 htpc));

		return 0;
	}

	isa_obj = vh_json_isa_obj(jval);

	if (!isa_obj)
	{
		elog(WARNING,
				emsg("JSON element at [%p] is not a JSON Object.  Unable to "
					 "insert the keys into a new hash table as requested.",
					 jval));

		return 0;
	}

	jobj = jval;

	/*
	 * Call our hashing function
	 */

	htbl = json_obj_key_hash(jobj, htpc, ignore_dup);

	return htbl;
}

bool
vh_json_obj_add_pair(Json jobj, Json jpair)
{
	bool isa_obj, isa_pair;

	isa_obj = vh_json_isa_obj(jobj);
	isa_pair = vh_json_isa_pair(jpair);

	if (!isa_obj)
	{
		elog(WARNING,
				emsg("A JSON Object does not exist at pointer [%p], unable to "
					 "add the JSON Pair at pointer [%p].",
					 jobj,
					 jpair));

		return false;
	}

	if (!isa_pair)
	{
		elog(WARNING,
				emsg("A JSON Pair does not exist at pointer [%p], unable to "
					 "non-JSON Pair items to a JSON Object.",
					 jpair));

		return false;
	}

	json_objarr_push(jobj, jpair);

	return true;
}

/*
 * json_obj_key_hash
 *
 * Does the heavy lifting of iterating thru an object and all of it's keys to
 * form HashTable of the keys.
 */
static HashTable 
json_obj_key_hash(JsonObjArr jobj, HtpCache htpc, 
				  bool ignore_dup)
{
	HashTableOpts hopts = { };
	HashTable htbl;
	Json pair, *htblval;
	const char *key;
	int32_t i, htblret;

	hopts.key_sz = sizeof(const char*);
	hopts.value_sz = sizeof(Json);
	hopts.func_hash = vh_htbl_hash_str;
	hopts.func_compare = vh_htbl_comp_str;
	hopts.mctx = vh_mctx_current();
	hopts.is_map = true;

	htbl = vh_htbl_create(&hopts, VH_HTBL_OPT_ALL);

	for (i = 0; i < jobj->size; i++)
	{
		pair = jobj->values[i];
		key = json_pair_key_name(pair, htpc);

		if (key)
		{
			htblval = vh_htbl_put(htbl, key, &htblret);
			*htblval = pair;
		}
	}

	return htbl;
}

/*
 * json_pair_key_name
 *
 * Gets the key name off the pair, uses an HtpCache to help minimize HeapBuffer
 * thrashing.
 */
static const char* 
json_pair_key_name(Json pair, HtpCache htpc)
{
	JsonPair jp;
	JsonPairRef jpr;
	JsonPairKeyRef jpkr;
	JsonPairValueRef jpvr;
	HeapTuple ht;
	HeapField hf;
	TableField tf;
	const char *key;

	switch (json_flags(pair) & json_flags_mask_type)
	{
		case JSON_TYPE_PAIR:
			jp = pair;
			key = vh_str_buffer(&jp->name);

			break;

		case JSON_TYPE_PAIR_HTP_REF:
			jpr = pair;

			assert(jpr->htp);

			if (!vh_htpc_get(htpc, jpr->htp, &ht))
			{
				key = 0;
				break;
			}

			hf = vh_htd_field_by_idx(ht->htd, jpr->field_idx);
			tf = vh_hf_tf(hf);

			if (!tf)
			{
				key = 0;
				break;
			}

			key = vh_str_buffer(tf->fname);

			break;

		case JSON_TYPE_PAIR_HTPKEY_REF:
			jpkr = pair;

			tf = vh_hf_tf(jpkr->hf);

			if (!tf)
			{
				key = 0;
				break;
			}

			key = vh_str_buffer(tf->fname);

			break;
		
		case JSON_TYPE_PAIR_HTPVAL_REF:

			jpvr = pair;
			key = vh_str_buffer(&jpvr->name);

			break;

		default:
			
			/*
			 * Fatal error if we get here, just assert our way out.  There's
			 * no way for us to get a non JsonPair like element in an object
			 * unless we weren't an object to begin with or the tree has
			 * become corrupted.
			 */

			assert(1 == 2);
			break;
	}

	return key;
}

/*
 * ============================================================================
 * Public To String Functions
 * ============================================================================
 */
String vh_json_stringify(Json root)
{
	String str = vh_str.Create();
	HashTable htbl;

	htbl = json_stringify_create_fmt_htbl();
	json_stringify(htbl, root, str);
	json_stringify_destroy_fmt_htbl(htbl);

	return str;
}

int32_t
vh_json_stringify_to(Json root, String str)
{
	HashTable htbl;

	htbl = json_stringify_create_fmt_htbl();
	json_stringify(htbl, root, str);
	json_stringify_destroy_fmt_htbl(htbl);

	return 0;
}


/*
 * ============================================================================
 * Private Helper Functions
 * ============================================================================
 */


static HeapField 
json_htp_resolve_field_name(HeapTuplePtr htp, const char *field_name)
{
	HeapTuple ht;
	HeapField hf;

	ht = vh_htp(htp);

	if (ht)
	{
		hf = (HeapField)vh_tdv_tf_name((TableDefVer)ht->htd, field_name);

		return hf;
	}

	return 0;
}

static Json 
json_make_objarr(void)
{
	JsonObjArr joa;

	joa = vhmalloc(sizeof(struct JsonObjArrData));
	joa->capacity = 0;
	joa->size = 0;
	joa->values = 0;

	return joa;
}

static Json 
json_objarr_pop(Json jobjarr)
{
	return 0;
}

static void
json_objarr_push(Json jobjarr, Json jval)
{
	JsonObjArr joa;

	joa = jobjarr;

	if (joa->capacity <= joa->size)
	{
		if (joa->values)
		{
			/*
			 * We need to reallocate the array.
			 */
			assert(joa->capacity > 0);

			joa->capacity = joa->capacity << 1;
			joa->values = vhrealloc(joa->values, 
									sizeof(uintptr_t) * joa->capacity);
		}
		else
		{
			/*
			 * We're still in an initialized state, make sure that's the case.
			 */
			assert(joa->size == 0);
			assert(joa->capacity == 0);

			joa->values = vhmalloc(sizeof(uintptr_t) * 4);
			joa->size = 0;
			joa->capacity = 4;
		}
	}

	joa->values[joa->size] = jval;
	joa->size++;
}

static void
json_objarr_destroy(Json jval)
{
	JsonObjArr joa;
	int32_t i;

	joa = jval;

	for (i = 0; i < joa->size; i++)
	{
		vh_json_destroy(joa->values[i]);
	}

	if (joa->values)	
		vhfree(joa->values);
}

static void
json_objarr_finalize(Json jval)
{
	JsonObjArr joa;
	int32_t i;

	joa = jval;

	for (i = 0; i < joa->size; i++)
	{
		vh_json_finalize(joa->values[i]);
	}

	if (joa->values)	
		vhfree(joa->values);

	joa->size = 0;
	joa->capacity = 0;
	joa->values = 0;
}

/*
 * json_stringify_create_fmt_htbl
 *
 * Creates a HashTable to store formatters by TypeTag.
 */
static HashTable
json_stringify_create_fmt_htbl(void)
{
	HashTable htbl;
	HashTableOpts hopts = { };

	hopts.key_sz = sizeof(TypeTag);
	hopts.value_sz = sizeof(void*);
	hopts.func_hash = vh_htbl_hash_int16;
	hopts.func_compare = vh_htbl_comp_int16;
	hopts.mctx = vh_mctx_current();
	hopts.is_map = true;

	htbl = vh_htbl_create(&hopts, VH_HTBL_OPT_ALL);
	assert(htbl);

	return htbl;
}

/*
 * json_stringify_destroy_fmt_htbl
 *
 * Iterates the HashTable to call the format destructors.
 */
static void
json_stringify_destroy_fmt_htbl(HashTable htbl)
{
	vh_htbl_iterate_map(htbl, json_stringify_destroy_formats, 0);
	vh_htbl_destroy(htbl);	
}

/*
 * json_stringify_destroy_formats
 *
 * Call back for a HashTable iterator to destroy any formatters we created
 * that should be released.  We let vh_tam_cstr_format_destroy figure out
 * if a non-null pointer should actually be destroyed.  There's no sense in
 * replicating that logic here and maintaining it in both places.
 *
 * We store the TypeTag as the key, so we need to do a lookup to get the
 * actual type to call vh_tam_cstr_format_destory.  We should be very aggressive
 * here.  If the HashTable says the entry is valid, at a minimum we should
 * be able to resolve the TypeTag to a Type.
 */
static bool 
json_stringify_destroy_formats(HashTable htbl, const void *key,
							   void *entry, void *data)
{
	TypeTag *tag = (TypeTag*)key;
	Type ty = vh_type_tag(*tag);
	void **formatter = entry;

	assert(*tag);
	assert(ty);

	if (ty && entry)
		vh_tam_cstr_format_destroy(ty, *formatter);

	return true;
}


/*
 * json_stringify
 *
 * Recursive function that continously appends the str until the entire tree has
 * been traversed.
 *
 * There are some tricky parts to this.  Since we want to have multiple entry points
 * for JSON parsing, we've made a rather abstract helper function to do the
 * stringification of the data values.  This way if we aren't writing to a String,
 * we can still handle all the advanced formatters required to make this work.
 *
 * One issue that's still lingering is detecting JSON literals.  It appears the
 * most seamless way to do this would be to send in a format string that indicates
 * a literal.  On the JSON side, we would be responsible for setting by type
 * a format string when the type should be output as a literal.  The formatters 
 * returned by vh_tam_cstr_format are stored in the HashTable, with the key being
 * the TypeTag and the value being a pointer to the formatter.  This can come
 * prepopulated or json_stringify_data will populate it as it moves along.
 *
 * This way the TAM does the encoding, rather than us.
 */
static void 
json_stringify(HashTable htbl, Json jval, String str)
{
	JsonObjArr joa;
	JsonPair jp;
	JsonPairRef jpr;
	JsonPairKeyRef jpkr;
	JsonPairValueRef jpvr;
	JsonValue jv;
	JsonValueRef jvr;
	Json *pp;
	HeapTuple ht;
	HeapField hf;
	void *fval;
	Type tys[VH_TAMS_MAX_DEPTH];
	size_t buf_len, buf_cap, fld_len;
	int32_t i;
	int8_t ty_depth;

	switch (json_flags(jval) & json_flags_mask_type)
	{
		case JSON_TYPE_ARRAY:
			joa = jval;
			
			vh_str.Append(str, "[");

			for (i = 0; i < joa->size; i++)
			{
				if (i > 0)
					vh_str.Append(str, ", ");

				json_stringify(htbl, joa->values[i], str);
			}

			vh_str.Append(str, "]");

			break;

		case JSON_TYPE_OBJECT:
			joa = jval;

			vh_str.Append(str, "{");

			for (i = 0; i < joa->size; i++)
			{
				if (i > 0)
					vh_str.Append(str, ", ");

				json_stringify(htbl, joa->values[i], str);
			}

			vh_str.Append(str, "}");

			break;

		case JSON_TYPE_PAIR:
			jp = jval;

			vh_str.Append(str, "\"");
			vh_str.AppendStr(str, &jp->name);
			vh_str.Append(str, "\" : ");
			
			if (json_flags(jp) & json_flags_objarr)
			{
				pp = json_jp_json(jp);

				json_stringify(htbl, *pp, str);
			}
			else if (json_isnull(jp))
			{
				vh_str.Append(str, "null");
			}
			else
			{
				/*
				 * Stringify the TypeVar on the JSON Pair
				 */

				ty_depth = vh_typevar_fill_stack(json_jp_value(jp), tys);	
				buf_len = vh_strlen(str);
				buf_cap = vh_str_capacity(str);

				assert(buf_cap > buf_len);

				fld_len = json_stringify_data(htbl,						/* HashTable */
									tys,								/* Type stack */
									ty_depth,							/* Type depth */
									json_jp_value(jp),					/* TypeVar data */
									vh_str_buffer(str) + buf_len,		/* Buffer for data */
									buf_cap - buf_len - 1,				/* Buffer space */
									json_stringify_extend_str_buff, 	/* Extender */
									str,								/* Buffer head */
									buf_len);							/* Buffer cursor */

				if (VH_STR_IS_OOL(str))
				{
					str->varlen.size = (buf_len + fld_len) | VH_STR_FLAG_OOL;
				}
				else
				{
					str->varlen.size = buf_len + fld_len;
				}
			}

			break;

		case JSON_TYPE_PAIR_HTP_REF:

			jpr = jval;

			if (jpr->htp)
			{
				ht = vh_htp(jpr->htp);
				hf = vh_htd_field_by_idx(ht->htd, jpr->field_idx);

				vh_str.Append(str, "\"");
				vh_str.AppendStr(str, ((TableField)hf)->fname);
				vh_str.Append(str, "\" : ");

				if (vh_htf_isnull(ht, hf))
				{
					vh_str.Append(str, "null");
				}
				else
				{
					ty_depth = hf->type_depth;
					buf_len = vh_strlen(str);
					buf_cap = vh_str_capacity(str);

					assert(buf_cap > buf_len);

					fval = vh_ht_field(ht, hf);

					fld_len = json_stringify_data(htbl,						/* HashTable */
										hf->types,							/* Type stack */
										ty_depth,							/* Type depth */
										fval,								/* TypeVar data */
										vh_str_buffer(str) + buf_len,		/* Buffer for data */
										buf_cap - buf_len - 1,				/* Buffer space */
										json_stringify_extend_str_buff, 	/* Extender */
										str,								/* Buffer head */
										buf_len);							/* Buffer cursor */

					if (VH_STR_IS_OOL(str))
					{
						str->varlen.size = (buf_len + fld_len) | VH_STR_FLAG_OOL;
					}
					else
					{
						str->varlen.size = buf_len + fld_len;
					}

					/*
					 * Needs to unpin the HeapTuplePtr.
					 */
				}
			}
			else
			{
				vh_str.Append(str, "null");
			}

			break;


		case JSON_TYPE_PAIR_HTPKEY_REF:
			
			jpkr = jval;

			vh_str.Append(str, "\"");
			vh_str.AppendStr(str, ((TableField)jpkr->hf)->fname);
			vh_str.Append(str, "\" : ");

			if (json_flags(jpkr) & json_flags_objarr)
			{
				pp = json_jpkr_json(jpkr);

				json_stringify(htbl, *pp, str);
			}
			else
			{
				/*
				 * Stringify the TypeVar on the JSON Pair
				 */

				ty_depth = jpkr->hf->type_depth;;	
				buf_len = vh_strlen(str);
				buf_cap = vh_str_capacity(str);

				assert(buf_cap > buf_len);

				fld_len = json_stringify_data(htbl,						/* HashTable */
									jpkr->hf->types,					/* Type stack */
									ty_depth,							/* Type depth */
									json_jpkr_value(jpkr),				/* TypeVar data */
									vh_str_buffer(str) + buf_len,		/* Buffer for data */
									buf_cap - buf_len - 1,				/* Buffer space */
									json_stringify_extend_str_buff, 	/* Extender */
									str,								/* Buffer head */
									buf_len);							/* Buffer cursor */

				if (VH_STR_IS_OOL(str))
				{
					str->varlen.size = (buf_len + fld_len) | VH_STR_FLAG_OOL;
				}
				else
				{
					str->varlen.size = buf_len + fld_len;
				}
			}

			break;

		case JSON_TYPE_PAIR_HTPVAL_REF:

			jpvr = jval;

			vh_str.Append(str, "\"");
			vh_str.AppendStr(str, &jpvr->name);
			vh_str.Append(str, "\" : ");

			buf_len = vh_strlen(str);
			buf_cap = vh_str_capacity(str);

			assert(buf_cap > buf_len);

			if (jpvr->htp)
			{
				ht = vh_htp(jpvr->htp);
				hf = vh_htd_field_by_idx(ht->htd, jpvr->field_idx);
				/*
				 * Need to grab the field index from the HeapTuple.
				 */

				if (vh_htf_isnull(ht, hf))
				{
					vh_str.Append(str, "null");
				}
				else
				{
					fval = vh_ht_field(ht, hf);

					fld_len = json_stringify_data(htbl,						/* HashTable */
										hf->types,							/* Type stack */
										hf->type_depth,						/* Type depth */
										fval,								/* HTP Field data */
										vh_str_buffer(str) + buf_len,		/* Buffer for data */
										buf_cap - buf_len - 1,				/* Buffer space */
										json_stringify_extend_str_buff, 	/* Extender */
										str,								/* Buffer head */
										buf_len);							/* Buffer cursor */

					if (VH_STR_IS_OOL(str))
					{
						str->varlen.size = (buf_len + fld_len) | VH_STR_FLAG_OOL;
					}
					else
					{
						str->varlen.size = buf_len + fld_len;
					}
				}

				/*
				 * Needs to unpin the HeapTuplePtr.
				 */
			}
			else
			{
				vh_str.Append(str, "null");
			}

			break;

		case JSON_TYPE_VALUE:

			jv = jval;

			if (json_isnull(jv))
			{
				vh_str.Append(str, "null");
			}
			else
			{
				ty_depth = vh_typevar_fill_stack(json_jv_value(jv), tys);
				buf_len = vh_strlen(str);
				buf_cap = vh_str_capacity(str);

				assert(buf_cap > buf_len);

				fld_len = json_stringify_data(htbl,						/* HashTable */
									tys,								/* Type stack */
									ty_depth,							/* Type depth */
									json_jv_value(jv),					/* TypeVar data */
									vh_str_buffer(str) + buf_len,		/* Buffer for data */
									buf_cap - buf_len - 1,				/* Buffer space */
									json_stringify_extend_str_buff, 	/* Extender */
									str,								/* Buffer head */
									buf_len);							/* Buffer cursor */

				if (VH_STR_IS_OOL(str))
				{
					str->varlen.size = (buf_len + fld_len) | VH_STR_FLAG_OOL;
				}
				else
				{
					str->varlen.size = buf_len + fld_len;
				}
			}

			break;

		case JSON_TYPE_VALUE_HTP_REF:

			jvr = jval;

			ht = vh_htp(jvr->htp);
			hf = vh_htd_field_by_idx(ht->htd, jvr->field_idx);
			fval = vh_ht_field(ht, hf);

			ty_depth = hf->type_depth;
			buf_len = vh_strlen(str);
			buf_cap = vh_str_capacity(str);

			assert(buf_cap > buf_len);

			if (vh_htf_isnull(ht, hf))
			{
				vh_str.Append(str, "null");
			}
			else
			{
				fld_len = json_stringify_data(htbl,						/* HashTable */
									hf->types,							/* Type stack */
									hf->type_depth,						/* Type depth */
									fval,								/* HTP Field data */
									vh_str_buffer(str) + buf_len,		/* Buffer for data */
									buf_cap - buf_len - 1,				/* Buffer space */
									json_stringify_extend_str_buff, 	/* Extender */
									str,								/* Buffer head */
									buf_len);							/* Buffer cursor */

				if (VH_STR_IS_OOL(str))
				{
					str->varlen.size = (buf_len + fld_len) | VH_STR_FLAG_OOL;
				}
				else
				{
					str->varlen.size = buf_len + fld_len;
				}
			}

			break;
	}
}

/*
 * json_stringify_data
 *
 * Keeps track of the formatters setup by the lowest level type, that's the one
 * we really care about.  Since formatters must be setup ahead of time, this
 * function will do that and then push the formatter back into the HashTable
 * so that when we come across that type again, we don't have to setup another
 * formatter.
 *
 * The caller should be responsible for tearing down the formatter when it's
 * done.
 */
static size_t
json_stringify_data(HashTable htbl, Type *tys, int32_t ty_depth,
					void *data,
					char *buffer, size_t buffer_sz,
					void* (*extend_buffer)(void *buffer_head, size_t c, size_t s),
					void *buffer_head, size_t buffer_cursor)
{
	static const struct CStrAMOptionsData cstropts = { .malloc = false };

	Type ty;
	void *formatters[VH_TAMS_MAX_DEPTH], *formatter, **htblformatter;
	TamGetUnion getters[VH_TAMS_MAX_DEPTH];
	size_t tam_len, tam_cursor;
	const char *pattern;
	int32_t htblret;
	int8_t i, try_count = 0;
	bool field_set;

	/*
	 * Build our formatters out type by type into a local array so that we can
	 * shove all this down vh_tam_cstr_get's throat.
	 */
	for (i = 0; i < VH_TAMS_MAX_DEPTH; i++)
	{
		ty = tys[i];

		if (!ty)
			break;

		htblformatter = htbl ? vh_htbl_get(htbl, &ty->id) : 0;

		if (htblformatter)
		{
			formatters[i] = *htblformatter;
		}
		else
		{
			/*
			 * We get to spin a new one up.  To do this we've got to look
			 * at the type and then get a JSON format string and call 
			 * vh_tam_cstr_format which will build us a formatter we can then
			 * push back into the hash table.
			 */
			pattern = json_stringify_get_fmt(ty);
			formatter = vh_tam_cstr_format(ty, 					/* Type */
										   pattern,				/* Pattern */
										   0,					/* Patterns */
										   0);					/* Patterns count */

			if (formatter)
			{
				formatters[i] = formatter;

				if (htbl)
				{
					htblformatter = vh_htbl_put(htbl, &ty->id, &htblret);
					*htblformatter = formatter;
				}
			}
			else
			{
				/*
				 * Somehow we need to put something in the hash table to indicate
				 * this silly guy doesn't actually have a format string or a formatter
				 * so don't even bother just pass a null formatter over.
				 */
				formatters[i] = formatter;
			}
		}
	}

	/*
	 * Get our functions
	 */
	
	if (vh_tams_fill_get_funcs(tys, getters, TAM_CStr))
	{
		/*
		 * In theory, we've got all the formatters and functions we need, so 
		 * all we get to do now is call the vh_tam_cstr_get function from 
		 * the top of the stack.
		 *
		 * Since we're not asking the TAM to do a malloc, we don't care about
		 * the return value.
		 *
		 * We do care about the buffer being overrun.  This function should 
		 * probably have a way to attempt a buffer extend just to avoid setting
		 * up the type formatters all over again just to do the extend.
		 */

		tam_cursor = 0;

		while (try_count < 3)
		{
			tam_len = buffer_sz;

			vh_tam_fireu_cstr_get(tys, 
								  getters, 
								  &cstropts, 
								  data, 
								  buffer, 
								  &tam_len, 
								  &tam_cursor, 
								  formatters);

			if (tam_cursor == tam_len)
			{
				/*
				 * Indicate we were successful putting the entire field on the
				 * buffer as requested.
				 */

				field_set = true;
				break;
			}
			
			/*
			 * Try to extend the buffer.
			 */

			if (!extend_buffer)
				break;

			/*
			 * There's a chance we wrote data to the buffer so we need to account or
			 * that by adding tam_cursor to our buffer_cursor.  Otherwise we'll overwrite
			 * where we already were.
			 */

			buffer = extend_buffer(buffer_head, buffer_cursor + tam_cursor, tam_len + 1);

			if (buffer)
				buffer_sz = tam_len + 1;
			else
				break;

			try_count++;
		}
	}	

	return field_set ? tam_len : 0;
}

/*
 * json_stringify_extend_str_buff
 *
 * This guy is a little tricky, but basically we're given the head of buffer 
 * and we must reallocate it.  For this particular implementation, the head
 * is a String.
 */
static void*
json_stringify_extend_str_buff(void *buffer_head,
							   size_t cursor,
							   size_t space)
{
	String str = buffer_head;
	char *buf;

	vh_str.Resize(str, vh_strlen(str) + space);
	buf = vh_str_buffer(str);
	buf += cursor;

	return buf;
}

static const char*
json_stringify_get_fmt(Type ty)
{
	const char *fmt = 0;
	int32_t i;

	for (i = 0; ; i++)
	{
		if (!stringify_fmts[i].id)
			break;

		if (stringify_fmts[i].id == ty->id)
		{
			fmt = stringify_fmts[i].fmt;
			break;
		}
	}

	return fmt;
}

/*
 * vh_json_htp_nm
 *
 * JSON is the input we use to form a HeapTuple for the table indicated by
 * table_name.  If we cannot find the table in the default catalog, then we
 * abort.
 */
int32_t
vh_json_htp_nm(const char *table_name,
			   Json json,
			   HeapTuplePtr *htp)
{
	SearchPath sptdv = vh_sptdv_default();
	TableDefVer tdv;
	int32_t tdv_ret, htp_tdv_ret;

	tdv = vh_sp_search(sptdv, &tdv_ret, 1, VH_SP_CTX_TNAME, table_name);

	if (tdv && tdv_ret >= 0)
	{
		htp_tdv_ret = vh_json_htp_tdv(tdv, json, htp);	
	}
	else
	{
		elog(WARNING,
				emsg("A table named [%s] could not be found in the defualt catalogs.  "
					 "Ensure the table exists or provide a SearchPath with the correct "
					 "context.",
					 table_name));

		htp_tdv_ret = -1;
	}

	vh_sp_destroy(sptdv);

	return htp_tdv_ret;
}

int32_t
vh_json_htp_tdv(TableDefVer tdv,
				Json json,
				HeapTuplePtr *htp)
{
	SList fields;
	HtpCache htpc;
	HeapTuplePtr htpl;
	HeapTuple ht;
	int32_t ret;

	fields = vh_tdv_tf_filter(tdv, 0, 0, false);
	assert(fields);

	htpc = vh_htpc_create();
	htpl = vh_allochtp_td(tdv->td);
	vh_htpc_get(htpc, htpl, &ht);
	ret = json_ht(json, fields, htpc, htpl, ht);
	vh_htpc_destroy(htpc);
	vh_SListDestroy(fields);	

	*htp = htpl;

	return ret;
}


int32_t
vh_htp_json(HeapTuplePtr htp,
			bool with_ref,
			Json *json)
{
	SList fields;
	HeapTuple ht;
	HeapTupleDef htd;
	int32_t ret;

	ht = vh_htp(htp);

	if (ht)
	{
		htd = ht->htd;
		fields = vh_tdv_tf_filter((TableDefVer)htd, 0, 0, false);
		assert(fields);

		ret = ht_json(htp, ht, fields, with_ref, json);

		vh_SListDestroy(fields);

		/*
		 * Needs to release the HeapTuple
		 */
		return ret;
	}

	return -2;
}

int32_t
vh_htp_json_nm_inc(HeapTuplePtr htp,
				   const char **field_list,
				   int32_t nfields,
				   bool with_ref,
				   Json *json)
{
	SList fields;
	HeapTuple ht;
	HeapTupleDef htd;
	int32_t ret;

	ht = vh_htp(htp);

	if (ht)
	{
		htd = ht->htd;
		fields = vh_tdv_tf_name_filter((TableDefVer)htd, field_list, nfields, false);
		assert(fields);

		ret = ht_json(htp, ht, fields, with_ref, json);

		vh_SListDestroy(fields);

		/*
		 * Needs to release the HeapTuple
		 */
		return ret;
	}

	return -2;
}

/*
 * ht_json
 *
 * Takes a list of HeapTuple fields and maps them over to a new JSON
 * object.  with_ref indicates the new JSON strucures should simply
 * reference the HTP.
 *
 * Otherwise, we do a full copy using the TypeVar subsystem.
 */
static int32_t 
ht_json(HeapTuplePtr htp, HeapTuple ht, 
		SList fields, bool with_ref, Json *json)
{
	static int32_t typevar_op_flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
													  VH_OP_DT_VAR,
													  VH_OP_ID_INVALID,
													  VH_OP_DT_HTM,
													  VH_OP_ID_HF);
	Json root;
	JsonPair jp;
	HeapField *hf_head, hf;
	int32_t i, hf_sz;
	TypeVar var_target;

	hf_sz = vh_SListIterator(fields, hf_head);

	if (!hf_sz)
		return -1;

	root = vh_json_make_object();

	for (i = 0; i < hf_sz; i++)
	{
		hf = hf_head[i];

		if (with_ref)
		{
			jp = vh_json_make_pair_htp_hf(htp, hf);
			vh_json_obj_add_pair(root, jp);
		}
		else
		{
			/*
			 * Copy the values over.
			 */
			jp = vh_json_make_pair(hf->types, hf->type_depth,
								   vh_str_buffer(((TableField)hf)->fname));
			var_target = json_jp_value(jp);

			if (!vh_htf_isnull(ht, hf))
			{
				vh_typevar_op("=", typevar_op_flags, var_target, ht, hf);
			}
			else
			{
				/*
				 * We really need to set a null flag somewhere to indicate
				 * NULL was present.
				 */
			}

			vh_json_obj_add_pair(root, jp);
		}
	}

	*json = root;

	return 0;	
}

/*
 * json_ht
 *
 * Takes a JSON structure and transform it to corresponding fields on
 * the HeapTuple.  We utilize the name matching principle for this to
 * work.
 */
static int32_t 
json_ht(Json json, SList fields,
		HtpCache htpc,
		HeapTuplePtr htp, HeapTuple ht)
{
	static int32_t typevar_op_flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
													  VH_OP_DT_HTM,
													  VH_OP_ID_HF,
													  VH_OP_DT_VAR,
													  VH_OP_ID_INVALID);
	static int32_t typevar_op_ht = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
												   VH_OP_DT_HTM,
												   VH_OP_ID_HF,
												   VH_OP_DT_HTP,
												   VH_OP_ID_FI);

	HashTable htbl;
	TableField *tf_head, tf;
	Json jval, *htblval;
	JsonPair jp;
	JsonPairRef jpr;
	JsonPairKeyRef jpkr;
	JsonPairValueRef jpvr;
	TypeVar var_json;
	int32_t tf_sz, i;

	assert(ht);
	assert(htp);
	assert(vh_json_isa_obj(json));

	htbl = json_obj_key_hash((JsonObjArr)json, htpc, true);

	tf_sz = vh_SListIterator(fields, tf_head);

	for (i = 0; i < tf_sz; i++)
	{
		tf = tf_head[i];

		htblval = vh_htbl_get(htbl, vh_str_buffer(tf->fname)); 

		if (htblval)
		{
			jval = *htblval;

			assert(vh_json_isa_pair(jval));

			switch (json_flags(jval) & json_flags_mask_type)
			{
				/*
				 * Extract the TypeVar from the JSON Pair and then do the assignment.
				 */
				case JSON_TYPE_PAIR:

					jp = jval;
					var_json = json_jp_value(jp);

					vh_htf_clearnull(ht, &tf->heap);
					vh_typevar_op("=", typevar_op_flags, ht, &tf->heap, var_json);

					break;
					
				case JSON_TYPE_PAIR_HTPKEY_REF:
					
					jpkr = jval;
					var_json = json_jpkr_value(jpkr);
					
					vh_htf_clearnull(ht, &tf->heap);
					vh_typevar_op("=", typevar_op_flags, ht, &tf->heap, var_json);

					break;

				/*
				 * Assign from HeapTuplePtr to HeapTuple.  There's no sense in playing
				 * with all that when vh_typevar_op can do it for us.
				 */
				case JSON_TYPE_PAIR_HTP_REF:

					jpr = jval;

					vh_typevar_op("=", typevar_op_ht, 
								  ht, &tf->heap, 
								  jpr->htp, jpr->field_idx);

					break;

				case JSON_TYPE_PAIR_HTPVAL_REF:

					jpvr = jval;

					vh_typevar_op("=", typevar_op_ht,
								  ht, &tf->heap,
								  jpvr->htp, jpvr->field_idx);

					break;

				/*
				 * Fatal error, we shouldn't get here if our asset above worked.
				 */
				default:

					return -2;
			}
		}
	}

	vh_htbl_destroy(htbl);

	return 0;
}

bool
vh_json_tree_istype(Json root, Type ty, int32_t flags, int32_t params, ...)
{
	Json json;
	TypeVar var;
	va_list args;
	bool is_typevar;

	if (!ty)
	{
		elog(WARNING,
			 emsg("Null Type pointer [%p] passed to vh_json_tree_istype",
				  ty));

		return false;
	}

	va_start(args, params);
	json = json_tree_traverse(root, flags, params, 0, args);
	va_end(args);

	if (json)
	{
		var = vh_json_typevar(json, &is_typevar);

		if (is_typevar && var)
		{
			return vh_typevar_isa(var, ty);
		}
	}

	return false;
}

bool
vh_json_tree_istys(Json root, Type *tys, int32_t flags, int32_t params, ...)
{
	Json json;
	TypeVar var;
	va_list args;
	bool is_typevar;

	if (!tys)
	{
		elog(WARNING,
			 emsg("Null Type stack pointer [%p] passed to vh_json_tree_istype",
				  tys));

		return false;
	}

	va_start(args, params);
	json = json_tree_traverse(root, flags, params, 0, args);
	va_end(args);

	if (json)
	{
		var = vh_json_typevar(json, &is_typevar);

		if (is_typevar && var)
		{
			return vh_typevar_isatys(var, tys);
		}
	}

	return false;
}

void*
vh_json_tree_typevar(Json root, int32_t flags, int32_t params, ...)
{
	Json json;
	TypeVar var;
	va_list args;
	bool is_typevar;

	va_start(args, params);
	json = json_tree_traverse(root, flags, params, 0, args);
	va_end(args);

	if (json)
	{
		var = vh_json_typevar(json, &is_typevar);

		if (is_typevar && var)
		{
			return var;
		}
	}

	return 0;
}

void*
vh_json_tree_var_istype(Json root, Type ty, int32_t flags, int32_t params, ...)
{
	Json json;
	TypeVar var;
	va_list args;
	bool is_typevar;

	if (!ty)
	{
		elog(WARNING,
			 emsg("Null Type pointer [%p] passed to vh_json_tree_istype",
				  ty));

		return false;
	}

	va_start(args, params);
	json = json_tree_traverse(root, flags, params, 0, args);
	va_end(args);

	if (json)
	{
		var = vh_json_typevar(json, &is_typevar);

		if (is_typevar && var)
		{
			return vh_typevar_isa(var, ty) ? var : 0;
		}
	}

	return 0;
}

void*
vh_json_tree_var_istys(Json root, Type *tys, int32_t flags, int32_t params, ...)
{
	Json json;
	TypeVar var;
	va_list args;
	bool is_typevar;

	if (!tys)
	{
		elog(WARNING,
			 emsg("Null Type Stack pointer [%p] passed to vh_json_tree_var_istys",
				  tys));

		return false;
	}

	va_start(args, params);
	json = json_tree_traverse(root, flags, params, 0, args);
	va_end(args);

	if (json)
	{
		var = vh_json_typevar(json, &is_typevar);

		if (is_typevar && var)
		{
			return vh_typevar_isatys(var, tys) ? var : 0;
		}
	}

	return 0;
}

bool
vh_json_tree_tvs_istype(TypeVarSlot *tvs,
						Json root, Type ty, int32_t flags, int32_t params, ...)
{
	Json json;
	TypeVar var;
	va_list args;
	bool is_typevar;

	if (!ty)
	{
		elog(WARNING,
			 emsg("Null Type pointer [%p] passed to vh_json_tree_tvs_istype",
				  ty));

		return false;
	}

	va_start(args, params);
	json = json_tree_traverse(root, flags, params, 0, args);
	va_end(args);

	if (json)
	{
		var = vh_json_typevar(json, &is_typevar);

		if (is_typevar && var)
		{
			if (vh_typevar_isa(var, ty))
			{
				vh_tvs_store_var(tvs, var, 0);

				return true;
			}
		}
	}

	return false;
}

bool
vh_json_tree_tvs_istys(TypeVarSlot *tvs,
					   Json root, Type *tys, int32_t flags, int32_t params, ...)
{
	Json json;
	TypeVar var;
	va_list args;
	bool is_typevar;

	if (!tys)
	{
		elog(WARNING,
			 emsg("Null Type Stack pointer [%p] passed to vh_json_tree_tvs_istys",
				  tys));

		return false;
	}

	va_start(args, params);
	json = json_tree_traverse(root, flags, params, 0, args);
	va_end(args);

	if (json)
	{
		var = vh_json_typevar(json, &is_typevar);

		if (is_typevar && var)
		{
			if (vh_typevar_isatys(var, tys))
			{
				vh_tvs_store_var(tvs, var, 0);

				return true;
			}
		}
	}

	return false;
}

static Json 
json_tree_traverse(Json root, int32_t flags, int32_t params,
				   struct JsonAccessData **path, va_list args)
{
	struct JsonAccessData *jad = 0;
	Json json;
	JsonObjArr jobjarr;
	int32_t i;
	bool is_typevar;

	union
	{
		const char *key_name;
		int32_t idx;
	} pathi;


	if (path)
	{
		jad = vhmalloc(sizeof(struct JsonAccessData) * params);
	}

	json = root;

	for (i = 0; i < params; i++)
	{
		if (!json)
		{
			/*
			 * There's not a JSON element to evaluate.  We assume the caller
			 * knows the anticipated structure of the JSON and appropriately
			 * delivers the varadic arguments.  We take an aggressive approach
			 * here by deducing argument type based on the type of @json.
			 */

			break;
		}

		switch (json_flags(json) & json_flags_mask_type)
		{
			case JSON_TYPE_ARRAY:

				jobjarr = json;
				pathi.idx = va_arg(args, int32_t);
					
				if (jobjarr->size == 0)
				{
					/*
					 * We either need to throw an error, quietly return or push
					 * a new element in here.
					 */
				}

				if (pathi.idx == -1)
				{
					/*
					 * Sentinel value to go to the end of the array
					 */


					json = jobjarr->values[jobjarr->size - 1];
				}
				else
				{
					/*
					 * Boundary checking
					 */

					if (pathi.idx > jobjarr->size ||
						pathi.idx < 0)
					{
						/*
						 * Throw an error or abort quietly depending on the flag
						 */
					}

					json = jobjarr->values[pathi.idx];
				}

				if (jad)
				{
					/*
					 * Update our JSON Access structure if requested by the caller.
					 */

					jad[i].json = json;

					jad[i].flags = JSON_ACCESS_ARRAY;
					jad[i].idx = pathi.idx;
				}

				break;

			case JSON_TYPE_OBJECT:

				pathi.key_name = va_arg(args, const char*);

				if (pathi.key_name)
				{
					json = vh_json_obj_key_byname(json, pathi.key_name);

					/*
					 * We've simply got the pair at this point, so we need to
					 * deduce if we've got a an object/array as the pair value
					 * or a scalar value.
					 *
					 * If we're the last traversal in the tree, we can skip this
					 * logic to produce a non-scalar value from a JsonPair.
					 */

					if (i + 1 < params)
					{
						json = vh_json_typevar(json, &is_typevar);

						if (is_typevar)
						{
							/*
							 * We've got a scalar value on the JSON pair resolved
							 * at @key_name.  Since we're expecting a non-scalar
							 * JSON Array or Object, we've got to throw an error.
							 */ 
						}
					}
				}
				
				if (jad)
				{
					/*
					 * Update our JSON Access structure if requested by the caller.
					 */

					jad[i].json = json;

					jad[i].flags = JSON_ACCESS_KEYNAME;
					jad[i].idx = pathi.idx;
				}
				
				break;

			default:

				/*
				 * We've been given a scalar value and we really can't go any
				 * further into the tree with it.  Set @json to null and figure
				 * out if we should throw an error based on the flags passed in.
				 */

				json = 0;

				break;
		}
	}

	if (i == params)
	{
		/*
		 * It's possible we can't make it far enough down the tree given the
		 * requested indices/key names from the user.  Handle this case by simply
		 * making sure i got iterated all the way out to the number of parameters.
		 */

		return json;
	}

	return 0;
}

