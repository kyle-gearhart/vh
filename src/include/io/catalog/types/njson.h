/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_catalog_types_njson_H
#define vh_io_catalog_types_njson_H

typedef struct TypeVarSlot TypeVarSlot;

/*
 * New JSON Structures
 *
 * A completely new set of JSON structures designed to be tightly coupled with
 * the existing HeapTuple and TypeVar infrastructure.
 */

typedef void *Json;

/*
 * vh_json_isa_*
 *
 * Returns TRUE if the Json structure represents the requested JSON type.
 */
bool vh_json_isa_obj(Json j);
bool vh_json_isa_array(Json j);
bool vh_json_isa_pair(Json j);
bool vh_json_isa_value(Json j);


/* vh_json_make_array	Makes an empty JSON array */
Json vh_json_make_array(void);

/*
 * vh_json_make_pair_*
 *
 * Pair is our most complicated set of make, simply because it has the most
 * implementations.  The most basic implementation is the vh_json_make_pair
 * routine, which allows users to assign the key value and the data value.
 *
 * JSON pairs may also reference a HeapTuplePtr.  There are three ways a 
 * HeapTuple pointer may be referenced:
 * 		1)	Key name reference and value reference		(htp)
 * 		2)	Key name reference and user defined value	(krh)
 * 		3)	User defined key name and value reference	(vrh)
 *
 * JSON pairs can also reference a JSON Array or JSON Object as its value.
 * 		1)	Key name references a JSON Array 			(arr)
 * 		2)	Key name refernces a JSON Object			(obj)
 */
Json vh_json_make_pair(Type *tys, int32_t ty_depth, const char *name);
Json vh_json_make_pair_htp_nm(HeapTuplePtr htp, const char *field_name);
Json vh_json_make_pair_htp_hf(HeapTuplePtr htp, HeapField hf);
Json vh_json_make_pair_krh_nm(HeapTuplePtr htp, const char *field_name);
Json vh_json_make_pair_krh_hf(HeapTuplePtr htp, HeapField hf);
Json vh_json_make_pair_vrh_nm(HeapTuplePtr htp, const char *htp_field, const char *key_name);
Json vh_json_make_pair_vrh_hf(HeapTuplePtr htp, HeapField hf, const char *key_name);
Json vh_json_make_pair_obj(const char *key_name);
Json vh_json_make_pair_arr(const char *key_name);
Json vh_json_make_pair_objarr(const char *key_name, Json jarr_or_obj);
Json vh_json_make_pair_null(const char *key_name);
Json vh_json_make_pair_tvs(const char *key_name, TypeVarSlot *slot);

Json vh_json_make_pair_bool(const char *name, bool value);
Json vh_json_make_pair_cstr(const char *name, const char *value);
Json vh_json_make_pair_i32(const char *name, int32_t value);

Json vh_json_make_object(void);

Json vh_json_make_value(Type *tys, int32_t ty_depth);
Json vh_json_make_value_htp_nm(HeapTuplePtr htp, const char *field_name);
Json vh_json_make_value_htp_hf(HeapTuplePtr htp, HeapField hf);
Json vh_json_make_value_null(void);

/*
 * Cleanup Routines
 *
 * 	_finalize		Destructs the underlying data type, if applicable
 * 	_free			Frees the space allocated to contain the JSON structure
 * 	_destroy		Finalize followed by free
 */

void vh_json_finalize(Json jval);
void vh_json_free(Json jval);
void vh_json_destroy(Json jval);



/*
 * vh_json_value
 *
 * Returns a TypeVar if the underlying JSON type stores a user editable value.
 * NOTE: some JSON types internally reference a HeapTuple for their value.  These
 * are only references on the JSON side of the house, so there is no value to
 * maintain.
 */
HeapTuplePtr vh_json_htp(Json jval);
void* vh_json_objarr(Json jval, bool *is_objarr);
void* vh_json_typevar(Json jval, bool *is_typevar);

/*
 * vh_json_arr_*
 *
 * Functions for reading and manipulating an array type.
 */
int32_t vh_json_arr_count(Json jval);
Json vh_json_arr_atidx(Json jval, int32_t idx);
bool vh_json_arr_push(Json jarray, Json jval);

/*
 * vh_json_obj_*
 *
 * Functions for reading and manipulating an object type.
 */

int32_t vh_json_obj_key_count(Json jval);
SList vh_json_obj_key_names(Json jval, bool ignore_duplicates);
HashTable vh_json_obj_key_hash(Json jval, bool ignore_duplicate);
HashTable vh_json_obj_key_hash_htpc(Json jval, HtpCache htpc, bool ignore_dup);

/*
 * Returns the JsonPair at a specific index or key name
 */

Json vh_json_obj_key_atidx(Json jval, uint32_t idx);
Json vh_json_obj_key_byname(Json jval, const char *key);

bool vh_json_obj_add_pair(Json jobj, Json jpair);


/*
 * To String Routines
 */

String vh_json_stringify(Json root);
int32_t vh_json_stringify_to(Json root, String str);


/*
 * HeapTuple facilitators
 *
 * json_htp 	JSON >> HTP
 * htp_json		HTP >> JSON
 *
 */

int32_t vh_json_htp_nm(const char *table_name,
					   Json json,
   					   HeapTuplePtr *htp);
int32_t vh_json_htp_tdv(TableDefVer tdv,
						Json json,
						HeapTuplePtr *htp);

/*
 * vh_htp_json
 *
 * Turns a HeapTuplePtr into a JSON structure, with or without reference.
 *
 * When reference is true, the JSON values cannot be modified directly.  Instead
 * the HeapTuple itself must be changed and the JSON will automatically update
 * its value.
 */
int32_t vh_htp_json(HeapTuplePtr htp,
					bool with_ref,
					Json *json);

/*
 * vh_htp_json_nm_inc
 *
 * Turns a HeapTuplePtr into a JSON structure, but only specific field names are
 * included in the JSON structure.
 */
int32_t vh_htp_json_nm_inc(HeapTuplePtr htp,
						   const char **field_list,
						   int32_t nfields,
						   bool with_ref,
						   Json *json);

int32_t vh_htp_json_hf_inc(HeapTuplePtr htp,
						   HeapField *field_list,
						   int32_t nfields,
						   bool with_ref,
						   Json *json);

/*
 * vh_htp_json_nm_exc
 *
 * Turns a HeapTuplePtr into a JSON structure, but specific field names are
 * excluded from the JSON structure.
 */

int32_t vh_htp_json_nm_exc(HeapTuplePtr htp,
						   const char **field_list,
						   int32_t nfields,
						   bool with_ref,
						   Json *json);


/*
 * JSON Tree Access
 *
 * We allow callers to specify the path to an element using a vardic function
 * call.  These can be helpful when validating a complex JSON element.
 */

#define VH_JSON_TREE_THROW				0x01
#define VH_JSON_TREE_WARN				0x02

bool vh_json_tree_istype(Json root, Type ty, int32_t flags, int32_t params, ...);
bool vh_json_tree_istys(Json root, Type *tys, int32_t flags, int32_t params, ...);

#define vh_jsont_isa(json, ty, flags, ...)										\
	vh_json_tree_istype((json), (ty), (flags), PP_NARG(...), __VA_ARGS__)

#define vh_jsont_istys(json, tys, flags, ...)									\
	vh_json_tree_istys((json), (tys), (flags), PP_NARG(...), __VA_ARGS__)


/*
 * Navigate a JSON tree for a scalar value and return the TypeVar representing the
 * scalar value.
 */
void* vh_json_tree_typevar(Json root, int32_t flags, int32_t params, ...);

/*
 * Navigate a JSON tree for a scalar value, validating the scalar value matches the
 * Type or Type stack specified.  Returns the TypeVar when the tree is succesfully
 * validated and the scalar value is of the expected Type or Type stack.  Otherwise
 * it will return null on a tree traversal error or if the scalar value doesn't match
 * the requested Type or Type stack.
 */
void* vh_json_tree_var_istype(Json root, Type ty, int32_t flags, int32_t params, ...);
void* vh_json_tree_var_istys(Json root, Type *tys, int32_t flags, int32_t params, ...);

#define vh_jsonv_isa(json, ty, flags, ...)										\
	vh_json_tree_var_istype((json), (ty), (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsonv_isatys(json, tys, flags, ...)									\
	vh_json_tree_var_istys((json), (tys), (flags), PP_NARG(...), __VA_ARGS__)


#define vh_jsonv_bool(json, flags, ...)											\
	vh_json_tree_var_istype((json), &vh_type_bool, (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsonv_int32(json, flags, ...)										\
	vh_json_tree_var_istype((json), &vh_type_int32, (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsonv_int64(json, flags, ...)										\
	vh_json_tree_var_istype((json), &vh_type_int64, (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsonv_dbl(json, flags, ...)											\
	vh_json_tree_var_istype((json), &vh_type_dbl, (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsonv_String(json, flags, ...)										\
	vh_json_tree_var_istype((json), &vh_type_String, (flags), PP_NARG(...), __VA_ARGS__)


/*
 * Navigate a JSON tree to a scalar value and validate the value is the desired
 * Type or Type Stack.  Then, store the scalar value's TypeVar in a TypeVarSlot.
 *
 * Returns true when the scalar value was of the desired Type or Type stack and
 * was stored in the TypeVarSlot pointed to by @tvs.
 */
bool vh_json_tree_tvs_istype(TypeVarSlot *tvs, 
							 Json root, Type ty, int32_t flags, int32_t params, ...);
bool vh_json_tree_tvs_istys(TypeVarSlot *tvs, 
							Json root, Type *tys, int32_t flags, int32_t params, ...);

#define vh_jsons_bool(json, flags, ...)											\
	vh_json_tree_tvs_istype((json), &vh_type_bool, (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsons_int32(json, flags, ...)										\
	vh_json_tree_tvs_istype((json), &vh_type_int32, (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsons_int64(json, flags, ...)										\
	vh_json_tree_tvs_istype((json), &vh_type_int64, (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsons_dbl(json, flags, ...)											\
	vh_json_tree_tvs_istype((json), &vh_type_dbl, (flags), PP_NARG(...), __VA_ARGS__)
#define vh_jsons_String(json, flags, ...)										\
	vh_json_tree_tvs_istype((json), &vh_type_String, (flags), PP_NARG(...), __VA_ARGS__)

#endif

