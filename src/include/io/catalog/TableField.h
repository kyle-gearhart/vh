/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_TableField_H
#define vh_datacatalog_TableField_H

#include "io/catalog/HeapField.h"

typedef struct HeapTupleData HeapTupleData, *HeapTuple;
typedef struct KeyGenData KeyGenData, *KeyGen;
typedef struct TableDefData TableDefData, *TableDef;

struct TableFieldBackEndOpts;

typedef struct TableFieldData
{
	HeapFieldData heap;
	TableDefVer tdv;
	TableDef related;
	KeyGen keygen;
	String fname;
	struct TypeAMFuncs *tam;
	struct TableFieldBackEndOpts *beopts;
	uint32_t n_be_opts;
	uint32_t relatedidx;
	uint8_t id;
	uint8_t db;
	uint8_t beacon_key;
	uint8_t modifyable;
} TableFieldData, *TableField;

TableField vh_tf_create(void);
void vh_tf_finalize(TableField tf);

/*
 * BackEnd Options
 */
void vh_tf_beopt_format_clear(TableField tf, BackEnd be);
void vh_tf_beopt_format_set(TableField tf, BackEnd be, const char *format);
bool vh_tf_beopt_format(TableField tf, BackEnd be, const char **dest_format);

struct TypeAMFuncs* vh_tf_be_has_tam_funcs(TableField tf, BackEnd be);
struct TypeAMFuncs* vh_tf_be_tam_funcs(TableField tf, BackEnd be);

/*
 * TAM Functions
 */

#define vh_tf_has_tam_funcs(tf)		( vh_hf_is_tablefield(tf) ? ((TableField)(tf))->tam : 0 )
struct TypeAMFuncs* vh_tf_tam_funcs(TableField tf);

#endif

