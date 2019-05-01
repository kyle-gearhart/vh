/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_vh_h
#define vh_datacatalog_vh_h

#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

typedef int8_t bool;
enum { false, true };


typedef struct BackEndData *BackEnd;
typedef struct BackEndConnectionData *BackEndConnection;
typedef struct BackEndCatalogData *BackEndCatalog;
typedef struct BeaconData *Beacon;
typedef struct CatalogContextData *CatalogContext;
typedef struct ConnectionCatalogData *ConnectionCatalog;
typedef struct HeapFieldData *HeapField;
typedef struct HeapTupleData *HeapTuple;
typedef struct HeapTupleDefData *HeapTupleDef;
typedef struct HtpCacheData *HtpCache;
typedef struct ShardData *Shard;
typedef struct TableCatalogData *TableCatalog;
typedef struct TableDefData *TableDef;
typedef struct TableDefVerData *TableDefVer;
typedef struct TableFieldData *TableField;
typedef struct TableKey TableKey;
typedef struct TableRelData *TableRel;
typedef struct TableRelQualData *TableRelQual;
typedef const struct TypeData* Type;
typedef struct TypeBackEndAMData *TypeBackEndAM;


/*
 * Catalog declares
 */
typedef struct BeaconCatalogData *BeaconCatalog;
typedef struct TypeCatalogData *TypeCatalog;
typedef struct XActData *XAct;

/*
 * Executor declares
 */
typedef struct ExecResultData *ExecResult;



/*
 * HeapTuplePtr and associated declares
 */
typedef uint32_t BufferBlockNo;
typedef uint8_t HeapBufferNo;
typedef uint64_t HeapTuplePtr;


/*
 * Node declares
 */
typedef struct NodeData *Node;
typedef struct NodeQueryData *NodeQuery;


/*
 * Utility declares
 */
typedef struct HashTableData *HashTable;
typedef HashTable KeySet;
typedef struct KeyValueListData *KeyValueList;
typedef HashTable KeyValueMap;
typedef struct SListData *SList;

typedef struct TypeVarOpExecData *TypeVarOpExec;
typedef struct TypeVarSlot TypeVarSlot;

/*
 * Include Memory.h before EQueue.h since it depends on definitions
 * in Memory.h
 */

#include "io/utils/mmgr/Memory.h"
#include "io/utils/EQueue.h"

#include "io/buffer/HeapBuffer.h"
#include "io/catalog/types/String.h"
#include "io/catalog/CatalogContext.h"




HeapTuplePtr vh_allochtp_td(TableDef td);
HeapTuplePtr vh_allochtp_nm(const char *table_name);
HeapTuplePtr vh_allochtp_sp(void *search_path, const char *table_name);
void vh_destroyhtp(HeapTuplePtr htp);

bool vh_sync(SList htps);
bool vh_sync_array(SList *htps, uint32_t nhtps);
bool vh_sync_htp(HeapTuplePtr htp);

#define VH_TAMS_MAX_DEPTH		8


/*
 * http://stackoverflow.com/questions/3868289/count-number-of-parameters-in-c-variable-argument-method-call
 */

#define PP_NARG(...) \
	         PP_NARG_(__VA_ARGS__,PP_RSEQ_N())
#define PP_NARG_(...) \
	         PP_ARG_N(__VA_ARGS__)
#define PP_ARG_N( \
		          _1, _2, _3, _4, _5, _6, _7, _8, _9,_10, \
		         _11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
		         _21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
		         _31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
		         _41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
		         _51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
		         _61,_62,_63,N,...) N
#define PP_RSEQ_N() \
	         63,62,61,60,                   \
         59,58,57,56,55,54,53,52,51,50, \
         49,48,47,46,45,44,43,42,41,40, \
         39,38,37,36,35,34,33,32,31,30, \
         29,28,27,26,25,24,23,22,21,20, \
         19,18,17,16,15,14,13,12,11,10, \
         9,8,7,6,5,4,3,2,1,0 

#endif


