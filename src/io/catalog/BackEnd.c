/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/tam.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/utils/htbl.h"
#include "io/utils/SList.h"

/*
 * For the NativeType mapping, we'll take a hash of the native name.  Otherwise,
 * we'd have to copy strings because the const char* passed is not guaranteed to
 * have the same lifetime as the BackEnd (BackEnds are forced into the top level
 * memory context).
 */
typedef struct BackEndNativeTypeData
{
	Type tys[VH_TAMS_MAX_DEPTH];
	char name[1];
} *BackEndNativeType;

struct IterateNativeType
{
	Type *search;
	BackEndNativeType bent;
};

static bool be_iter_native_type(HashTable htbl, const void *key, void *entry,
								void *data);


/*
 * Back End Credentials
 */

struct BackEndCredentialData
{
	BackEndCredentialVal val;
};

void*
vh_be_conn_create(BackEnd be, size_t sz)
{
	BackEndConnection conn;

	assert(sz > sizeof(struct BackEndConnectionData));

	conn = vhmalloc(sz);
	vh_be_conn_init(conn);
	
	conn->be = be;

	return conn;
}

void
vh_be_conn_init(BackEndConnection bec)
{
	bec->be = 0;
	bec->currentdb = 0;
	bec->intx = false;
	bec->in2pc = false;
}


struct TypeAMFuncs* 
vh_be_type_getam(BackEnd be, Type ty)
{
	TypeAMFuncs *tamf;

	if (be->type_am)
	{
		tamf = vh_htbl_get(be->type_am, &ty->id);

		return tamf;
	}

	return 0;
}

bool
vh_be_type_hasam(BackEnd be, Type ty)
{
	return vh_be_type_getam(be, ty) != 0;
}

bool
vh_be_type_setam(BackEnd be, Type ty, TypeAMFuncs tyam)
{	
	HashTableOpts hopts = { };
	MemoryContext mtop;
	TypeAMFuncs *at;
	int32_t ret;

	mtop = vh_mctx_top();

	if (!be->type_am)
	{
		hopts.key_sz = sizeof(TypeTag);
		hopts.value_sz = sizeof(TypeAMFuncs);
		hopts.func_hash = vh_htbl_hash_int16;
		hopts.func_compare = vh_htbl_comp_int16;
		hopts.mctx = mtop;
		hopts.is_map = true;

		be->type_am = vh_htbl_create(&hopts, VH_HTBL_OPT_ALL);

		at = vh_htbl_put(be->type_am, &ty->id, &ret);
		assert(ret == 1 || ret == 2);

		memcpy(at, &tyam, sizeof(TypeAMFuncs));

		return true;
	}
	else
	{
		at = vh_htbl_get(be->type_am, &ty->id);

		if (!at)
		{
			at = vh_htbl_put(be->type_am, &ty->id, &ret);
			assert(ret == 1 || ret == 2);

			memcpy(at, &tyam, sizeof(TypeAMFuncs));

			return true;
		}
	}

	return false;
}

Type*
vh_be_type_getnative(BackEnd be, const char* nnm)
{	
	BackEndNativeType *search;
   
	if (be->native_types)
	{
		search = vh_htbl_get(be->native_types, nnm);

		if (search)
		{
			return &((*search)->tys[0]);
		}
	}

	return 0;
}

/*
 * vh_be_type_getbe
 *
 * We're given a Type stack that we need to find the text name for the type on
 * the backend.  Since our hash table is only one way (i.e. on the name) and
 * we don't expect to do this only for the CREATE TABLE commands, we'll just
 * iterate the hash table to find it.
 */
const char*
vh_be_type_getbe(BackEnd be, Type *tys)
{
	struct IterateNativeType nt = { };

	if (be->native_types)
	{
		nt.search = tys;

		vh_htbl_iterate_map(be->native_types,
							be_iter_native_type,
							&nt);

		if (nt.bent)
		{
			return &nt.bent->name[0];
		}
	}
	
	return 0;
}

bool
vh_be_type_setnative(BackEnd be, const char* nnm, Type *typestack)
{
	MemoryContext mtop;
	HashTableOpts hopts = { };
	BackEndNativeType *search, bent;
	char *namebuf;
	int32_t ret;

	mtop = vh_mctx_top();

	if (!be->native_types)
	{
		hopts.key_sz = sizeof(const char*);
		hopts.value_sz = sizeof(BackEndNativeType);
		hopts.func_hash = vh_htbl_hash_str;
		hopts.func_compare = vh_htbl_comp_str;
		hopts.mctx = mtop;
		hopts.is_map = true;

		be->native_types = vh_htbl_create(&hopts, VH_HTBL_OPT_ALL);

		bent = vhmalloc_ctx(mtop, sizeof(struct BackEndNativeTypeData) + strlen(nnm));
		namebuf = &bent->name[0];

		vh_type_stack_copy(bent->tys, typestack);
		strcpy(namebuf, nnm);

		search = vh_htbl_put(be->native_types, namebuf, &ret);
		assert(ret == 1 || ret == 2);

		*search = bent;

		return true;
	}
	else
	{
		search = vh_htbl_get(be->native_types, nnm);

		if (search)
		{
			elog(WARNING,
					emsg("Type %s has already been assigned to the BackEnd %s",
						 nnm,
						 be->name));
			return false;
		}

		bent = vhmalloc_ctx(mtop, sizeof(struct BackEndNativeTypeData) + strlen(nnm));
		namebuf = &bent->name[0];

		vh_type_stack_copy(bent->tys, typestack);
		strcpy(namebuf, nnm);

		search = vh_htbl_put(be->native_types, namebuf, &ret);
		assert(ret == 1 || ret == 2);

		if (search)
		{
			*search = bent;

			return true;
		}
	}

	return false;
}


BackEndCredential
vh_be_cred_create(BackEndCredStorageMode storagemode)
{
	BackEndCredential cred = vhmalloc(sizeof(struct BackEndCredentialData));

	memset(cred, 0, sizeof(struct BackEndCredentialData));

	return cred;
};

bool
vh_be_cred_store(BackEndCredential becred, BackEndCredentialVal *input)
{
	becred->val = *input;

	return true;
}

BackEndCredentialVal
vh_be_cred_retrieve(BackEndCredential becred)
{
	return becred->val;
}

/*
 * be_iter_native_type
 *
 * HashTable iterator to find the BackEndNativeType entry via the type stack.
 */
static bool
be_iter_native_type(HashTable htbl, const void *key, void *entry, void *data)
{
	struct IterateNativeType *nt = data;
	BackEndNativeType *bent = entry;

	if (vh_type_stack_match(nt->search, (*bent)->tys))
	{
		nt->bent = *bent;
		return false;
	}

	return true;
}

/*
 * ============================================================================
 * BackEnd Connection Function
 * ============================================================================
 */

BackEndConnection
vh_be_connect(BackEnd be, BackEndCredential becred)
{
	BackEndCredentialVal becval = { };
	BackEndConnection bec;
	bool res;
	
	if (be && 
		be->at.connect)
	{
		becval = vh_be_cred_retrieve(becred);
		
		if (be->at.createconn)
		{
			bec = be->at.createconn();
		}
		else
		{
			bec = vh_be_conn_create(be, sizeof(struct BackEndConnectionData));
			vh_be_conn_init(&bec);
		}


		res = be->at.connect(bec, &becval);
		vh_be_cred_wipe(becval);

		if (!res)
		{
			if (be->at.freeconn)
			{
				be->at.freeconn(bec);
			}

			return 0;
		}

		return bec;
	}

	elog(ERROR,
		 emsg("Invalid BackEnd pointer [%p] or corrupt function table for the BackEnd "
			  "interface",
			  be));

	return 0;
}

bool
vh_be_disconnect(BackEndConnection bec)
{
	BackEnd be;
	bool res;

	be = bec->be;

	if (be && be->at.disconnect)
	{
		res = be->at.disconnect(bec);

		if (res)
		{
			if (be->at.freeconn)
			{
				be->at.freeconn(bec);
			}
			else
			{
				vhfree(bec);	
			}
		}

		return res;
	}

	return false;
}

/*
 * ============================================================================
 * BackEnd Function Table Stubs
 * ============================================================================
 */

bool
vh_be_exec(BackEndConnection bec, BackEndExecPlan beep)
{
	BackEnd be;
	bool error = false;

	assert(bec);
	assert(beep);

	be = bec->be;

	if (be->at.exec)
	{
		VH_TRY();
		{
			be->at.exec(beep);
		}
		VH_CATCH();
		{
			error = true;
		}
		VH_ENDTRY();
	}
	else
	{
		elog(WARNING,
				emsg("BackEnd [%s / %p] does not have an exec function implemented!",
					 be->name,
					 be));
		error = true;
	}

	return error;
}

bool
vh_be_xact_begin(BackEndConnection bec)
{
	BackEnd be = bec->be;
	bool success = true;

	assert(bec);
	assert(be);

	if (be->at.xactbegin)
	{
		if (vh_be_conn_intx(bec))
		{
			elog(WARNING,
					emsg("BackEndConnection [%p] is already in a transaction.  "
						 "Will not attempt to open another transaction.",
						 bec));
		}
		else
		{
			success = be->at.xactbegin(bec);
			bec->intx = success;
		}
	}

	return success;
}

bool
vh_be_xact_commit(BackEndConnection bec)
{
	BackEnd be = bec->be;
	bool success = true;

	assert(bec);
	assert(be);

	if (be->at.xactcommit)
	{
		if (vh_be_conn_intx(bec))
		{
			success = be->at.xactcommit(bec);
			bec->intx = !success;
		}
		else
		{
			elog(WARNING,
					emsg("BackEndConnection [%p] does not have an open transaction.  "
						 "Cannot commit",
						 bec));
			success = false;
		}
	}

	return success;
}

bool
vh_be_xact_rollback(BackEndConnection bec)
{
	BackEnd be = bec->be;
	bool success = true;

	assert(bec);
	assert(be);

	if (be->at.xactrollback)
	{
		if (vh_be_conn_intx(bec))
		{
			success = be->at.xactrollback(bec);
			bec->intx = !success;
		}
		else
		{
			elog(WARNING,
					emsg("BackEndConnection [%p] does not have an open transaction.  "
						 "Cannot commit",
						 bec));
			success = false;
		}
	}

	return success;
}

bool
vh_be_command(BackEnd be, Node node, String *cmd,
			  int32_t param_offset, TypeVarSlot **param_values, int32_t *p_count)
{
	String str;

	if (be->at.command)
	{
		str = be->at.command(node, param_offset, param_values, p_count);
		
		*cmd = str;

		return str ? true : false;	
	}

	return false;
}

bool
vh_be_param(BackEnd be, ParameterList pl, TypeVarSlot *tvs, Parameter *out)
{
	Parameter param;
	Type tys[VH_TAMS_MAX_DEPTH];
	TamUnion tam_funcs[VH_TAMS_MAX_DEPTH];
	void *tam_formats[VH_TAMS_MAX_DEPTH], *tvs_val;
	int8_t ty_depth;
	bool is_null;

	if (be->at.param)
	{
		is_null = vh_tvs_isnull(tvs);

		if (is_null)
		{
			param = be->at.param(pl, 0, 0, 0, 0, true);
			*out = param;

			return true;
		}

		tvs_val = vh_tvs_value(tvs);
		ty_depth = vh_tvs_fill_tys(tvs, tys);
		assert(ty_depth > 0 && ty_depth <= VH_TAMS_MAX_DEPTH);

		vh_tam_be_fill_get(be->tam,
								be,
								tys,
								ty_depth,
								tam_funcs,
								tam_formats);

		param = be->at.param(pl,
							 tys,
							 &tam_funcs[0].get,
							 tam_formats,
							 tvs_val,
							 is_null);

		*out = param;

		return true;
	}

	return false;
}

