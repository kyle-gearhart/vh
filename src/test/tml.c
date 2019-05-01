/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"

#include "io/analytics/ml/ml.h"
#include "io/analytics/ml/mlknn.h"
#include "io/analytics/ml/mlnaive_bayes.h"

#include "io/be/postgres/Postgres.h"
#include "io/buffer/HeapBuffer.h"
#include "io/catalog/BackEndCatalog.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/PrepTup.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/sp/spht.h"
#include "io/catalog/types/Array.h"
#include "io/catalog/types/Date.h"
#include "io/catalog/types/DateTime.h"
#include "io/executor/eresult.h"
#include "io/executor/exec.h"
#include "io/executor/xact.h"
#include "io/nodes/NodeFrom.h"
#include "io/nodes/NodeQueryInsert.h"
#include "io/nodes/NodeQuerySelect.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/shard/Shard.h"
#include "io/sql/InfoScheme.h"
#include "io/utils/SList.h"
#include "io/utils/stopwatch.h"

#include "test.h"

/*
 * Machine Learning Test Suite
 *
 * Instead of reading a bunch of CSV files, we'll depend on those to be 
 * already loaded into a Postgres instance.  We can use the query executor
 * to select everything from a table and then go from there.
 *
 * Ulimately we want to check to make sure the alogorithms themselves are
 * performing correctly.  A 100% or 0% predication rate should be alarming.
 */

static Shard shard = 0;

static void setup_data_connection(void);
static SearchPath setup_sp(const char *field_name);

/*
 * ============================================================================
 * KNN Testing
 * ============================================================================
 */
static void test_ml_knn_abalone(void);

void
test_ml_entry(void)
{
	printf("\n================================================================="
		   "\nMachine Learning Test Suite"
		   "\n================================================================="
		   "\n");

	printf("\t1)\tSetting up connection to data source...");
	setup_data_connection();
	printf("done!\n");

	printf("\t2)\tAttempting KNN testing...");
	test_ml_knn_abalone();
	printf("done!\n");
}


static void
setup_data_connection(void)
{
	BackEndCredentialVal becredval = { };
	BackEndCredential becred;
	ShardAccess sa;
	BackEnd be;

	strcpy(&becredval.username[0], "postgres");
	strcpy(&becredval.password[0], "^N[D.:;vH<73aq7:");
	strcpy(&becredval.hostname[0], "127.0.0.1");
	strcpy(&becredval.hostport[0], "5432");

	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_USERNAME);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_PASSWORD);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_HOSTNAME);
	vh_be_credval_member_set(becredval, VH_BE_CREDVAL_HOSTPORT);

	becred = vh_be_cred_create(BECSM_PlainText);
	vh_be_cred_store(becred, &becredval);

	be = vh_cat_be_getbyname(ctx_catalog->catalogBackEnd, "Postgres");
	sa = vh_sharda_create(becred, be);
	sa->database = vh_str.Convert("vh_test");

	shard = vh_shard_create((ShardId){}, sa, sa); 
}

static SearchPath
setup_sp(const char *field_name)
{
	SearchPath sp = vh_spht_tf_create(field_name);

	return sp;
}

static void
test_ml_knn_abalone(void)
{
	union
	{
		PrepCol pc;
		MachLearn ml;
	} ml;

	SearchPath class;
	SearchPath paths[8];
	SList set_train, set_test;
	BackEndConnection bec;
	ExecResult er;
	HeapTuplePtr htp;
	float test;
	int32_t i = 0;

	class = setup_sp("a");

	paths[0] = setup_sp("b");
	paths[1] = setup_sp("c");
	paths[2] = setup_sp("d");
	paths[3] = setup_sp("e");
	paths[4] = setup_sp("f");
	paths[5] = setup_sp("g");
	paths[6] = setup_sp("h");
	paths[7] = setup_sp("i");

	ml.ml = vh_ml_knn_create(paths, 8, 
							 class, 
							 vh_mlf_distance_euclidean, KNN_Classification);
	assert(ml.ml);

	bec = vh_ConnectionGet(ctx_catalog->catalogConnection, shard->access[0]);
	er = vh_exec_query_str(bec, "SELECT * FROM ml_abalone");
	vh_ConnectionReturn(ctx_catalog->catalogConnection, bec);

	/*
	 * Fill up our training sets and testing sets
	 */
	vh_htp_SListCreate(set_train);
	vh_htp_SListCreate(set_test);

	if (vh_exec_result_iter_first(er))
	{
		do
		{
			htp = vh_exec_result_iter_htp(er, 0);

			if (i % 3 == 0)
				vh_htp_SListPush(set_test, htp);
			else
				vh_htp_SListPush(set_train, htp);

			i++;
		} while (vh_exec_result_iter_next(er));
	}

	printf("\n\t\tKNN Abalone Total Records: %d", i);
	//test = vh_ml_test_classification(ml.ml, set_train, set_test, class);
	printf("\n\t\tKNN Abalone Prediction Rate: %f\n", test);

	ml.ml = vh_ml_naive_bayes_create(paths, 8, class,
		   							 vh_mlf_probability_gaussian);

	printf("\n\n\tNaive Bayes Abalone");
	test = vh_ml_test_classification(ml.ml, set_train, set_test, class);
	printf("\n\n\tNaive Bayes Abalone Prediction Rate: %f\n", test);
}

