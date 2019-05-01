/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_executor_esg_quals_H
#define vh_datacatalog_sql_executor_esg_quals_H

/*
 * Used to scan qual nodes for certain characteristics
 */

void vh_esg_quals_addfromht(Node nquals, HeapTuple ht, HeapField *hfs, uint32_t nhfs, bool force);
int32_t vh_esg_quals_comphf(Node nquals, HeapTuple ht, HeapField *hfs, uint32_t hfsz);

struct pavl_table* vh_esg_quals_maphf(Node nquals, HeapField *hfs, uint32_t hfsz);

struct ESG_PullShardOpts
{
	TableDef td;
	bool xbeacon;
};

struct ESG_PullShardRet
{
	struct pavl_table *shards;
	bool is_xbeacon;
};

void vh_esg_quals_pullshard(struct ESG_PullShardOpts *opts, Node nquals,
 							struct ESG_PullShardRet *ret);

struct pavl_table* vh_esg_quals_pullnfrom(Node root);

#endif

