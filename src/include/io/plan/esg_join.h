/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_executor_esg_join_H
#define vh_datacatalog_sql_executor_esg_join_H

KeyValueList vh_esg_join_beac(Node root);
SList vh_esg_join_pullup(Node root);
SList vh_esg_join_list_tds(SList njoins, bool unique);


#endif

