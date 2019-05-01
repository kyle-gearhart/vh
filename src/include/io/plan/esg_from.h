/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_esg_from_H
#define vh_datacatalog_executor_esg_from_H

/*
 * Helper functions for the ExecStepGroup family of modules, used to extract
 * information from a NodeFrom tree.
 */

KeyValueList vh_esg_from_beac(Node nfrom);
SList vh_esg_from_pullup(Node nfrom);
SList vh_esg_from_list_tds(SList tds, bool unique);

#endif
