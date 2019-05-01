/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_BackEndCatalog_H
#define vh_datacatalog_BackEndCatalog_H

typedef struct BackEndData BackEndData, *BackEnd;
typedef struct BackEndCatalogData *BackEndCatalog;

BackEndCatalog vh_cat_be_create(void);
void vh_cat_be_destroy(BackEndCatalog);

bool vh_cat_be_add(BackEndCatalog, BackEnd);
BackEnd vh_cat_be_getbyname(BackEndCatalog, const char*);;
BackEnd vh_cat_be_getbyid(BackEndCatalog, int);

#endif

