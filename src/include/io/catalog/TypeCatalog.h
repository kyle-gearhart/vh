/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_TypeCatalog_H
#define vh_datacatalog_TypeCatalog_H

/*
 * The TypeCatalog is stored in a single opaque structure with two HashTables.
 * The first hash table indexes the type by their C typedef (vh_type_ctype) and
 * the second maps them by their TypeTag.
 *
 * It's important all types be registered prior to any forks.  We should not be
 * adding types in a forked process.
 */

void vh_type_add(Type ty);
void vh_type_remove(Type ty);

Type vh_type_ctype(const char *tname);
Type vh_type_tag(TypeTag tag);

#endif

