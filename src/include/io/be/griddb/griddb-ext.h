/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_be_griddb_ext_H
#define vh_io_be_griddb_ext_H

typedef struct GRDBconnData *GRDBconn;


GRDBconn GRDBconnect(const char *hostaddr,
					 const char *port,
					 const char *db,
					 const char *user,
					 const char *pass);

extern struct BackEndData vh_be_griddb;

#endif

