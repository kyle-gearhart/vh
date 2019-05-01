/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_estepfwd_H
#define vh_datacatalog_executor_estepfwd_H


/*
 * Just a few typedef since most of the estep translation units will need
 * to declare the pointers to each specific ExecStep.
 */

typedef struct ExecStepDiscardData *ExecStepDiscard;
typedef struct ExecStepFetchData *ExecStepFetch;
typedef struct ExecStepFunnelData *ExecStepFunnel;

#endif
