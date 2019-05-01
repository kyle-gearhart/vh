/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_utils_stopwatch_H
#define vh_datacatalog_utils_stopwatch_H

#include <time.h>

struct vh_stopwatch
{
	struct timespec t_start, t_end;
	bool running, finished;	
};

void vh_stopwatch_start(struct vh_stopwatch *watch);
void vh_stopwatch_end(struct vh_stopwatch *watch);
int64_t vh_stopwatch_ns(struct vh_stopwatch *watch);
int64_t vh_stopwatch_ms(struct vh_stopwatch *watch);

#endif

