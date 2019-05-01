/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/utils/stopwatch.h"

#define NANOSECONDS_PER_MS 		(1000000)
#define MILLISECONDS_PER_SEC 	(1000)

void
vh_stopwatch_start(struct vh_stopwatch *watch)
{
	clock_gettime(CLOCK_MONOTONIC, &watch->t_start);
	watch->running = true;
	watch->finished = false;
}

void
vh_stopwatch_end(struct vh_stopwatch *watch)
{
	clock_gettime(CLOCK_MONOTONIC, &watch->t_end);
	watch->running = false;
	watch->finished = true;
}

int64_t
vh_stopwatch_ns(struct vh_stopwatch *watch)
{
	if (watch->finished)
		return watch->t_end.tv_nsec - watch->t_start.tv_nsec;

	return 0;
}

int64_t
vh_stopwatch_ms(struct vh_stopwatch *watch)
{
	if (watch->finished)
		return (watch->t_end.tv_nsec - watch->t_start.tv_nsec) / NANOSECONDS_PER_MS +
			   ((watch->t_end.tv_sec - watch->t_start.tv_sec) * MILLISECONDS_PER_SEC);

	return 0;
}

