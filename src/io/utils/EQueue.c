/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <execinfo.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/syslog.h>

#include "vh.h"
#include "io/utils/SList.h"



/*
 * ============================================================================
 * syslog Infrastructure
 * ============================================================================
 */

struct ErrorSysLogData
{
	int32_t facility;
	bool started;
	char identifier[1];
};

static int32_t err_queue_syslog(Error err, void *user);


/*
 * ============================================================================
 * Console Infrastructure
 * ============================================================================
 */

static int32_t err_queue_console(Error err, void *user);




/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

static int32_t err_queue_push_func(ErrorQueue eq, int32_t levels, 
								   vh_err_flush_func func, void *user);


static int32_t err_flush(ErrorQueue eq, Error err);


#ifdef _MSC_VER

#define snprintf c99_snprintf

/*
inline int c99_snprintf(char* str, size_t size, const char* format, ...)
{
    int count;
    va_list ap;

    va_start(ap, format);
    count = c99_vsnprintf(str, size, format, ap);
    va_end(ap);

    return count;
}
*/

inline int c99_vsnprintf(char* str, size_t size, const char* format, va_list ap)
{
    int count = -1;

    if (size != 0)
        count = _vsnprintf_s(str, size, _TRUNCATE, format, ap);
    if (count == -1)
        count = _vscprintf(format, ap);

    return count;
}

#endif // _MSC_VER


sigjmp_buf *vh_exception_stack = 0;


const char*
vh_err_msg(const char *message, ...)
{
	ErrorQueue q;
	char *buffer = 0;
	int count;
	va_list ap;

	q = vh_err_queue_get();

	if (q)
	{
#ifdef _WIN32
		va_start(ap, message);
		count = c99_vsnprintf(buffer, 0, message, ap);
		va_end(ap);
#else
		va_start(ap, message);
		count = vsnprintf(buffer, 0, message, ap);
		va_end(ap);
#endif

		if (count)
		{
			buffer = vh_mctx_alloc(q->mctx, (count + 1));
#ifdef _WIN32
			va_start(ap, message);
			count = c99_vsnprintf(buffer, count + 1, message, ap);
			va_end(ap);
#else
			va_start(ap, message);
			count = vsnprintf(buffer, count + 1, message, ap);
			va_end(ap);
#endif
		}
	}

	return buffer;
}


void
vh_err_queue(ErrorQueue queue,
	 		 ErrorLevel level, 
	  		 const char *filepath, 
  			 double lineno, 
  			 const char *message)
{
	Error error = &queue->ed;
	void *stack_list[50];
	int32_t res;
	
	error->message = message;
	error->message_len = strlen(message);
	error->level = level;
	error->file = filepath;
	error->file_line_number = (int32_t)lineno;
	error->stack_depth = backtrace(stack_list, 50);
	error->stack_symbols = backtrace_symbols(stack_list, error->stack_depth);

	switch (level)
	{
		case DEBUG1:
		case DEBUG2:
			error->leveltxt = "DEBUG";
			break;

		case INFO:
			error->leveltxt = "INFO";
			break;

		case WARNING:
			error->leveltxt = "WARNING";
			break;

		case ERROR:
			error->leveltxt = "ERROR";
			break;

		case FATAL:
			error->leveltxt = "FATAL";
			break;

		case PANIC:
			error->leveltxt = "PANIC";
			break;
	}

	gettimeofday(&error->tv, 0);
	error->pid = getpid();

	if (error->stack_depth > 50)
	{
		/*
		 * Stack depth is too deep to fit.  We won't do anything here but
		 * we probably should notify the user somehow.
		 */
	}

	res = err_flush(queue, error);

	if (res)
	{
	}

	if (error->stack_symbols)
		free(error->stack_symbols);
}

ErrorQueue
vh_err_queue_get(void)
{
	CatalogContext context;

	context = vh_ctx();
	
	return (ErrorQueue)context->errorQueue;
}

ErrorQueue
vh_err_queue_alloc(MemoryContext parent, int32_t flushes)
{
	ErrorQueue q;
	size_t alloc_sz;

	/*
	 * always alloate the ErrorQueueData struct in the parent context.
	 * this allows for the memory context in the error queue itself to
	 * be detached, allowing for easier memory management when a backround
	 * process flushes queued entries
	 */

	alloc_sz = sizeof(struct ErrorQueueData) +
	   		   ((flushes - 1) * sizeof(struct ErrorFlushFuncData));

	q = vh_mctx_alloc(parent, alloc_sz);
	memset(q, 0, alloc_sz);

	q->sz_flushes = flushes;
	q->n_flushes = 0;
	q->mctx = vh_MemoryPoolCreate(parent, 8192, "elog memory block");

	return q;
}

static int32_t
err_queue_push_func(ErrorQueue eq, int32_t levels,
	   				vh_err_flush_func func, void *user)
{
	int32_t idx = eq->n_flushes;

	if (idx < eq->sz_flushes)
	{
		eq->flush[idx].error_levels = levels;
		eq->flush[idx].func = func;
		eq->n_flushes++;

		return 0;
	}

	return 1;
}

static int32_t 
err_flush(ErrorQueue eq, Error err)
{
	int32_t i, levels, ret;

	for (i = 0; i < eq->n_flushes; i++)
	{
		levels = eq->flush[i].error_levels;

		/*
		 * Only fire the function if the levels filter is zero or the flag on the
		 * levels has been set for this particular Error Level.
		 *
		 * If the function returns less than zero, then we'll need to abort
		 * iterating the list.  Otherwise, just keep on moving.
		 */
		if (levels == 0 || (levels & err->level))
		{
			ret = eq->flush[i].func(err, eq->flush[i].user);

			if (ret < 0)
			{
				return ret;
			}
		}
	}

	return 0;
}

void
vh_rethrow()
{
	if (vh_exception_stack)
		siglongjmp(*vh_exception_stack, 1);
}



/* 
 * ============================================================================
 * Flush Functions 
 * ============================================================================
 */

int32_t vh_err_queue_console(ErrorQueue eq, int32_t levels)
{
	if (eq)
	{
		return err_queue_push_func(eq, levels, err_queue_console, 0);
	}

	return -1;
}

static int32_t
err_queue_console(Error err, void *user)
{
	struct tm *t;
	char buffer[50];
	size_t count, total;
	int32_t rc;

	t = localtime(&err->tv.tv_sec);

	total = count = strftime(&buffer[0], 50, "\n%Y-%m-%d %H:%M:%S", t);
	total += count = snprintf(&buffer[count], 50 - count, " [%s] %d ", err->leveltxt, err->pid);
	rc = write(fileno(stdout), &buffer[0], total);
	rc = write(fileno(stdout), err->message, err->message_len); 

	return rc;
}

int32_t
vh_err_queue_syslog(ErrorQueue eq, int32_t levels, const char *identifier)
{
	struct ErrorSysLogData *esl;
	size_t alloc_sz, ident_len;
	
	if (!identifier)
	{
		return -2;
	}

	alloc_sz = sizeof(struct ErrorSysLogData);
	ident_len = strlen(identifier);
	alloc_sz += ident_len + 1;


	if (eq)
	{
		esl = vh_mctx_alloc(eq->mctx, alloc_sz);
		strcpy(&esl->identifier[0], identifier);

		esl->started = false;

		return err_queue_push_func(eq, levels, err_queue_syslog, esl);
	}

	return -1;
}

static int32_t
err_queue_syslog(Error err, void *user)
{
	struct ErrorSysLogData *esl = user;

	if (!esl->started)
	{
		openlog(&esl->identifier[0], 
				LOG_PID | LOG_NDELAY | LOG_NOWAIT, 
				esl->facility);
		esl->started = true;
	}

	syslog(err->level, "[%s]\t%s", err->leveltxt, err->message);

	return 0;
}

