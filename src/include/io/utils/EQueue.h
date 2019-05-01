/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_UTILS_EQUEUE_H
#define VH_UTILS_EQUEUE_H

#include <time.h>

#define elog(level, message) do { \
			ErrorQueue errq = vh_err_queue_get(); 								\
			vh_err_queue(errq, (level), __FILE__, __LINE__, (message)); 		\
			} while (0)

#define emsg vh_err_msg

#include <setjmp.h>

typedef enum
{
	DEBUG1 = 0x01,
	DEBUG2 = 0x02,
	INFO = 0x04,
	WARNING = 0x08,
	ERROR = 0x10,
	ERROR1 = 0x10,
	ERROR2 = 0x10,
	FATAL = 0x20,
	PANIC = 0x40
} ErrorLevel;


typedef struct ErrorData *Error;
struct ErrorData
{
	ErrorLevel level;
	const char *message;
	int32_t message_len;
	const char *leveltxt;
	const char *file;
	int32_t file_line_number;
	int32_t stack_depth;
	char **stack_symbols;
	struct timeval tv;
	pid_t pid;
};

typedef int32_t (*vh_err_flush_func)(Error err, void *user);

struct ErrorFlushFuncData
{
	void *user;
	vh_err_flush_func func;
	int32_t error_levels;
};

typedef struct ErrorQueueData *ErrorQueue;
struct ErrorQueueData
{
	int32_t sz_flushes;
	int32_t n_flushes;
	MemoryContext mctx;

	struct ErrorData ed;	
	struct ErrorFlushFuncData flush[1];
};

/*
 * Error Queue Setup
 */
ErrorQueue vh_err_queue_alloc(MemoryContext parent, int32_t flushes);
void vh_err_queue_destroy(ErrorQueue equeue);


/*
 * Logging Functions
 *
 * When an error message is raised one or more functions may be called to output
 * the formatted message text to the console, syslog, etc.  In theory, these
 * outputs may be user implemented.
 *
 * These don't match the vh_err_queue_func parameter, because these are the SETUP
 * functions.  We only expose the setup functions which may have to allocate a
 * user data structure for the underlying logging function to work appropriately.
 *
 * Instead of forcing the caller to first create this user data area, then add the
 * the function to the array, we just hide all of that behind these functions.
 */

int32_t vh_err_queue_console(ErrorQueue eq, int32_t levels);
int32_t vh_err_queue_syslog(ErrorQueue eq, int32_t levels, const char *identifier);


/*
 * Error Queue Run Time
 */
ErrorQueue vh_err_queue_get();

const char* vh_err_msg(const char *message, ...);
void vh_err_queue(ErrorQueue queue, 
				  ErrorLevel level, 
				  const char *filepath, double lineno, 
				  const char *message);

extern sigjmp_buf *vh_exception_stack;

#define VH_TRY() 																\
	do {																		\
		sigjmp_buf *copy_stack = vh_exception_stack;							\
		sigjmp_buf local_stack;													\
		MemoryContext mctx_err = vh_mctx_current();								\
																				\
		if (sigsetjmp(local_stack, 0) == 0)										\
		{																		\
			vh_exception_stack = &local_stack;						
#define VH_CATCH() 																\
		} 																		\
		else																	\
		{																		\
			vh_exception_stack = copy_stack;									\
			if (vh_mctx_current() != mctx_err)									\
			{																	\
				vh_mctx_switch(mctx_err);										\
			}		
#define VH_ENDTRY() 															\
		} 																		\
		vh_exception_stack = copy_stack; 										\
	} while(0)

void vh_err_rethrow();

#endif

