/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_utils_lock_SLock_H
#define vh_datacatalog_utils_lock_SLock_H

#include <stdint.h>


	typedef struct SLock
	{
		enum LockState { Unlocked, Locked } lock;
	} SLock;

	inline void vh_SLockLock(SLock* lock)
	{
		/*
		__asm
		{
spin_lock:
			mov eax, 1
			xchng eax, lock->lock
			test eax, eax
			jnz spin_lock
			ret
		}
		*/
	}

	inline void vh_SLockUnlock(SLock* lock)
	{
		/*
		__asm
		{
spin_unlock:
			mov eax, 0
			xchng eax, lock->lock
			ret
		}
		*/
	}

#endif

