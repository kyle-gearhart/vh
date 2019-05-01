/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_prepcol_H
#define vh_catalog_prepcol_H

#include "io/catalog/TypeVarSlot.h"

/*
 * PrepCol
 *
 * PrepCol allows for input data values to be enriched on the fly for a target slot.
 *
 * The slot may be assigned or appended to, but it cannot be destroyed.  There is one
 * primary function that must be implemented for a basic PrepCol to work appropriately.
 */

typedef const struct PrepColFuncTableData *PrepColFuncTable;
typedef struct PrepColData *PrepCol;

struct PrepColFuncTableData
{
	int32_t (*populate_slot)(void* pc, TypeVarSlot *slot_target, 
	   						 TypeVarSlot **datas, int32_t ndatas);

	int32_t (*init_run)(void* pc);
	int32_t (*finalize_run)(void* pc);

	int32_t (*finalize)(void* pc);
};

struct PrepColData
{
	PrepColFuncTable funcs;
};


void* vh_pc_create(PrepColFuncTable funcs, size_t sz);

/*
 * Base PrepCol Functions, we want these to be fast because they'll get called
 * a whole bunch.  We expect @pc to be valid, we're not going to check it.
 */
#define vh_pc_finalize(pc)		((pc) && (pc)->funcs->finalize ? pc->funcs->finalize((pc)) : (0==1))
#define vh_pc_init_run(pc)		((pc) && (pc)->funcs->init_run ? pc->funcs->init_run(pc) : (void))
#define vh_pc_finalize_run(pc)	((pc) && (pc)->funcs->finalize_run ? pc->funcs->finalize_run(pc) : (0==1))


/*
 * Return values:
 * 		<0	Error
 * 		0	No action taken
 * 		1	Action taken
 */
#define vh_pc_populate_slot(pc, tgt, datas, ndatas)	((pc)->funcs->populate_slot((pc),		\
																				(tgt),		\
																				(datas),	\
																				(ndatas)))

#define vh_pc_destroy(pc)		vh_pc_finalize_run(pc), vh_pc_finalize(pc), vhfree((pc))



#endif

