/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_IO_BUFFER_SLOT_H
#define VH_IO_BUFFER_SLOT_H

/*
 * We use slots to tell us what to do with a given structure when 
 * the user no longer needs it.  Slots concentrate on the primary data
 * structures holding user data.
 */


/*
 * HeapTupleDefSlot
 */
typedef struct HeapTupleDefSlot
{
	HeapTupleDef htd;
	bool htds_shouldrelease;
} HeapTupleDefSlot;

void vh_slot_htds_init(HeapTupleDefSlot *htds);
void vh_slot_htds_reset(HeapTupleDefSlot *htds);
void vh_slot_htds_store(HeapTupleDefSlot *htds,
						HeapTupleDef htd,
						bool shouldrelease);

/* 
 * TableDefSlot
 */
typedef struct TableDefSlot
{
	TableDefVer tdv;
	TableCatalog tc;

	bool tds_isempty;
	bool tds_shouldrelease;
	bool tds_shouldreleasecatalog;
} TableDefSlot;

void vh_slot_td_init(TableDefSlot *tds);
void vh_slot_td_reset(TableDefSlot *tds);
void vh_slot_td_store(TableDefSlot *tds,
					  TableDefVer tdv,
					  bool shouldrelease, bool shouldreleasecatalog);

/*
 * HeapTupleSlot
 */

typedef struct HeapTupleSlot
{
	union
	{
		HeapTuple ht;
		HeapTuplePtr htp;
	};

	bool hts_isbuffer;
} HeapTupleSlot;

#endif

