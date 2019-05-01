/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/buffer/slot.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/TableDef.h"


void
vh_slot_td_init(TableDefSlot *tds)
{
	tds->tdv = 0;
	tds->tc = 0;

	tds->tds_isempty = true;
	tds->tds_shouldrelease = false;
	tds->tds_shouldreleasecatalog = false;
}

void
vh_slot_td_reset(TableDefSlot *tds)
{
	if (tds->tds_isempty)
		return;

	if (tds->tds_shouldreleasecatalog)
		vh_cat_tbl_remove(tds->tc,
						  tds->tdv->td);
	if (tds->tds_shouldrelease)
	{
		vh_td_finalize(tds->tdv->td);
	}

	tds->tds_isempty = true;
}

void
vh_slot_td_store(TableDefSlot *tds,
				 TableDefVer tdv,
				 bool shouldrelease, bool tds_shouldreleasecatalog)
{
	if (!tds->tds_isempty)
		vh_slot_td_reset(tds);

	tds->tdv = tdv;
	tds->tc = 0;

	tds->tds_isempty = false;
	tds->tds_shouldrelease = shouldrelease;
	tds->tds_shouldreleasecatalog = tds_shouldreleasecatalog;
}


