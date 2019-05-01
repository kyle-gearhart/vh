/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/Type.h"

/*
 * vh_hf_init
 *
 * Initialize the types array and set type depth appropriately.
 */
void
vh_hf_init(HeapField hf)
{
	vh_type_stack_init(&hf->types[0]);
	hf->type_depth = 0;
	hf->tag = 0 | vh_hf_tag_heapfield;
}

bool
vh_hf_push_type(HeapField hf, Type ty)
{
	int8_t i;
	bool success = false;

	VH_TRY();
	{
		i = vh_type_stack_push(&hf->types[0], ty);
		
		if (i >= 0)
		{
			hf->type_depth = i + 1;
			success = true;
		}
	}
	VH_CATCH();
	{
		success = false;
	}
	VH_ENDTRY();

	return success;
}

