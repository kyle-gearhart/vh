/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/executor/param.h"
#include "io/plan/paramt.h"

struct ParameterListData
{
	Parameter first;
	Parameter last;
	int32_t count;
	
	Parameter iter;
	MemoryContext mctx;
};

ParameterList
vh_param_createlist(void)
{
	ParameterList pl;
	MemoryContext mctx = vh_mctx_current();
	
	pl = vhmalloc(sizeof(struct ParameterListData));
	pl->first = 0;
	pl->last = 0;
	pl->count = 0;
	pl->iter = 0;
	pl->mctx = mctx;
	
	return pl;
}

void
vh_param_destroylist(ParameterList pl, vh_param_dl_cb dl_cb,
						  void *dl_cb_data)
{
	Parameter p;
	
	if (pl)
	{
		vh_param_it_init(pl);
		
		while ((p = vh_param_it_next(pl)))
		{
			if (p->destroy.func)
				p->destroy.func(p, p->destroy.data);
			else if (dl_cb)
				dl_cb(pl, p, dl_cb_data);
			else
			{
				if (p->free)
					vhfree(p->value);
				
				vhfree(p);
			}
		}
		
		vhfree(pl);
	}
}

void
vh_param_add(ParameterList params, 
				  Parameter param)
{
	if (params->last)
	{
		params->last->next = param;
		params->last = param;
	}
	else
	{
		params->first = param;
		params->last = param;
	}

	params->count++;
	param->next = 0;
}

void*
vh_param_create(ParameterList pl, size_t sz)
{
	Parameter p;
	
	if (pl)
	{
		p = vhmalloc_ctx(pl->mctx, sz);
		
		p->list = pl;
		p->next = 0;
		p->value = 0;
		p->size = 0;
		p->null = false;
		p->free = false;
		
		return p;
	}
	
	return 0;
}

MemoryContext
vh_param_switchmctx(ParameterList pl)
{
	return vh_mctx_switch(pl->mctx);
}

int32_t
vh_param_count(ParameterList pl)
{
	if (pl)
		return pl->count;

	return 0;
}

void
vh_param_it_init(ParameterList pl)
{
	if (pl)
		pl->iter = pl->first;
}

void*
vh_param_it_first(ParameterList pl)
{
	if (pl)
	{
		if (pl->first)
			pl->iter = pl->first->next;
		
		return pl->first;
	}
	
	return 0;
}

void*
vh_param_it_next(ParameterList pl)
{
	Parameter p;
	
	if (pl)
	{
		p = pl->iter;

		if (p)
			pl->iter = p->next;
		
		return p;
	}
	
	return 0;
}

void*
vh_param_it_last(ParameterList pl)
{
	if (pl)
	{
		pl->iter = 0;
		
		return pl->last;
	}
	
	return 0;
}

