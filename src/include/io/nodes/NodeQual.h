/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeQual_H
#define vh_datacatalog_sql_nodes_NodeQual_H

#include "io/catalog/TypeVarSlot.h"
#include "io/nodes/Node.h"

typedef enum QualOperator
{
	LessThan,
	LessThanEq,
	Eq,
	NotEq,
	GreaterThan,
	GreaterThanEq,
	In,
	ContainsRange,
	RangeContainedBy,
	RangeOverlap,
	RangeAdjacentTo
} QualOperator;

typedef enum QualChainMethod
{
	And,
	Or
} QualChainMethod;

#define VH_NQUAL_FMT_INVALID	0x00
#define VH_NQUAL_FMT_NF			0x01
#define VH_NQUAL_FMT_TF			0x02
#define VH_NQUAL_FMT_TVS		0x04
#define VH_NQUAL_FMT_TVSLIST	0x08
#define VH_NQUAL_FMT_FUNC		0x10

typedef struct NodeQualSData
{
	int32_t fmt;

	union
	{
		NodeField nf;
		TableField tf;
		TypeVarSlot tvs;
		TypeVarSlot *tvslist;
		const char *func;
	};
	
} *NodeQualS;

typedef struct NodeQualData
{
	struct NodeData node;
	QualChainMethod cm;
	struct NodeQualSData lhs;
	QualOperator oper;
	struct NodeQualSData rhs;
} *NodeQual;

NodeQual vh_nsql_qual_create(QualChainMethod cm, QualOperator oper);

#define vh_nsql_quals_isnf(qs)			((qs)->fmt & VH_NQUAL_FMT_NF)
#define vh_nsql_quals_istf(qs)			((qs)->fmt & VH_NQUAL_FMT_TF)
#define vh_nsql_quals_istvs(qs)			((qs)->fmt & VH_NQUAL_FMT_TVS)
#define vh_nsql_quals_istvslist(qs)		((qs)->fmt & VH_NQUAL_FMT_TVSLIST)
#define vh_nsql_quals_isfunc(qs)		((qs)->fmt & VH_NQUAL_FMT_FUNC)

#define vh_nsql_quals_nf(qs)			(vh_nsql_quals_isnf(qs) ? (qs)->nf : 0)
#define vh_nsql_quals_tf(qs)			(vh_nsql_quals_istf(qs) ? (qs)->tf : 0)
#define vh_nsql_quals_tvs(qs)			(vh_nsql_quals_istvs(qs) ? &(qs)->tvs : 0)
#define vh_nsql_quals_tvslist(qs)		(vh_nsql_quals_istvslist(qs) ? (qs)->tvslist : 0)
#define vh_nsql_quals_func(qs)			(vh_nsql_quals_isfunc(qs) ? (qs)->func : 0)

#define vh_nsql_qual_lhs_nf(q)			vh_nsql_quals_nf(&(q)->lhs)
#define vh_nsql_qual_lhs_tf(q)			vh_nsql_quals_tf(&(q)->lhs)
#define vh_nsql_qual_lhs_tvs(q)			vh_nsql_quals_tvs(&(q)->lhs)
#define vh_nsql_qual_lhs_func(q)		vh_nsql_quals_func(&(q)->lhs)

#define vh_nsql_qual_rhs_nf(q)			vh_nsql_quals_nf(&(q)->rhs)
#define vh_nsql_qual_rhs_tf(q)			vh_nsql_quals_tf(&(q)->rhs)
#define vh_nsql_qual_rhs_tvs(q)			vh_nsql_quals_tvs(&(q)->rhs)
#define vh_nsql_qual_rhs_func(q)		vh_nsql_quals_func(&(q)->rhs)

#define vh_nsql_quals_nf_set(qs, f)		((qs)->nf = (f), (qs)->fmt = VH_NQUAL_FMT_NF)
#define vh_nsql_quals_tf_set(qs, f)		((qs)->tf = (f), (qs)->fmt = VH_NQUAL_FMT_TF)
#define vh_nsql_quals_tvs_set(qs)		((qs)->fmt = VH_NQUAL_FMT_TVS)
#define vh_nsql_quals_tvslist_set(qs, f, s)	((qs)->fmt = (s << 8) | VH_NQUAL_FMT_TVSLIST,	\
											 (qs)->tvslist = (f))
#define vh_nsql_quals_func_set(qs, f)	((qs)->func = (f), (qs)->fmt = VH_NQUAL_FMT_FUNC)

#define vh_nsql_qual_lhs_nf_set(qs, f)	vh_nsql_quals_nf_set(&(qs)->lhs, f)
#define vh_nsql_qual_lhs_tf_set(qs, f)	vh_nsql_quals_tf_set(&(qs)->lhs, f)
#define vh_nsql_qual_lhs_tvs_set(qs)	vh_nsql_quals_tvs_set(&(qs)->lhs)
#define vh_nsql_qual_lhs_func_set(qs, f)	vh_nsql_quals_func_set(&(qs)->lhs, f)

#define vh_nsql_qual_rhs_nf_set(qs, f)	vh_nsql_quals_nf_set(&(qs)->rhs, f)
#define vh_nsql_qual_rhs_tf_set(qs, f)	vh_nsql_quals_tf_set(&(qs)->rhs, f)
#define vh_nsql_qual_rhs_tvs_set(qs)	vh_nsql_quals_tvs_set(&(qs)->rhs)
#define vh_nsql_qual_rhs_func_set(qs, f)	vh_nsql_quals_func_set(&(qs)->rhs, f)

/*
 * We store the size of the TVS List array in the upper 24 bits of the FMT data
 * member.
 */

#define VH_NSQL_QUAL_FMT_MASK 	(0x000000ff)
#define VH_NSQL_QUAL_SZ_MASK	(0xffffff00)

#define vh_nsql_qual_fmt(q)		((q)->fmt & VH_NSQL_QUAL_FMT_MASK)
#define vh_nsql_qual_sz(q)		(((q)->fmt & VH_NSQL_QUAL_SZ_MASK) >> 8)

#endif

