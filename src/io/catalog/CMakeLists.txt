
add_subdirectory(types)

set(vh_catalog_SRCS 
					catalog/BackEnd.c
					catalog/BackEndCatalog.c
					catalog/CatalogContext.c
					catalog/HeapField.c
					catalog/HeapTuple.c
					catalog/HeapTupleDef.c
					catalog/PrepTup.c
					catalog/PrintTup.c
					catalog/TableCatalog.c
					catalog/TableDef.c
					catalog/TableField.c
					catalog/TableSet.c
					catalog/Type.c
					catalog/TypeCatalog.c
					catalog/TypeVar.c
					catalog/TypeVarPage.c
					catalog/TypeVarSlot.c
					catalog/tam.c 
					catalog/searchpath.c

					catalog/acm/acm.c
					catalog/acm/acm_avg.c
					catalog/acm/acm_maxmin.c
					catalog/acm/acm_stat.c
					catalog/acm/acm_sum.c

					catalog/prepcol/prepcol.c
					catalog/prepcol/pcdefaultv.c
					catalog/prepcol/pcminmax.c
					catalog/prepcol/pcseq.c
					catalog/prepcol/pctsint.c

					catalog/sp/spht.c
					catalog/sp/sptd.c

					PARENT_SCOPE )

