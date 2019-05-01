
set(vh_PATH analytics)

set(vh_analytics_SRCS 	${vh_PATH}/nest.c
						${vh_PATH}/nestidx.c
						${vh_PATH}/nestidx_btree.c
						${vh_PATH}/nestlevel.c
					    ${vh_PATH}/nloutput.c	
					
						${vh_PATH}/ml/ml.c
						${vh_PATH}/ml/mlfdistance.c
						${vh_PATH}/ml/mlfprobability.c
						${vh_PATH}/ml/mlknn.c
						${vh_PATH}/ml/mlnaive_bayes.c
						${vh_PATH}/ml/mlnormalize.c
							
						PARENT_SCOPE)

