/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_utils_base64_H
#define vh_datacatalog_utils_base64_H

#include "vh.h"

char* vh_base64_encode(const unsigned char *data,
				   	   uint32_t input_length,
			   		   uint32_t *output_length);

unsigned char* vh_base64_decode(const char *data,
			   					uint32_t input_length,
		   						uint32_t *output_length);

size_t vh_base64_encode_str(const unsigned char *bytes, 
							String target,
			   				size_t bytes_len);

size_t vh_hex_encode_str(const unsigned char * bytes,
						 String target,
						 size_t bytes_len);


#endif

