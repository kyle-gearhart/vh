/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <stdio.h>

#include "vh.h"
#include "io/utils/base64.h"
#include "io/utils/crypt/base64.h"


/*
 * Reimplmented for VH memory functions by copying:
 * http://stackoverflow.com/questions/342409/how-do-i-base64-encode-decode-in-c
 */


static char encoding_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
								'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
								'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
								'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
								'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
								'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
								'w', 'x', 'y', 'z', '0', '1', '2', '3',
								'4', '5', '6', '7', '8', '9', '+', '/'};
static char *decoding_table = NULL;
static int mod_table[] = {0, 2, 1};

static void build_decoding_table();

char*
vh_base64_encode(const unsigned char *data,
				 uint32_t input_length,
				 uint32_t *output_length)
{
	uint32_t i, j;
	char *encoded_data;

	*output_length = (4 * ((input_length + 2) / 3));
	encoded_data = (char*)vhmalloc(*output_length);

	if (encoded_data == NULL) 
		return NULL;

	for (i = 0, j = 0; i < input_length;) 
	{
		uint32_t octet_a = i < input_length ? (unsigned char)data[i++] : 0;
		uint32_t octet_b = i < input_length ? (unsigned char)data[i++] : 0;
		uint32_t octet_c = i < input_length ? (unsigned char)data[i++] : 0;

		uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

		encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
		encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
		encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
		encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
	}

	for (i = 0; i < mod_table[input_length % 3]; i++)
		encoded_data[*output_length - 1 - i] = '=';

	return encoded_data;
}

unsigned char*
vh_base64_decode(const char *data,
				 uint32_t input_length,
				 uint32_t *output_length)
{
	uint32_t i, j;

	if (decoding_table == NULL) 
		build_decoding_table();

	if (input_length % 4 != 0) 
		return NULL;

	*output_length = input_length / 4 * 3;
	if (data[input_length - 1] == '=') 
		(*output_length)--;
	if (data[input_length - 2] == '=') 
		(*output_length)--;

	unsigned char *decoded_data = (unsigned char*)vhmalloc(*output_length);
	
	if (decoded_data == NULL) 
		return NULL;

	for (i = 0, j = 0; i < input_length;) {

		uint32_t sextet_a = data[i] == '=' ? 0 & i++ : decoding_table[data[i++]];
		uint32_t sextet_b = data[i] == '=' ? 0 & i++ : decoding_table[data[i++]];
		uint32_t sextet_c = data[i] == '=' ? 0 & i++ : decoding_table[data[i++]];
		uint32_t sextet_d = data[i] == '=' ? 0 & i++ : decoding_table[data[i++]];

		uint32_t triple = (sextet_a << 3 * 6)
		+ (sextet_b << 2 * 6)
		+ (sextet_c << 1 * 6)
		+ (sextet_d << 0 * 6);

		if (j < *output_length) decoded_data[j++] = (triple >> 2 * 8) & 0xFF;
		if (j < *output_length) decoded_data[j++] = (triple >> 1 * 8) & 0xFF;
		if (j < *output_length) decoded_data[j++] = (triple >> 0 * 8) & 0xFF;
	}

	return decoded_data;
}

static void build_decoding_table() 
{
	int i;

	decoding_table = (char*)vhmalloc(256);

	for (i = 0; i < 64; i++)
		decoding_table[(unsigned char) encoding_table[i]] = i;
}

static void base64_cleanup() 
{
	if (decoding_table)
		vhfree(decoding_table);
	decoding_table = 0;
}

size_t
vh_base64_encode_str(const unsigned char *bytes,
					 String target,
					 size_t bytes_len)
{
	size_t bytes_written;
	unsigned char *buf;

	if (target && bytes_len)
	{
		vh_str.Resize(target, (bytes_len * 2) + 1);
		buf = vh_str_buffer(target);

		bytes_written = base64_encode(bytes, buf, bytes_len, 0);
		buf += bytes_written;
		*buf = '\0';

		if (VH_STR_IS_OOL(target))
		{
			target->varlen.size = bytes_written | VH_STR_FLAG_OOL;
		}
		else
		{
			target->varlen.size = bytes_written;
		}

		return bytes_written;
	}

	return 0;
}

size_t
vh_hex_encode_str(const unsigned char *bytes,
				  String target,
				  size_t bytes_len)
{
	size_t i, bytes_written, total = 0;
	unsigned char *buf;

	if (target && bytes_len)
	{
		vh_str.Resize(target, (bytes_len * 2) + 1);
		buf = vh_str_buffer(target);

		for (i = 0; i < bytes_len; i++)
		{
			bytes_written = sprintf(buf, "%02x", bytes[i]);

			buf += bytes_written;
			total += bytes_written;
		}

		*buf = '\0';

		if (VH_STR_IS_OOL(target))
		{
			target->varlen.size = total | VH_STR_FLAG_OOL;
		}
		else
		{
			target->varlen.size = total;
		}


		return total;
	}

	return 0;
}

