/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_utils_tcpstream_H
#define vh_io_utils_tcpstream_H

struct addrinfo;

struct TcpStreamConnectionData
{
	int32_t sock;

	char *host;
	char *hostaddr;
	char *port;

	struct addrinfo *addr_cur;
};

struct TcpBufferData
{
	StringData data;
	int32_t cursor;
};

typedef struct TcpStreamConnectionData *TcpStreamConnection;
typedef struct TcpBufferData *TcpBuffer;

void* vh_tcps_buf_create(size_t sz);

int32_t vh_tcps_buf_remain(TcpBuffer buf);

void vh_tcps_buf_put(TcpBuffer buf, void *ptr, int32_t len);
void vh_tcps_buf_pbool(TcpBuffer buf, int8_t val);
void vh_tcps_buf_pi16(TcpBuffer buf, int16_t val);
void vh_tcps_buf_pi32(TcpBuffer buf, int32_t val);
void vh_tcps_buf_pi64(TcpBuffer buf, int64_t val);
void vh_tcps_buf_sbool(TcpBuffer buf, int32_t at, int8_t val);
void vh_tcps_buf_si32(TcpBuffer buf, int32_t at, int32_t val);
void vh_tcps_buf_si64(TcpBuffer buf, int32_t at, int64_t val);

int32_t vh_tcps_buf_copy(TcpBuffer buf, int32_t len, void *dest, int32_t dest_len);
bool vh_tcps_buf_gbool(TcpBuffer buf);
int32_t vh_tcps_buf_gi32(TcpBuffer buf);
int64_t vh_tcps_buf_gi64(TcpBuffer buf);



int32_t vh_tcps_connect(TcpStreamConnection tcpsc);
int32_t vh_tcps_reqresp(TcpStreamConnection tcpsc, TcpBuffer bufin, TcpBuffer bufout);
int32_t vh_tcps_disconnect(TcpStreamConnection tcpsc);

#endif

