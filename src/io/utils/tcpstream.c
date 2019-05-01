/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <sys/fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netdb.h>

#include "vh.h"
#include "io/utils/tcpstream.h"

#define Min(x, y)			( x < y ? x : y)
#define INVALID_SOCKET 		0


#ifdef WIN32
#define SOCK_ERRNO (WSAGetLastError())
#define SOCK_STRERROR winsock_strerror
#define SOCK_ERRNO_SET(e)	WSASetLastError(e)
#else
#define SOCK_ERRNO errno
#define SOCK_STRERROR 
#define SOCK_ERRNO_SET(e)	(errno = (e))
#endif

static int32_t resolveHosts(TcpStreamConnection conn);
static int32_t connectSocket(TcpStreamConnection conn);

static int32_t send_buffer(TcpStreamConnection conn, TcpBuffer buf);
static int32_t recv_buffer(TcpStreamConnection conn, TcpBuffer buf);

static ssize_t raw_read(TcpStreamConnection conn, void *ptr, size_t len);
static ssize_t raw_write(TcpStreamConnection conn, const void *ptr, size_t len);


int32_t
vh_tcps_connect(TcpStreamConnection tcpsc)
{
	int32_t ret;

	ret = resolveHosts(tcpsc);

	if (!ret)
		ret = connectSocket(tcpsc);

	return ret;
}

int32_t
vh_tcps_reqresp(TcpStreamConnection conn,
				TcpBuffer bufin, TcpBuffer bufout)
{
	int32_t ret;

	ret = send_buffer(conn, bufin);
	ret = recv_buffer(conn, bufout);

	return ret;
}

int32_t
vh_tcps_disconnect(TcpStreamConnection conn)
{
	return 0;
}

static int32_t
connectSocket(TcpStreamConnection conn)
{
	conn->sock = socket(conn->addr_cur->ai_family, SOCK_STREAM, 0);

	if (conn->sock == INVALID_SOCKET)
	{
	}

	/*
	 * Set no-delay
	 */

	/*
	 * Set non-blocking
	 */

	if (fcntl(conn->sock, F_SETFD, FD_CLOEXEC) == -1)
	{
		return -1;
	}


	/*
	 * Set keep alives
	 */


	/*
	 * Set signal pipes
	 */

	/*
	if (setsockopt(conn->sock, SOL_SOCKET, SO_NOSIGPIPE, (char*)&optval, sizeof(optval)) == 0)
	{
	}
	*/

	/*
	 * Start/make the connection
	 */

	if (connect(conn->sock, conn->addr_cur->ai_addr, conn->addr_cur->ai_addrlen) < 0)
	{
		if (SOCK_ERRNO == EINPROGRESS ||
			SOCK_ERRNO == EWOULDBLOCK ||
			SOCK_ERRNO == EINTR)
		{
			return 0;
		}
	}

	return 0;	
}

static int32_t
resolveHosts(TcpStreamConnection conn)
{
	struct addrinfo hint;
	int32_t ret;

	if (!conn->host && !conn->hostaddr)
	{
	}

	if (!conn->port)
	{
	}

	memset(&hint, 0, sizeof(struct addrinfo));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;

	if (conn->hostaddr)
	{
		hint.ai_flags = AI_NUMERICHOST;
		ret = getaddrinfo(conn->hostaddr, conn->port, &hint,&conn->addr_cur);

		if (ret || !conn->addr_cur)
		{
			return -1;
		}
	}
	else
	{
		ret = getaddrinfo(conn->host, conn->port, &hint, &conn->addr_cur);

		if (ret || !conn->addr_cur)
		{
			return -1;
		}
	}

	return 0;
}

void*
vh_tcps_buf_create(size_t sz)
{
	TcpBuffer buf;

	assert(sz >= sizeof(struct TcpBufferData));

	buf = vhmalloc(sizeof(struct TcpBufferData));
	vh_str_init(&buf->data);
	buf->cursor = 0;

	return buf;
}

int32_t
vh_tcps_buf_remain(TcpBuffer buf)
{
	int32_t remain;

	if (buf)
	{
		remain = vh_strlen(&buf->data) - buf->cursor;

		return remain;
	}

	return 0;
}

void
vh_tcps_buf_pbool(TcpBuffer buf, int8_t val)
{
	int8_t temp_val = val ? 1 : 0;

	vh_str.AppendN(&buf->data, (const char*)&temp_val, sizeof(int8_t));
	buf->cursor += sizeof(int8_t);
}

bool
vh_tcps_buf_gbool(TcpBuffer buf)
{
	char *cursor = vh_str_buffer(&buf->data) + buf->cursor;
	bool *val = (bool*)cursor;

	buf->cursor += sizeof(bool);

	return *val;
}

int32_t
vh_tcps_buf_gi32(TcpBuffer buf)
{
	char *cursor = vh_str_buffer(&buf->data) + buf->cursor;
	int32_t *val = (int32_t*)cursor;

	buf->cursor += sizeof(int32_t);

	return *val;
}

int64_t
vh_tcps_buf_gi64(TcpBuffer buf)
{
	char *cursor = vh_str_buffer(&buf->data) + buf->cursor;
	int64_t *val = (int64_t*)cursor;;

	buf->cursor += sizeof(int64_t);

	return *val;
}

void
vh_tcps_buf_pi16(TcpBuffer buf, int16_t val)
{
	vh_str.AppendN(&buf->data, (const char*)&val, sizeof(int16_t));
	buf->cursor += sizeof(int16_t);
}

void
vh_tcps_buf_pi32(TcpBuffer buf, int32_t val)
{
	vh_str.AppendN(&buf->data, (const char*)&val, sizeof(int32_t));
	buf->cursor += sizeof(int32_t);
}

void
vh_tcps_buf_si32(TcpBuffer buf, int32_t at, int32_t val)
{
	char *buffer = vh_str_buffer(&buf->data);
	int32_t *target = (int32_t*)(buffer + at);

	*target = val;
}

void
vh_tcps_buf_pi64(TcpBuffer buf, int64_t val)
{
	vh_str.AppendN(&buf->data, (const char*)&val, sizeof(int64_t));
	buf->cursor += sizeof(int32_t);
}

static int32_t 
recv_buffer(TcpStreamConnection conn, TcpBuffer buf)
{
	ssize_t n;
	size_t elen;
	char *buffer = vh_str_buffer(&buf->data);
	int32_t len;

	elen = vh_strlen(&buf->data);
	buffer += buf->cursor;

	if (VH_STR_IS_OOL(&buf->data))
	{
		len = buf->data.capacity - vh_strlen(&buf->data);
	}
	else
	{
		len = VH_STR_INLINE_BUFFER - vh_strlen(&buf->data);
	}

	n = raw_read(conn, buffer, len);

	if (VH_STR_IS_OOL(&buf->data))
	{
		buf->data.varlen.size = (elen + n) | VH_STR_FLAG_OOL;
	}
	else
	{
		buf->data.varlen.size = (elen + n);
	}

	return n;
}

static int32_t
send_buffer(TcpStreamConnection conn, TcpBuffer buf)
{
	char *ptr = vh_str_buffer(&buf->data);
	int32_t len = vh_strlen(&buf->data);
	int32_t res = 0;
	int32_t sent;
	int32_t remaining = len;

	while (len > 0)
	{
		sent = raw_write(conn, ptr, len);

		if (sent < 0)
		{
		}
		else
		{
			ptr += sent;
			len -= sent;
			remaining -= sent;
		}
	}

	return res;
}


static ssize_t 
raw_read(TcpStreamConnection conn, void *ptr, size_t len)
{
	ssize_t n;
	int32_t result_errno = 0;

	n = recv(conn->sock, ptr, len, 0);

	if (n < 0)
	{
		result_errno = SOCK_ERRNO;

		switch (result_errno)
		{
			case EAGAIN:

#ifdef WIN32
			case EWOULDBLOCK:
#endif
			case EINTR:

				break;

			case ECONNRESET:
				break;

			default:
				break;
		}
	}

	SOCK_ERRNO_SET(result_errno);

	return n;
}

static ssize_t
raw_write(TcpStreamConnection conn, const void *ptr, size_t len)
{
	ssize_t n;
	int32_t flags = 0;
	int32_t result_errno;

retry_masked:
	n = send(conn->sock, ptr, len, flags);

	if (n < 0)
	{
		result_errno = SOCK_ERRNO;

		if (flags != 0)
		{
			flags = 0;
			goto retry_masked;
		}

		switch (result_errno)
		{
			case EAGAIN:
#ifdef WIN32
			case EWOULDBLOCK:
#endif
			case EINTR:
				break;

			case EPIPE:


			case ECONNRESET:

				break;

			default:

				break;
		}
	}

	SOCK_ERRNO_SET(result_errno);

	return n;
}

