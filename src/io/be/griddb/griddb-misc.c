/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/be/griddb/griddb-int.h"

#define STATEMENT_TYPE_NUMBER_V2_OFFSET 	100

GRDBbuffer
grdb_buffer_create(void)
{
	GRDBbuffer buffer = vh_tcps_buf_create(sizeof(struct GRDBbufferData));

	buffer->lenword_at = 0;

	return buffer;
}

void
grdb_tcpbuf_get_str(String str, TcpBuffer buf)
{
	int32_t len = vh_tcps_buf_gi32(buf);
	char *strbuf = vh_str_buffer(&buf->data);

	strbuf += buf->cursor;
	buf->cursor += len;

	vh_str.AssignN(str, strbuf, len);
}

int32_t
grdb_tcpbuf_put_str(String str, TcpBuffer buf)
{
	size_t len = vh_strlen(str);

	vh_tcps_buf_pi32(buf, len);
	vh_str.AppendStr(&buf->data, str);
	buf->cursor += len;

	return len + sizeof(int32_t);
}

int32_t
grdb_tcpbuf_put_cstr(const char *str, TcpBuffer buf)
{
	size_t len = 0;

	if (str)
	{
		len = strlen(str);
		vh_tcps_buf_pi32(buf, len);
		vh_str.AppendN(&buf->data, str, len);
		buf->cursor += len;
	}
	else
	{
		vh_tcps_buf_pi32(buf, 0);
	}

	return len + sizeof(int32_t);
}

int32_t
fillRequestHeader(GRDBbuffer buf, GRDBconn conn,
				  GRDBstatement statement, int32_t partitionId, 
				  int64_t statementId, bool firstStatement)
{
	vh_tcps_buf_pi32(&buf->buf, EE_MAGIC_NUMBER);

	if (conn->ipv6Enabled)
	{
		vh_tcps_buf_pi64(&buf->buf, 0);
		vh_tcps_buf_pi64(&buf->buf, 0);
	}
	else
	{
		vh_tcps_buf_pi32(&buf->buf, 0);
	}

	vh_tcps_buf_pi32(&buf->buf, 0);
	vh_tcps_buf_pi32(&buf->buf, -1);

	/*
	 * Set where the length word is at, the value going in this position
	 * is length of the acutal request, without the header.
	 */
	buf->lenword_at = buf->buf.cursor;

	vh_tcps_buf_pi32(&buf->buf, 0);				/* Length without eeHead buf->size - buf->length_word */

	conn->eeHeadLength = buf->buf.cursor;

	if (conn->protocolVersion < 2)
		vh_tcps_buf_pi32(&buf->buf, statement);		/* statementTypeNumber */
	else
		vh_tcps_buf_pi32(&buf->buf, statement + STATEMENT_TYPE_NUMBER_V2_OFFSET);

	vh_tcps_buf_pi32(&buf->buf, partitionId);	/* partitionId */

	/* statementId */
	if (conn->protocolVersion >= 3 && !firstStatement)
	{
		vh_tcps_buf_pi64(&buf->buf, statementId);
	}
	else
	{
		vh_tcps_buf_pi32(&buf->buf, (int32_t)statementId);
	}
	
	return 0;
}


/*
 * Optional Requests Utility
 */

GRDBoptRequest grdb_optrequest_start(TcpBuffer buf)
{
	GRDBoptRequest req;

	if (buf)
	{
		req = vhmalloc(sizeof(struct GRDBoptRequestData));
		req->buffer = buf;
		req->lenword_at = buf->cursor;
		req->body_len = 0;

		vh_tcps_buf_pi32(buf, 0);

		return req;
	}

	return 0;
}

void
grdb_optrequest_pbool(GRDBoptRequest req, 
					  GRDBoptRequestType ty, 
					  bool val)
{
	if (req)
	{
		switch (ty)
		{
			case FOR_UPDATE:
			case CONTAINER_LOCK_REQUIRED:
			case SYSTEM_MODE:
			case REQUEST_MODULE_TYPE:

				vh_tcps_buf_pi16(req->buffer, (int16_t) ty);
				vh_tcps_buf_pbool(req->buffer, val);
				req->body_len += sizeof(int16_t) + sizeof(bool);

				break;

			default:
				elog(ERROR1, emsg("Optional Request Type %d is not a boolean value",
							ty));
		}
	}
}

void
grdb_optrequest_pi32(GRDBoptRequest req,
					 GRDBoptRequestType ty,
					 int32_t val)
{
	if (req)
	{
		switch (ty)
		{
			case TRANSACTION_TIMEOUT:
			case CONTAINER_ATTRIBUTE:
			case ROW_INSERT_UPDATE:
			case STATEMENT_TIMEOUT:

				vh_tcps_buf_pi16(req->buffer, (int16_t) ty);
				vh_tcps_buf_pi32(req->buffer, val);
				req->body_len += sizeof(int16_t) + sizeof(int32_t);

				break;

			default:
				elog(ERROR1, emsg("Optional Request Type %d is not a 32 bit integer value",
							ty));
		}
	}
}

void
grdb_optreques_pi64(GRDBoptRequest req,
					GRDBoptRequestType ty,
					int64_t val)
{
	if (req)
	{
		switch (ty)
		{
			case FETCH_LIMIT:
			case FETCH_SIZE:

				vh_tcps_buf_pi16(req->buffer, (int16_t) ty);
				vh_tcps_buf_pi64(req->buffer, val);
				req->body_len += sizeof(int16_t) + sizeof(int32_t);

				break;

			default:
				elog(ERROR1, emsg("Optional Request Type %d is not a 64 bit integer value",
							ty));
		}
	}
}

void
grdb_optrequest_pstr(GRDBoptRequest req, 
					 GRDBoptRequestType ty, 
					 String val)
{
	int32_t len;

	if (req)
	{
		switch (ty)
		{
			case DB_NAME:

				vh_tcps_buf_pi16(req->buffer, (int16_t) ty);
				len = grdb_tcpbuf_put_str(val, req->buffer);

				req->body_len += sizeof(int16_t) + len;

				break;

			default:
				elog(ERROR1, emsg("Optional Request Type %d is not a string value",
							ty));
		}
	}
}

void
grdb_optrequest_pcstr(GRDBoptRequest req,
					  GRDBoptRequestType ty,
					  const char *val)
{
	int32_t len;

	if (req)
	{
		switch (ty)
		{
			case DB_NAME:

				vh_tcps_buf_pi16(req->buffer, (int16_t) ty);
				len = grdb_tcpbuf_put_cstr(val, req->buffer);

				req->body_len += sizeof(int16_t) + len;

			default:
				elog(ERROR1, emsg("Optional Request Type %d is not a string value",
							ty));
		}
	}
}

void
grdb_optrequest_finish(GRDBoptRequest req)
{
	if (req)
	{
		if (req->body_len)
		{
			vh_tcps_buf_si32(req->buffer, req->lenword_at, req->body_len);
		}

		vhfree(req);
	}
}

