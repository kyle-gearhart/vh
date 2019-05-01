/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/be/griddb/griddb-int.h"

static void readRemoteException(GRDBconn conn, GRDBstatementResult res,
								TcpBuffer buf);


/*
 * Needs to handle HeartBeats
 */

int32_t
executeStatement(GRDBconn conn, int64_t statementId,
				 GRDBbuffer input, TcpBuffer *bufout)
{
	TcpBuffer outbuf = vh_tcps_buf_create(sizeof(struct TcpBufferData));
	int32_t res, respBodyLength, respStatementType;
	GRDBstatementResult statementResult;

	if (!conn || !input)
	{
		elog(ERROR1,
				emsg("Missing GRDBconn and/or input GRDBbuffer"));

		return -1;
	}

	if (conn->connected)
	{
		vh_str.Resize(&outbuf->data, 1024);
		
		vh_tcps_buf_si32(&input->buf, input->lenword_at,
						 (vh_strlen(&input->buf.data) - conn->eeHeadLength));
		
		res = vh_tcps_reqresp(&conn->conn, &input->buf, outbuf);

		if (res > 0)
		{
			if (vh_tcps_buf_gi32(outbuf) != EE_MAGIC_NUMBER)
			{
				elog(ERROR1, emsg("Corrupt message when procesing statement %lld",
							statementId));

				return -2;
			}
			
			outbuf->cursor = input->lenword_at;

			/*
			 * Check how much we've read back from the wire and wait for 
			 * more if necessary
			 */
			respBodyLength = vh_tcps_buf_gi32(outbuf);



			respStatementType = vh_tcps_buf_gi32(outbuf);
			statementResult = (GRDBstatementResult)vh_tcps_buf_gbool(outbuf);

			if (statementResult == SR_SUCCESS)
			{
				if (bufout)
					*bufout = outbuf;

				return respBodyLength;
			}
			else
			{
				readRemoteException(conn, statementResult, outbuf);

				if (bufout)
					*bufout = outbuf;
			}
		}		
	}

	return 0;
}

static void 
readRemoteException(GRDBconn conn, GRDBstatementResult res,
					TcpBuffer buf)
{
	int32_t count = vh_tcps_buf_gi32(buf);
	int32_t i, code, line;
	StringData builder, message, typename, filename, functionname;

	vh_str_init(&builder);
	vh_str_init(&message);
	vh_str_init(&typename);
	vh_str_init(&filename);
	vh_str_init(&functionname);

	for (i = 0; i < count; i++)
	{
		code = vh_tcps_buf_gi32(buf);
	
		grdb_tcpbuf_get_str(&message, buf);
		grdb_tcpbuf_get_str(&typename, buf);
		grdb_tcpbuf_get_str(&filename, buf);
		grdb_tcpbuf_get_str(&functionname, buf);	
		line = vh_tcps_buf_gi32(buf);
	}

}

