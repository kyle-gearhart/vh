/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/be/griddb/griddb-int.h"
#include "io/utils/base64.h"
#include "io/utils/crypt/md5.h"
#include "io/utils/crypt/sha256.h"

#define DEFAULT_PROTOCOL_VERSION	10


/*
 * CONNECT Message Helpers
 */
static void doConnect(GRDBconn conn);
static void fillConnectMessage(GRDBconn conn, GRDBbuffer buf);

/*
 * LOGIN Message Helpers
 */
static void doLogin(GRDBconn conn);


/*
 * LOGOUT Message Helpers
 */
static void doLogout(GRDBconn conn);


/*
 * DISCONNECT Message Helpers
 */
static void doDisconnect(GRDBconn conn);

/*
 * Challenge
 */
typedef struct GRDBchallengeData *GRDBchallenge;
typedef struct GRDBpassDigestData *GRDBpassDigest;

static void challenge_init(GRDBchallenge chg);
static void challenge_finalize(GRDBchallenge chg);
static GRDBchallenge challenge_getresponse(TcpBuffer buf, 
										   GRDBconn conn, 
										   GRDBchallenge last_challenge);
static void challenge_putrequest(GRDBbuffer buf, GRDBconn conn, GRDBchallenge lchg);
static void challenge_build_put(GRDBbuffer buf,
								GRDBconn conn, 
			 					GRDBchallenge chg,
			 					GRDBpassDigest digest);

static GRDBchallenge login_impl(GRDBconn conn, 
								GRDBpassDigest pass,
								GRDBchallenge last_chg);

struct GRDBchallengeData
{
	StringData nonce;
	StringData nc;
	StringData opaque;
	StringData baseSalt;

	StringData cnonce;
	bool challenging;
};


/*
 * Password Digest
 */

static void passdigest_init(GRDBpassDigest pass);
static void passdigest_finalize(GRDBpassDigest pass);
static GRDBpassDigest passdigest_make(const char *user, const char *pass);

struct GRDBpassDigestData
{
	StringData basicSecret;
	StringData cryptBase;
	StringData challengeBase;
};

static const GRDBauthMode DEFAULT_MODE = CHALLENGE;
static const char *DEFAULT_METHOD = "POST";
static const char *DEFAULT_REALM = "DB_Auth";
static const char *DEFAULT_URI = "/";
static const char *DEFAULT_QOP = "auth";

GRDBconn
GRDBconnect(const char *hostaddr,
			const char *port,
			const char *db,
			const char *user,
			const char *pass)
{
	GRDBconn conn;
	int32_t res;

	conn = vhmalloc(sizeof(struct GRDBconnData));
	conn->conn.hostaddr = strdup(hostaddr);
	conn->conn.port = strdup(port);

	conn->user = strdup(user);
	conn->pass = strdup(pass);

	if (GRDBconnectdb(conn))
	{
		elog(ERROR1, emsg("Unable to connect to GridDb host %s as requested",
					hostaddr));

		return 0;
	}

	return conn;
}


/*
 * GRDBconnectDb
 *
 * We're going to do a CONNECT message followed by a LOGIN message.
 * This is different from the standard GRDB client library, which do
 * these separately.  We still have the ability to do that, but we choose
 * not to stay with the standard VH BackEnd call structure.
 */
int32_t
GRDBconnectdb(GRDBconn conn)
{
	if (conn)
	{
		conn->auth_mode = DEFAULT_MODE;
		conn->protocolVersion = DEFAULT_PROTOCOL_VERSION;

		doConnect(conn);
		doLogin(conn);

		if (conn->loggedIn)
			return 0;
	}	

	return -1;
}

static void
doConnect(GRDBconn conn)
{
	GRDBbuffer outbuf = grdb_buffer_create();
	TcpBuffer inbuf;
	int32_t resp_len, ret;

	ret = vh_tcps_connect(&conn->conn);

	if (ret)
	{
		conn->connected = false;

		elog(ERROR1, emsg("Unable to connect to GridDb as requested"));

		return;
	}

	conn->connected = true;

	fillConnectMessage(conn, outbuf);
	resp_len = executeStatement(conn, 0, outbuf, &inbuf); 

	if (resp_len)
	{
		conn->auth_mode = vh_tcps_buf_gbool(inbuf);
	}
}

static void
fillConnectMessage(GRDBconn conn, GRDBbuffer buf)
{
	fillRequestHeader(buf, conn, CONNECT, SPECIAL_PARTITION_ID, 0, true);
	vh_tcps_buf_pi32(&buf->buf, conn->protocolVersion);
}

static void
doLogin(GRDBconn conn)
{
	GRDBchallenge chg, chg_last;
	GRDBpassDigest digest;

	digest = passdigest_make(conn->user, conn->pass);
	chg = login_impl(conn, digest, 0);

	if (chg)
	{
		chg_last = login_impl(conn, digest, chg);

		challenge_finalize(chg);
		vhfree(chg);

		if (chg_last)
		{
			challenge_finalize(chg_last);
			vhfree(chg_last);
		}
	}
	else
	{
		conn->loggedIn = true;
		conn->pass = 0;
	}
}

static GRDBchallenge
login_impl(GRDBconn conn, GRDBpassDigest pass, GRDBchallenge chg)
{
	GRDBchallenge nchg;
	GRDBbuffer buf = grdb_buffer_create();
	GRDBoptRequest optReq;
	TcpBuffer obuf;

	fillRequestHeader(buf, conn, LOGIN, SPECIAL_PARTITION_ID, 0, false);

	if (conn->protocolVersion >= 3)
	{
		optReq = grdb_optrequest_start(&buf->buf);

		if (conn->txTimeout >= 0)
		{
		}

		if (conn->database)
		{
		}

		grdb_optrequest_finish(optReq);
	}

	/*
	 * 1)	Username (string)
	 * 2)	Password challenge (string)
	 * 3)	Timeout (int32_t)
	 * 4)	Owner mode (bool)
	 * 5)	Cluster name
	 * 6)	Challenge
	 */	
	
	grdb_tcpbuf_put_cstr(conn->user, &buf->buf);
	challenge_build_put(buf, conn, chg, pass);
	vh_tcps_buf_pi32(&buf->buf, 10000);
	vh_tcps_buf_pbool(&buf->buf, false);
	grdb_tcpbuf_put_cstr("primary", &buf->buf);
	challenge_putrequest(buf, conn, chg);

	executeStatement(conn, 1, buf, &obuf);

	nchg = challenge_getresponse(obuf, conn, chg);

	return nchg;
}

static void
doLogout(GRDBconn conn)
{
	GRDBbuffer buf = grdb_buffer_create();
	GRDBoptRequest req;

	fillRequestHeader(buf, conn, LOGOUT, SPECIAL_PARTITION_ID, 0, false);

	req = grdb_optrequest_start(&buf->buf);
	grdb_optrequest_finish(req);

	executeStatement(conn, 0, buf, 0);

	conn->loggedIn = false;
}

static void
doDisconnect(GRDBconn conn)
{
	GRDBbuffer buf = grdb_buffer_create();
	GRDBoptRequest req;

	fillRequestHeader(buf, conn, DISCONNECT, SPECIAL_PARTITION_ID, 0, false);

	req = grdb_optrequest_start(&buf->buf);
	grdb_optrequest_finish(req);

	executeStatement(conn, 0, buf, 0);

	vh_tcps_disconnect(&conn->conn);

	conn->connected = false;
}


static void
challenge_init(GRDBchallenge chg)
{
	vh_str_init(&chg->nonce);
	vh_str_init(&chg->nc);
	vh_str_init(&chg->opaque);
	vh_str_init(&chg->baseSalt);
	vh_str_init(&chg->cnonce);
	
	chg->challenging = false;
}

static void
challenge_finalize(GRDBchallenge chg)
{
	vh_str_finalize(&chg->nonce);
	vh_str_finalize(&chg->nc);
	vh_str_finalize(&chg->opaque);
	vh_str_finalize(&chg->baseSalt);
	vh_str_finalize(&chg->cnonce);
}

static GRDBchallenge 
challenge_getresponse(TcpBuffer buf, 
					  GRDBconn conn, 
					  GRDBchallenge last_challenge)
{
	GRDBchallenge chg;
	GRDBauthMode buf_mode;
	int32_t remain;
   	bool respChallenging, challenging;

	if (buf)
	{
		remain = vh_tcps_buf_remain(buf);

		if (remain)
			buf_mode = (GRDBauthMode)vh_tcps_buf_gbool(buf);
		else
			elog(ERROR2, emsg("Invalid username or password"));
	}

	if (buf_mode != conn->auth_mode)
	{
		if (buf_mode == BASIC)
		{
			conn->auth_mode = buf_mode;

			chg = vhmalloc(sizeof(struct GRDBchallengeData));
			challenge_init(chg);

			return chg;
		}
	}
	else if (buf_mode == NONE)
	{
		return 0;
	}

	respChallenging = vh_tcps_buf_gbool(buf);

	if (last_challenge)
		challenging = last_challenge->challenging;
	
	if (buf_mode != BASIC &&
		!(respChallenging ^ challenging))
	{
		elog(ERROR1, emsg("Challenge message corrupted"));
	}

	if (respChallenging)
	{
		chg = vhmalloc(sizeof(struct GRDBchallengeData));
		challenge_init(chg);

		grdb_tcpbuf_get_str(&chg->nonce, buf);
		grdb_tcpbuf_get_str(&chg->nc, buf);
		grdb_tcpbuf_get_str(&chg->opaque, buf);
		grdb_tcpbuf_get_str(&chg->baseSalt, buf);

		chg->challenging = true;

		return chg;
	}

	return 0;
}

static void
challenge_putrequest(GRDBbuffer buf, GRDBconn conn,
					 GRDBchallenge lchg)
{
	if (conn->auth_mode == NONE)
	{
		return;
	}

	vh_tcps_buf_pbool(&buf->buf, (bool)conn->auth_mode);

	if (lchg && lchg->challenging)
	{
		vh_tcps_buf_pbool(&buf->buf, true);
		grdb_tcpbuf_put_str(&lchg->opaque, &buf->buf);
		grdb_tcpbuf_put_str(&lchg->cnonce, &buf->buf);
	}
	else
	{
		vh_tcps_buf_pbool(&buf->buf, false);
	}
}

/*
 * challenge_build_put
 *
 * Do a few things here, create the password digest, build the challenge string,
 * and put it on the buffer.
 */
static void
challenge_build_put(GRDBbuffer buf,
					GRDBconn conn, 
					GRDBchallenge chg,
					GRDBpassDigest digest)
{
	bool isChallenging = chg ? chg->challenging : false;
	StringData concat, ha1, ha2, ha3, cd, secret;
	MD5_CTX md_ctx;
	SHA256_CTX sh_ctx;
	unsigned char *str_buf;
	unsigned char md_buf[MD5_BLOCK_SIZE], sh_buf[SHA256_BLOCK_SIZE];

	if (!isChallenging)
	{
		if (conn->auth_mode == CHALLENGE)
		{
			/*
			 * Put an empty string
			 */
			grdb_tcpbuf_put_cstr(0, &buf->buf);
		}
		else
		{
			grdb_tcpbuf_put_str(&digest->basicSecret, &buf->buf);
		}

		return;
	}
	else
	{
		/*
		 * Build a random cnonce
		 */



		/*
		 * We get to build the challenge response for the wire!  This is an
		 * unbelievable mess, but here's the gist of it:
		 * 	1)	Form HA1
		 * 		a)	Concatenate digest->challengeBase + ":" + nonce + ":" cnonce
		 * 		b)	MD5 hash
		 * 	2)	Form HA2
		 * 		a)	Concatenate DEFAULT_METHOD + ":" + DEFAULT_URI
		 * 	3)	MD5 Hash HA1 and HA2 as HA3
		 * 		a)	Concatentate HA1 + ":" + nonce + ":" + nc + ":" + conce + ":" +
		 * 			DEFAULT_QOP + ":" + HA2
		 * 		b)	MD5 hash
		 * 	4)	Form CryptSecret
		 * 		a)	Concatentate baseSalt + ":" digest->cryptBase
		 * 		b)	SHA256 Hash
		 * 	5)	Concatentate "#1#" + HA3 + "#" + CryptSecret
		 *
		 * That last bit is what we get to put on the wire.
		 */

		vh_str_init(&concat);

		vh_str.AssignStr(&concat, &digest->challengeBase);
		vh_str.Append(&concat, ":");
		vh_str.AppendStr(&concat, &chg->nonce);
		vh_str.Append(&concat, ":");
		vh_str.AppendStr(&concat, &chg->cnonce);

		str_buf = vh_str_buffer(&concat);
		md5_init(&md_ctx);
		md5_update(&md_ctx, str_buf, vh_strlen(&concat));
		md5_final(&md_ctx, md_buf);

		vh_str_init(&ha1);
		vh_hex_encode_str(md_buf, &ha1, MD5_BLOCK_SIZE);


		/*
		 * Step 2
		 */
		vh_str.Assign(&concat, DEFAULT_METHOD);
		vh_str.Append(&concat, ":");
		vh_str.Append(&concat, DEFAULT_URI);

		str_buf = vh_str_buffer(&concat);
		md5_init(&md_ctx);
		md5_update(&md_ctx, str_buf, vh_strlen(&concat));
		md5_final(&md_ctx, md_buf);

		vh_str_init(&ha2);
		vh_hex_encode_str(md_buf, &ha2, MD5_BLOCK_SIZE);


		/*
		 * Step 3
		 */
		vh_str.AssignStr(&concat, &ha1);
		vh_str.Append(&concat, ":");
		vh_str.AppendStr(&concat, &chg->nonce);
		vh_str.Append(&concat, ":");
		vh_str.AppendStr(&concat, &chg->nc);
		vh_str.Append(&concat, ":");
		vh_str.AppendStr(&concat, &chg->cnonce);
		vh_str.Append(&concat, ":");
		vh_str.Append(&concat, DEFAULT_QOP);
		vh_str.Append(&concat, ":");
		vh_str.AppendStr(&concat, &ha2);

		str_buf = vh_str_buffer(&concat);
		md5_init(&md_ctx);
		md5_update(&md_ctx, str_buf, vh_strlen(&concat));
		md5_final(&md_ctx, md_buf);

		vh_str_init(&ha3);
		vh_base64_encode_str(md_buf, &ha3, MD5_BLOCK_SIZE);

		vh_str_finalize(&ha1);
		vh_str_finalize(&ha2);


		/*
		 * Step 4
		 */
		vh_str.AssignStr(&concat, &chg->baseSalt);
		vh_str.Append(&concat, ":");
		vh_str.AppendStr(&concat, &digest->cryptBase);

		str_buf = vh_str_buffer(&concat);
		sha256_init(&sh_ctx);
		sha256_update(&sh_ctx, str_buf, vh_strlen(&concat));
		sha256_final(&sh_ctx, sh_buf);

		vh_str_init(&secret);
		vh_hex_encode_str(sh_buf, &secret, SHA256_BLOCK_SIZE);

		/*
		 * Step 5
		 */

		vh_str.Assign(&concat, "#1#");
		vh_str.AppendStr(&concat, &ha3);
		vh_str.Append(&concat, "#");
		vh_str.AppendStr(&concat, &secret);

		/*
		 * Put all that mess on the wire
		 */

		grdb_tcpbuf_put_str(&concat, &buf->buf);

		vh_str_finalize(&concat);
		vh_str_finalize(&ha3);
		vh_str_finalize(&secret);
	}

}

static void
passdigest_init(GRDBpassDigest digest)
{
	if (digest)
	{
		vh_str_init(&digest->basicSecret);
		vh_str_init(&digest->cryptBase);
		vh_str_init(&digest->challengeBase);
	}
}

static GRDBpassDigest
passdigest_make(const char *user, const char *pass)
{
	GRDBpassDigest digest;
	SHA256_CTX sha_ctx;
	MD5_CTX md5_ctx;
	BYTE sha_buf[SHA256_BLOCK_SIZE];
	BYTE md_buf[MD5_BLOCK_SIZE];
	StringData str_temp;
		
	if (user && pass)
	{
		digest = vhmalloc(sizeof(struct GRDBpassDigestData));
		passdigest_init(digest);

		/*
		 * basicSecret
		 */
		sha256_init(&sha_ctx);
		sha256_update(&sha_ctx, (const unsigned char*)pass, strlen(pass)); 
		sha256_final(&sha_ctx, sha_buf);
		vh_hex_encode_str(sha_buf, &digest->basicSecret, SHA256_BLOCK_SIZE);

		/*
		 * cryptBase
		 */
		vh_str_init(&str_temp);

		vh_str.Append(&str_temp, user);
		vh_str.Append(&str_temp, ":");
		vh_str.Append(&str_temp, pass);

		sha256_init(&sha_ctx);
		sha256_update(&sha_ctx, 
					  (const unsigned char*)vh_str_buffer(&str_temp), 
					  vh_strlen(&str_temp));
		sha256_final(&sha_ctx, sha_buf);
		vh_hex_encode_str(sha_buf, &digest->cryptBase, SHA256_BLOCK_SIZE);


		/*
		 * challengeBase
		 */	
		vh_str.Assign(&str_temp, user);
		vh_str.Append(&str_temp, ":");
		vh_str.Append(&str_temp, DEFAULT_REALM);
		vh_str.Append(&str_temp, ":");
		vh_str.Append(&str_temp, pass);

		md5_init(&md5_ctx);
		md5_update(&md5_ctx,
				   (const unsigned char*)vh_str_buffer(&str_temp),
				   vh_strlen(&str_temp));
		md5_final(&md5_ctx, md_buf);
		vh_hex_encode_str(md_buf, &digest->challengeBase, MD5_BLOCK_SIZE);	

		return digest;
	}
}

