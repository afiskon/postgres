/*-------------------------------------------------------------------------
 *
 * protocol.h
 *		Definitions of the request/response codes for the wire protocol.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/protocol.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROTOCOL_H
#define PROTOCOL_H

typedef enum PqMsg
{
	/* These are the request codes sent by the frontend. */

	PqMsg_Bind = 'B',
	PqMsg_Close = 'C',
	PqMsg_Describe = 'D',
	PqMsg_Execute = 'E',
	PqMsg_FunctionCall = 'F',
	PqMsg_Flush = 'H',
	PqMsg_Parse = 'P',
	PqMsg_Query = 'Q',
	PqMsg_Sync = 'S',
	PqMsg_Terminate = 'X',
	PqMsg_CopyFail = 'f',
	PqMsg_GSSResponse = 'p',
	PqMsg_PasswordMessage = 'p',
	PqMsg_SASLInitialResponse = 'p',
	PqMsg_SASLResponse = 'p',

	/* These are the response codes sent by the backend. */

	PqMsg_ParseComplete = '1',
	PqMsg_BindComplete = '2',
	PqMsg_CloseComplete = '3',
	PqMsg_NotificationResponse = 'A',
	PqMsg_CommandComplete = 'C',
	PqMsg_DataRow = 'D',
	PqMsg_ErrorResponse = 'E',
	PqMsg_CopyInResponse = 'G',
	PqMsg_CopyOutResponse = 'H',
	PqMsg_EmptyQueryResponse = 'I',
	PqMsg_BackendKeyData = 'K',
	PqMsg_NoticeResponse = 'N',
	PqMsg_Progress = 'P',
	PqMsg_AuthenticationRequest = 'R',
	PqMsg_ParameterStatus = 'S',
	PqMsg_RowDescription = 'T',
	PqMsg_FunctionCallResponse = 'V',
	PqMsg_CopyBothResponse = 'W',
	PqMsg_ReadyForQuery = 'Z',
	PqMsg_NoData = 'n',
	PqMsg_PortalSuspended = 's',
	PqMsg_ParameterDescription = 't',
	PqMsg_NegotiateProtocolVersion = 'v',

	/* These are the codes sent by both the frontend and backend. */

	PqMsg_CopyDone = 'c',
	PqMsg_CopyData = 'd',

}			PqMsg;

/* These are the authentication request codes sent by the backend. */

#define AUTH_REQ_OK			0	/* User is authenticated  */
#define AUTH_REQ_KRB4		1	/* Kerberos V4. Not supported any more. */
#define AUTH_REQ_KRB5		2	/* Kerberos V5. Not supported any more. */
#define AUTH_REQ_PASSWORD	3	/* Password */
#define AUTH_REQ_CRYPT		4	/* crypt password. Not supported any more. */
#define AUTH_REQ_MD5		5	/* md5 password */
/* 6 is available.  It was used for SCM creds, not supported any more. */
#define AUTH_REQ_GSS		7	/* GSSAPI without wrap() */
#define AUTH_REQ_GSS_CONT	8	/* Continue GSS exchanges */
#define AUTH_REQ_SSPI		9	/* SSPI negotiate without wrap() */
#define AUTH_REQ_SASL	   10	/* Begin SASL authentication */
#define AUTH_REQ_SASL_CONT 11	/* Continue SASL authentication */
#define AUTH_REQ_SASL_FIN  12	/* Final SASL message */
#define AUTH_REQ_MAX	   AUTH_REQ_SASL_FIN	/* maximum AUTH_REQ_* value */

#endif							/* PROTOCOL_H */
