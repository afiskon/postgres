/*-------------------------------------------------------------------------
 *
 * bytea.c
 *	  Functions for the bytea type.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/bytea.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

//#include <ctype.h>
//#include <limits.h>

#include "access/detoast.h"
//#include "access/toast_compression.h"
//#include "catalog/pg_collation.h"
///#include "catalog/pg_type.h"
///#include "common/hashfn.h"
///#include "common/int.h"
//#include "common/unicode_category.h"
//#include "common/unicode_norm.h"
//#include "common/unicode_version.h"
#include "funcapi.h"
///#include "lib/hyperloglog.h"
#include "libpq/pqformat.h"
///#include "miscadmin.h"
///#include "nodes/execnodes.h"
////#include "parser/scansup.h"
////#include "port/pg_bswap.h"
//#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
///#include "utils/guc.h"
////#include "utils/lsyscache.h"
////#include "utils/memutils.h"
//#include "utils/pg_locale.h"
///#include "utils/sortsupport.h"
//#include "utils/varlena.h"

/* subroutine to initialize state */
static StringInfo
makeStringAggState(FunctionCallInfo fcinfo)
{
	StringInfo	state;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "bytea_string_agg_transfn called in non-aggregate context");
	}

	/*
	 * Create state in aggregate context.  It'll stay there across subsequent
	 * calls.
	 */
	oldcontext = MemoryContextSwitchTo(aggcontext);
	state = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);

	return state;
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

#define VAL(CH)			((CH) - '0')
#define DIG(VAL)		((VAL) + '0')

/*
 *		byteain			- converts from printable representation of byte array
 *
 *		Non-printable characters must be passed as '\nnn' (octal) and are
 *		converted to internal form.  '\' must be passed as '\\'.
 *		ereport(ERROR, ...) if bad form.
 *
 *		BUGS:
 *				The input is scanned twice.
 *				The error checking of input is minimal.
 */
Datum
byteain(PG_FUNCTION_ARGS)
{
	char	   *inputText = PG_GETARG_CSTRING(0);
	Node	   *escontext = fcinfo->context;
	char	   *tp;
	char	   *rp;
	int			bc;
	bytea	   *result;

	/* Recognize hex input */
	if (inputText[0] == '\\' && inputText[1] == 'x')
	{
		size_t		len = strlen(inputText);

		bc = (len - 2) / 2 + VARHDRSZ;	/* maximum possible length */
		result = palloc(bc);
		bc = hex_decode_safe(inputText + 2, len - 2, VARDATA(result),
							 escontext);
		SET_VARSIZE(result, bc + VARHDRSZ); /* actual length */

		PG_RETURN_BYTEA_P(result);
	}

	/* Else, it's the traditional escaped style */
	for (bc = 0, tp = inputText; *tp != '\0'; bc++)
	{
		if (tp[0] != '\\')
			tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
			tp += 4;
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
			tp += 2;
		else
		{
			/*
			 * one backslash, not followed by another or ### valid octal
			 */
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "bytea")));
		}
	}

	bc += VARHDRSZ;

	result = (bytea *) palloc(bc);
	SET_VARSIZE(result, bc);

	tp = inputText;
	rp = VARDATA(result);
	while (*tp != '\0')
	{
		if (tp[0] != '\\')
			*rp++ = *tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
		{
			bc = VAL(tp[1]);
			bc <<= 3;
			bc += VAL(tp[2]);
			bc <<= 3;
			*rp++ = bc + VAL(tp[3]);

			tp += 4;
		}
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
		{
			*rp++ = '\\';
			tp += 2;
		}
		else
		{
			/*
			 * We should never get here. The first pass should not allow it.
			 */
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "bytea")));
		}
	}

	PG_RETURN_BYTEA_P(result);
}

/*
 *		byteaout		- converts to printable representation of byte array
 *
 *		In the traditional escaped format, non-printable characters are
 *		printed as '\nnn' (octal) and '\' as '\\'.
 */
Datum
byteaout(PG_FUNCTION_ARGS)
{
	bytea	   *vlena = PG_GETARG_BYTEA_PP(0);
	char	   *result;
	char	   *rp;

	if (bytea_output == BYTEA_OUTPUT_HEX)
	{
		/* Print hex format */
		rp = result = palloc(VARSIZE_ANY_EXHDR(vlena) * 2 + 2 + 1);
		*rp++ = '\\';
		*rp++ = 'x';
		rp += hex_encode(VARDATA_ANY(vlena), VARSIZE_ANY_EXHDR(vlena), rp);
	}
	else if (bytea_output == BYTEA_OUTPUT_ESCAPE)
	{
		/* Print traditional escaped format */
		char	   *vp;
		uint64		len;
		int			i;

		len = 1;				/* empty string has 1 char */
		vp = VARDATA_ANY(vlena);
		for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++)
		{
			if (*vp == '\\')
				len += 2;
			else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
				len += 4;
			else
				len++;
		}

		/*
		 * In principle len can't overflow uint32 if the input fit in 1GB, but
		 * for safety let's check rather than relying on palloc's internal
		 * check.
		 */
		if (len > MaxAllocSize)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg_internal("result of bytea output conversion is too large")));
		rp = result = (char *) palloc(len);

		vp = VARDATA_ANY(vlena);
		for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++)
		{
			if (*vp == '\\')
			{
				*rp++ = '\\';
				*rp++ = '\\';
			}
			else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
			{
				int			val;	/* holds unprintable chars */

				val = *vp;
				rp[0] = '\\';
				rp[3] = DIG(val & 07);
				val >>= 3;
				rp[2] = DIG(val & 07);
				val >>= 3;
				rp[1] = DIG(val & 03);
				rp += 4;
			}
			else
				*rp++ = *vp;
		}
	}
	else
	{
		elog(ERROR, "unrecognized \"bytea_output\" setting: %d",
			 bytea_output);
		rp = result = NULL;		/* keep compiler quiet */
	}
	*rp = '\0';
	PG_RETURN_CSTRING(result);
}

/*
 *		bytearecv			- converts external binary format to bytea
 */
Datum
bytearecv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	bytea	   *result;
	int			nbytes;

	nbytes = buf->len - buf->cursor;
	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);
	pq_copymsgbytes(buf, VARDATA(result), nbytes);
	PG_RETURN_BYTEA_P(result);
}

/*
 *		byteasend			- converts bytea to binary format
 *
 * This is a special case: just copy the input...
 */
Datum
byteasend(PG_FUNCTION_ARGS)
{
	bytea	   *vlena = PG_GETARG_BYTEA_P_COPY(0);

	PG_RETURN_BYTEA_P(vlena);
}

Datum
bytea_string_agg_transfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	/* Append the value unless null, preceding it with the delimiter. */
	if (!PG_ARGISNULL(1))
	{
		bytea	   *value = PG_GETARG_BYTEA_PP(1);
		bool		isfirst = false;

		/*
		 * You might think we can just throw away the first delimiter, however
		 * we must keep it as we may be a parallel worker doing partial
		 * aggregation building a state to send to the main process.  We need
		 * to keep the delimiter of every aggregation so that the combine
		 * function can properly join up the strings of two separately
		 * partially aggregated results.  The first delimiter is only stripped
		 * off in the final function.  To know how much to strip off the front
		 * of the string, we store the length of the first delimiter in the
		 * StringInfo's cursor field, which we don't otherwise need here.
		 */
		if (state == NULL)
		{
			state = makeStringAggState(fcinfo);
			isfirst = true;
		}

		if (!PG_ARGISNULL(2))
		{
			bytea	   *delim = PG_GETARG_BYTEA_PP(2);

			appendBinaryStringInfo(state, VARDATA_ANY(delim),
								   VARSIZE_ANY_EXHDR(delim));
			if (isfirst)
				state->cursor = VARSIZE_ANY_EXHDR(delim);
		}

		appendBinaryStringInfo(state, VARDATA_ANY(value),
							   VARSIZE_ANY_EXHDR(value));
	}

	/*
	 * The transition type for string_agg() is declared to be "internal",
	 * which is a pass-by-value type the same size as a pointer.
	 */
	if (state)
		PG_RETURN_POINTER(state);
	PG_RETURN_NULL();
}

Datum
bytea_string_agg_finalfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	if (state != NULL)
	{
		/* As per comment in transfn, strip data before the cursor position */
		bytea	   *result;
		int			strippedlen = state->len - state->cursor;

		result = (bytea *) palloc(strippedlen + VARHDRSZ);
		SET_VARSIZE(result, strippedlen + VARHDRSZ);
		memcpy(VARDATA(result), &state->data[state->cursor], strippedlen);
		PG_RETURN_BYTEA_P(result);
	}
	else
		PG_RETURN_NULL();
}

/*-------------------------------------------------------------
 * byteaoctetlen
 *
 * get the number of bytes contained in an instance of type 'bytea'
 *-------------------------------------------------------------
 */
Datum
byteaoctetlen(PG_FUNCTION_ARGS)
{
	Datum		str = PG_GETARG_DATUM(0);

	/* We need not detoast the input at all */
	PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ);
}


