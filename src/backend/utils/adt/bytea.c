/*-------------------------------------------------------------------------
 *
 * bytea.c
 *	  Functions for the bytea type.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/bytea.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <limits.h>

#include "access/detoast.h"
#include "access/toast_compression.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "common/int.h"
#include "common/unicode_category.h"
#include "common/unicode_norm.h"
#include "common/unicode_version.h"
#include "funcapi.h"
#include "lib/hyperloglog.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "parser/scansup.h"
#include "port/pg_bswap.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/sortsupport.h"
#include "utils/varlena.h"


/* GUC variable */
int			bytea_output = BYTEA_OUTPUT_HEX;

typedef struct varlena VarString;

/*
 * State for text_position_* functions.
 * AALEKSEEV TODO DELETE
 */
typedef struct
{
	bool		is_multibyte_char_in_char;	/* need to check char boundaries? */

	char	   *str1;			/* haystack string */
	char	   *str2;			/* needle string */
	int			len1;			/* string lengths in bytes */
	int			len2;

	/* Skip table for Boyer-Moore-Horspool search algorithm: */
	int			skiptablemask;	/* mask for ANDing with skiptable subscripts */
	int			skiptable[256]; /* skip distance for given mismatched char */

	char	   *last_match;		/* pointer to last match in 'str1' */

	/*
	 * Sometimes we need to convert the byte position of a match to a
	 * character position.  These store the last position that was converted,
	 * so that on the next call, we can continue from that point, rather than
	 * count characters from the very beginning.
	 */
	char	   *refpoint;		/* pointer within original haystack string */
	int			refpos;			/* 0-based character offset of the same point */
} TextPositionState;

typedef struct
{
	char	   *buf1;			/* 1st string, or abbreviation original string
								 * buf */
	char	   *buf2;			/* 2nd string, or abbreviation strxfrm() buf */
	int			buflen1;		/* Allocated length of buf1 */
	int			buflen2;		/* Allocated length of buf2 */
	int			last_len1;		/* Length of last buf1 string/strxfrm() input */
	int			last_len2;		/* Length of last buf2 string/strxfrm() blob */
	int			last_returned;	/* Last comparison result (cache) */
	bool		cache_blob;		/* Does buf2 contain strxfrm() blob, etc? */
	bool		collate_c;
	Oid			typid;			/* Actual datatype (text/bpchar/bytea/name) */
	hyperLogLogState abbr_card; /* Abbreviated key cardinality state */
	hyperLogLogState full_card; /* Full key cardinality state */
	double		prop_card;		/* Required cardinality proportion */
	pg_locale_t locale;
} VarStringSortSupport;

/*
 * Output data for split_text(): we output either to an array or a table.
 * tupstore and tupdesc must be set up in advance to output to a table.
 */
typedef struct
{
	ArrayBuildState *astate;
	Tuplestorestate *tupstore;
	TupleDesc	tupdesc;
} SplitTextOutputData;

/*
 * This should be large enough that most strings will fit, but small enough
 * that we feel comfortable putting it on the stack
 */
#define TEXTBUFLEN		1024

#define DatumGetVarStringP(X)		((VarString *) PG_DETOAST_DATUM(X))
#define DatumGetVarStringPP(X)		((VarString *) PG_DETOAST_DATUM_PACKED(X))

static int	varstrfastcmp_c(Datum x, Datum y, SortSupport ssup);
static int	bpcharfastcmp_c(Datum x, Datum y, SortSupport ssup);
static int	namefastcmp_c(Datum x, Datum y, SortSupport ssup);
static int	varlenafastcmp_locale(Datum x, Datum y, SortSupport ssup);
static int	namefastcmp_locale(Datum x, Datum y, SortSupport ssup);
static int	varstrfastcmp_locale(char *a1p, int len1, char *a2p, int len2, SortSupport ssup);
static Datum varstr_abbrev_convert(Datum original, SortSupport ssup);
static bool varstr_abbrev_abort(int memtupcount, SortSupport ssup);
static int32 text_length(Datum str);
static text *text_catenate(text *t1, text *t2);
static text *text_substring(Datum str,
							int32 start,
							int32 length,
							bool length_not_specified);
static text *text_overlay(text *t1, text *t2, int sp, int sl);
static int	text_position(text *t1, text *t2, Oid collid);
static void text_position_setup(text *t1, text *t2, Oid collid, TextPositionState *state);
static bool text_position_next(TextPositionState *state);
static char *text_position_next_internal(char *start_ptr, TextPositionState *state);
static char *text_position_get_match_ptr(TextPositionState *state);
static int	text_position_get_match_pos(TextPositionState *state);
static void text_position_cleanup(TextPositionState *state);
static void check_collation_set(Oid collid);
static int	text_cmp(text *arg1, text *arg2, Oid collid);
static bytea *bytea_catenate(bytea *t1, bytea *t2);
static bytea *bytea_substring(Datum str,
							  int S,
							  int L,
							  bool length_not_specified);
static bytea *bytea_overlay(bytea *t1, bytea *t2, int sp, int sl);
static void appendStringInfoText(StringInfo str, const text *t);
static bool split_text(FunctionCallInfo fcinfo, SplitTextOutputData *tstate);
static void split_text_accum_result(SplitTextOutputData *tstate,
									text *field_value,
									text *null_string,
									Oid collation);
static text *array_to_text_internal(FunctionCallInfo fcinfo, ArrayType *v,
									const char *fldsep, const char *null_string);
static StringInfo makeStringAggState(FunctionCallInfo fcinfo);
static bool text_format_parse_digits(const char **ptr, const char *end_ptr,
									 int *value);
static const char *text_format_parse_format(const char *start_ptr,
											const char *end_ptr,
											int *argpos, int *widthpos,
											int *flags, int *width);
static void text_format_string_conversion(StringInfo buf, char conversion,
										  FmgrInfo *typOutputInfo,
										  Datum value, bool isNull,
										  int flags, int width);
static void text_format_append_string(StringInfo buf, const char *str,
									  int flags, int width);

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

/* ========== PUBLIC ROUTINES ========== */

/*
 * charlen_to_bytelen()
 *	Compute the number of bytes occupied by n characters starting at *p
 *
 * It is caller's responsibility that there actually are n characters;
 * the string need not be null-terminated.
 */
static int
charlen_to_bytelen(const char *p, int n)
{
	if (pg_database_encoding_max_length() == 1)
	{
		/* Optimization for single-byte encodings */
		return n;
	}
	else
	{
		const char *s;

		for (s = p; n > 0; n--)
			s += pg_mblen(s);

		return s - p;
	}
}


static void
check_collation_set(Oid collid)
{
	if (!OidIsValid(collid))
	{
		/*
		 * This typically means that the parser could not resolve a conflict
		 * of implicit collations, so report it that way.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string comparison"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));
	}
}

/*
 * varstr_cmp()
 * AALEKSEEV TODO DELELTE?
 *
 * Comparison function for text strings with given lengths, using the
 * appropriate locale. Returns an integer less than, equal to, or greater than
 * zero, indicating whether arg1 is less than, equal to, or greater than arg2.
 *
 * Note: many functions that depend on this are marked leakproof; therefore,
 * avoid reporting the actual contents of the input when throwing errors.
 * All errors herein should be things that can't happen except on corrupt
 * data, anyway; otherwise we will have trouble with indexing strings that
 * would cause them.
 */
int
varstr_cmp(const char *arg1, int len1, const char *arg2, int len2, Oid collid)
{
	int			result;
	pg_locale_t mylocale;

	check_collation_set(collid);

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->collate_is_c)
	{
		result = memcmp(arg1, arg2, Min(len1, len2));
		if ((result == 0) && (len1 != len2))
			result = (len1 < len2) ? -1 : 1;
	}
	else
	{
		/*
		 * memcmp() can't tell us which of two unequal strings sorts first,
		 * but it's a cheap way to tell if they're equal.  Testing shows that
		 * memcmp() followed by strcoll() is only trivially slower than
		 * strcoll() by itself, so we don't lose much if this doesn't work out
		 * very often, and if it does - for example, because there are many
		 * equal strings in the input - then we win big by avoiding expensive
		 * collation-aware comparisons.
		 */
		if (len1 == len2 && memcmp(arg1, arg2, len1) == 0)
			return 0;

		result = pg_strncoll(arg1, len1, arg2, len2, mylocale);

		/* Break tie if necessary. */
		if (result == 0 && mylocale->deterministic)
		{
			result = memcmp(arg1, arg2, Min(len1, len2));
			if ((result == 0) && (len1 != len2))
				result = (len1 < len2) ? -1 : 1;
		}
	}

	return result;
}

/*
 * Generic sortsupport interface for character type's operator classes.
 * Includes locale support, and support for BpChar semantics (i.e. removing
 * trailing spaces before comparison).
 *
 * Relies on the assumption that text, VarChar, BpChar, and bytea all have the
 * same representation.  Callers that always use the C collation (e.g.
 * non-collatable type callers like bytea) may have NUL bytes in their strings;
 * this will not work with any other collation, though.
 */
void
varstr_sortsupport(SortSupport ssup, Oid typid, Oid collid)
{
	bool		abbreviate = ssup->abbreviate;
	bool		collate_c = false;
	VarStringSortSupport *sss;
	pg_locale_t locale;

	check_collation_set(collid);

	locale = pg_newlocale_from_collation(collid);

	/*
	 * If possible, set ssup->comparator to a function which can be used to
	 * directly compare two datums.  If we can do this, we'll avoid the
	 * overhead of a trip through the fmgr layer for every comparison, which
	 * can be substantial.
	 *
	 * Most typically, we'll set the comparator to varlenafastcmp_locale,
	 * which uses strcoll() to perform comparisons.  We use that for the
	 * BpChar case too, but type NAME uses namefastcmp_locale. However, if
	 * LC_COLLATE = C, we can make things quite a bit faster with
	 * varstrfastcmp_c, bpcharfastcmp_c, or namefastcmp_c, all of which use
	 * memcmp() rather than strcoll().
	 */
	if (locale->collate_is_c)
	{
		if (typid == BPCHAROID)
			ssup->comparator = bpcharfastcmp_c;
		else if (typid == NAMEOID)
		{
			ssup->comparator = namefastcmp_c;
			/* Not supporting abbreviation with type NAME, for now */
			abbreviate = false;
		}
		else
			ssup->comparator = varstrfastcmp_c;

		collate_c = true;
	}
	else
	{
		/*
		 * We use varlenafastcmp_locale except for type NAME.
		 */
		if (typid == NAMEOID)
		{
			ssup->comparator = namefastcmp_locale;
			/* Not supporting abbreviation with type NAME, for now */
			abbreviate = false;
		}
		else
			ssup->comparator = varlenafastcmp_locale;

		/*
		 * Unfortunately, it seems that abbreviation for non-C collations is
		 * broken on many common platforms; see pg_strxfrm_enabled().
		 *
		 * Even apart from the risk of broken locales, it's possible that
		 * there are platforms where the use of abbreviated keys should be
		 * disabled at compile time.  Having only 4 byte datums could make
		 * worst-case performance drastically more likely, for example.
		 * Moreover, macOS's strxfrm() implementation is known to not
		 * effectively concentrate a significant amount of entropy from the
		 * original string in earlier transformed blobs.  It's possible that
		 * other supported platforms are similarly encumbered.  So, if we ever
		 * get past disabling this categorically, we may still want or need to
		 * disable it for particular platforms.
		 */
		if (!pg_strxfrm_enabled(locale))
			abbreviate = false;
	}

	/*
	 * If we're using abbreviated keys, or if we're using a locale-aware
	 * comparison, we need to initialize a VarStringSortSupport object. Both
	 * cases will make use of the temporary buffers we initialize here for
	 * scratch space (and to detect requirement for BpChar semantics from
	 * caller), and the abbreviation case requires additional state.
	 */
	if (abbreviate || !collate_c)
	{
		sss = palloc(sizeof(VarStringSortSupport));
		sss->buf1 = palloc(TEXTBUFLEN);
		sss->buflen1 = TEXTBUFLEN;
		sss->buf2 = palloc(TEXTBUFLEN);
		sss->buflen2 = TEXTBUFLEN;
		/* Start with invalid values */
		sss->last_len1 = -1;
		sss->last_len2 = -1;
		/* Initialize */
		sss->last_returned = 0;
		if (collate_c)
			sss->locale = NULL;
		else
			sss->locale = locale;

		/*
		 * To avoid somehow confusing a strxfrm() blob and an original string,
		 * constantly keep track of the variety of data that buf1 and buf2
		 * currently contain.
		 *
		 * Comparisons may be interleaved with conversion calls.  Frequently,
		 * conversions and comparisons are batched into two distinct phases,
		 * but the correctness of caching cannot hinge upon this.  For
		 * comparison caching, buffer state is only trusted if cache_blob is
		 * found set to false, whereas strxfrm() caching only trusts the state
		 * when cache_blob is found set to true.
		 *
		 * Arbitrarily initialize cache_blob to true.
		 */
		sss->cache_blob = true;
		sss->collate_c = collate_c;
		sss->typid = typid;
		ssup->ssup_extra = sss;

		/*
		 * If possible, plan to use the abbreviated keys optimization.  The
		 * core code may switch back to authoritative comparator should
		 * abbreviation be aborted.
		 */
		if (abbreviate)
		{
			sss->prop_card = 0.20;
			initHyperLogLog(&sss->abbr_card, 10);
			initHyperLogLog(&sss->full_card, 10);
			ssup->abbrev_full_comparator = ssup->comparator;
			ssup->comparator = ssup_datum_unsigned_cmp;
			ssup->abbrev_converter = varstr_abbrev_convert;
			ssup->abbrev_abort = varstr_abbrev_abort;
		}
	}
}

/*
 * sortsupport comparison func (for C locale case)
 */
static int
varstrfastcmp_c(Datum x, Datum y, SortSupport ssup)
{
	VarString  *arg1 = DatumGetVarStringPP(x);
	VarString  *arg2 = DatumGetVarStringPP(y);
	char	   *a1p,
			   *a2p;
	int			len1,
				len2,
				result;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	result = memcmp(a1p, a2p, Min(len1, len2));
	if ((result == 0) && (len1 != len2))
		result = (len1 < len2) ? -1 : 1;

	/* We can't afford to leak memory here. */
	if (PointerGetDatum(arg1) != x)
		pfree(arg1);
	if (PointerGetDatum(arg2) != y)
		pfree(arg2);

	return result;
}

/*
 * sortsupport comparison func (for BpChar C locale case)
 *
 * BpChar outsources its sortsupport to this module.  Specialization for the
 * varstr_sortsupport BpChar case, modeled on
 * internal_bpchar_pattern_compare().
 */
static int
bpcharfastcmp_c(Datum x, Datum y, SortSupport ssup)
{
	BpChar	   *arg1 = DatumGetBpCharPP(x);
	BpChar	   *arg2 = DatumGetBpCharPP(y);
	char	   *a1p,
			   *a2p;
	int			len1,
				len2,
				result;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = bpchartruelen(a1p, VARSIZE_ANY_EXHDR(arg1));
	len2 = bpchartruelen(a2p, VARSIZE_ANY_EXHDR(arg2));

	result = memcmp(a1p, a2p, Min(len1, len2));
	if ((result == 0) && (len1 != len2))
		result = (len1 < len2) ? -1 : 1;

	/* We can't afford to leak memory here. */
	if (PointerGetDatum(arg1) != x)
		pfree(arg1);
	if (PointerGetDatum(arg2) != y)
		pfree(arg2);

	return result;
}

/*
 * sortsupport comparison func (for NAME C locale case)
 */
static int
namefastcmp_c(Datum x, Datum y, SortSupport ssup)
{
	Name		arg1 = DatumGetName(x);
	Name		arg2 = DatumGetName(y);

	return strncmp(NameStr(*arg1), NameStr(*arg2), NAMEDATALEN);
}

/*
 * sortsupport comparison func (for locale case with all varlena types)
 */
static int
varlenafastcmp_locale(Datum x, Datum y, SortSupport ssup)
{
	VarString  *arg1 = DatumGetVarStringPP(x);
	VarString  *arg2 = DatumGetVarStringPP(y);
	char	   *a1p,
			   *a2p;
	int			len1,
				len2,
				result;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	result = varstrfastcmp_locale(a1p, len1, a2p, len2, ssup);

	/* We can't afford to leak memory here. */
	if (PointerGetDatum(arg1) != x)
		pfree(arg1);
	if (PointerGetDatum(arg2) != y)
		pfree(arg2);

	return result;
}

/*
 * sortsupport comparison func (for locale case with NAME type)
 */
static int
namefastcmp_locale(Datum x, Datum y, SortSupport ssup)
{
	Name		arg1 = DatumGetName(x);
	Name		arg2 = DatumGetName(y);

	return varstrfastcmp_locale(NameStr(*arg1), strlen(NameStr(*arg1)),
								NameStr(*arg2), strlen(NameStr(*arg2)),
								ssup);
}

/*
 * sortsupport comparison func for locale cases
 */
static int
varstrfastcmp_locale(char *a1p, int len1, char *a2p, int len2, SortSupport ssup)
{
	VarStringSortSupport *sss = (VarStringSortSupport *) ssup->ssup_extra;
	int			result;
	bool		arg1_match;

	/* Fast pre-check for equality, as discussed in varstr_cmp() */
	if (len1 == len2 && memcmp(a1p, a2p, len1) == 0)
	{
		/*
		 * No change in buf1 or buf2 contents, so avoid changing last_len1 or
		 * last_len2.  Existing contents of buffers might still be used by
		 * next call.
		 *
		 * It's fine to allow the comparison of BpChar padding bytes here,
		 * even though that implies that the memcmp() will usually be
		 * performed for BpChar callers (though multibyte characters could
		 * still prevent that from occurring).  The memcmp() is still very
		 * cheap, and BpChar's funny semantics have us remove trailing spaces
		 * (not limited to padding), so we need make no distinction between
		 * padding space characters and "real" space characters.
		 */
		return 0;
	}

	if (sss->typid == BPCHAROID)
	{
		/* Get true number of bytes, ignoring trailing spaces */
		len1 = bpchartruelen(a1p, len1);
		len2 = bpchartruelen(a2p, len2);
	}

	if (len1 >= sss->buflen1)
	{
		sss->buflen1 = Max(len1 + 1, Min(sss->buflen1 * 2, MaxAllocSize));
		sss->buf1 = repalloc(sss->buf1, sss->buflen1);
	}
	if (len2 >= sss->buflen2)
	{
		sss->buflen2 = Max(len2 + 1, Min(sss->buflen2 * 2, MaxAllocSize));
		sss->buf2 = repalloc(sss->buf2, sss->buflen2);
	}

	/*
	 * We're likely to be asked to compare the same strings repeatedly, and
	 * memcmp() is so much cheaper than strcoll() that it pays to try to cache
	 * comparisons, even though in general there is no reason to think that
	 * that will work out (every string datum may be unique).  Caching does
	 * not slow things down measurably when it doesn't work out, and can speed
	 * things up by rather a lot when it does.  In part, this is because the
	 * memcmp() compares data from cachelines that are needed in L1 cache even
	 * when the last comparison's result cannot be reused.
	 */
	arg1_match = true;
	if (len1 != sss->last_len1 || memcmp(sss->buf1, a1p, len1) != 0)
	{
		arg1_match = false;
		memcpy(sss->buf1, a1p, len1);
		sss->buf1[len1] = '\0';
		sss->last_len1 = len1;
	}

	/*
	 * If we're comparing the same two strings as last time, we can return the
	 * same answer without calling strcoll() again.  This is more likely than
	 * it seems (at least with moderate to low cardinality sets), because
	 * quicksort compares the same pivot against many values.
	 */
	if (len2 != sss->last_len2 || memcmp(sss->buf2, a2p, len2) != 0)
	{
		memcpy(sss->buf2, a2p, len2);
		sss->buf2[len2] = '\0';
		sss->last_len2 = len2;
	}
	else if (arg1_match && !sss->cache_blob)
	{
		/* Use result cached following last actual strcoll() call */
		return sss->last_returned;
	}

	result = pg_strcoll(sss->buf1, sss->buf2, sss->locale);

	/* Break tie if necessary. */
	if (result == 0 && sss->locale->deterministic)
		result = strcmp(sss->buf1, sss->buf2);

	/* Cache result, perhaps saving an expensive strcoll() call next time */
	sss->cache_blob = false;
	sss->last_returned = result;
	return result;
}

/*
 * Conversion routine for sortsupport.  Converts original to abbreviated key
 * representation.  Our encoding strategy is simple -- pack the first 8 bytes
 * of a strxfrm() blob into a Datum (on little-endian machines, the 8 bytes are
 * stored in reverse order), and treat it as an unsigned integer.  When the "C"
 * locale is used, or in case of bytea, just memcpy() from original instead.
 */
static Datum
varstr_abbrev_convert(Datum original, SortSupport ssup)
{
	const size_t max_prefix_bytes = sizeof(Datum);
	VarStringSortSupport *sss = (VarStringSortSupport *) ssup->ssup_extra;
	VarString  *authoritative = DatumGetVarStringPP(original);
	char	   *authoritative_data = VARDATA_ANY(authoritative);

	/* working state */
	Datum		res;
	char	   *pres;
	int			len;
	uint32		hash;

	pres = (char *) &res;
	/* memset(), so any non-overwritten bytes are NUL */
	memset(pres, 0, max_prefix_bytes);
	len = VARSIZE_ANY_EXHDR(authoritative);

	/* Get number of bytes, ignoring trailing spaces */
	if (sss->typid == BPCHAROID)
		len = bpchartruelen(authoritative_data, len);

	/*
	 * If we're using the C collation, use memcpy(), rather than strxfrm(), to
	 * abbreviate keys.  The full comparator for the C locale is always
	 * memcmp().  It would be incorrect to allow bytea callers (callers that
	 * always force the C collation -- bytea isn't a collatable type, but this
	 * approach is convenient) to use strxfrm().  This is because bytea
	 * strings may contain NUL bytes.  Besides, this should be faster, too.
	 *
	 * More generally, it's okay that bytea callers can have NUL bytes in
	 * strings because abbreviated cmp need not make a distinction between
	 * terminating NUL bytes, and NUL bytes representing actual NULs in the
	 * authoritative representation.  Hopefully a comparison at or past one
	 * abbreviated key's terminating NUL byte will resolve the comparison
	 * without consulting the authoritative representation; specifically, some
	 * later non-NUL byte in the longer string can resolve the comparison
	 * against a subsequent terminating NUL in the shorter string.  There will
	 * usually be what is effectively a "length-wise" resolution there and
	 * then.
	 *
	 * If that doesn't work out -- if all bytes in the longer string
	 * positioned at or past the offset of the smaller string's (first)
	 * terminating NUL are actually representative of NUL bytes in the
	 * authoritative binary string (perhaps with some *terminating* NUL bytes
	 * towards the end of the longer string iff it happens to still be small)
	 * -- then an authoritative tie-breaker will happen, and do the right
	 * thing: explicitly consider string length.
	 */
	if (sss->collate_c)
		memcpy(pres, authoritative_data, Min(len, max_prefix_bytes));
	else
	{
		Size		bsize;

		/*
		 * We're not using the C collation, so fall back on strxfrm or ICU
		 * analogs.
		 */

		/* By convention, we use buffer 1 to store and NUL-terminate */
		if (len >= sss->buflen1)
		{
			sss->buflen1 = Max(len + 1, Min(sss->buflen1 * 2, MaxAllocSize));
			sss->buf1 = repalloc(sss->buf1, sss->buflen1);
		}

		/* Might be able to reuse strxfrm() blob from last call */
		if (sss->last_len1 == len && sss->cache_blob &&
			memcmp(sss->buf1, authoritative_data, len) == 0)
		{
			memcpy(pres, sss->buf2, Min(max_prefix_bytes, sss->last_len2));
			/* No change affecting cardinality, so no hashing required */
			goto done;
		}

		memcpy(sss->buf1, authoritative_data, len);

		/*
		 * pg_strxfrm() and pg_strxfrm_prefix expect NUL-terminated strings.
		 */
		sss->buf1[len] = '\0';
		sss->last_len1 = len;

		if (pg_strxfrm_prefix_enabled(sss->locale))
		{
			if (sss->buflen2 < max_prefix_bytes)
			{
				sss->buflen2 = Max(max_prefix_bytes,
								   Min(sss->buflen2 * 2, MaxAllocSize));
				sss->buf2 = repalloc(sss->buf2, sss->buflen2);
			}

			bsize = pg_strxfrm_prefix(sss->buf2, sss->buf1,
									  max_prefix_bytes, sss->locale);
			sss->last_len2 = bsize;
		}
		else
		{
			/*
			 * Loop: Call pg_strxfrm(), possibly enlarge buffer, and try
			 * again.  The pg_strxfrm() function leaves the result buffer
			 * content undefined if the result did not fit, so we need to
			 * retry until everything fits, even though we only need the first
			 * few bytes in the end.
			 */
			for (;;)
			{
				bsize = pg_strxfrm(sss->buf2, sss->buf1, sss->buflen2,
								   sss->locale);

				sss->last_len2 = bsize;
				if (bsize < sss->buflen2)
					break;

				/*
				 * Grow buffer and retry.
				 */
				sss->buflen2 = Max(bsize + 1,
								   Min(sss->buflen2 * 2, MaxAllocSize));
				sss->buf2 = repalloc(sss->buf2, sss->buflen2);
			}
		}

		/*
		 * Every Datum byte is always compared.  This is safe because the
		 * strxfrm() blob is itself NUL terminated, leaving no danger of
		 * misinterpreting any NUL bytes not intended to be interpreted as
		 * logically representing termination.
		 *
		 * (Actually, even if there were NUL bytes in the blob it would be
		 * okay.  See remarks on bytea case above.)
		 */
		memcpy(pres, sss->buf2, Min(max_prefix_bytes, bsize));
	}

	/*
	 * Maintain approximate cardinality of both abbreviated keys and original,
	 * authoritative keys using HyperLogLog.  Used as cheap insurance against
	 * the worst case, where we do many string transformations for no saving
	 * in full strcoll()-based comparisons.  These statistics are used by
	 * varstr_abbrev_abort().
	 *
	 * First, Hash key proper, or a significant fraction of it.  Mix in length
	 * in order to compensate for cases where differences are past
	 * PG_CACHE_LINE_SIZE bytes, so as to limit the overhead of hashing.
	 */
	hash = DatumGetUInt32(hash_any((unsigned char *) authoritative_data,
								   Min(len, PG_CACHE_LINE_SIZE)));

	if (len > PG_CACHE_LINE_SIZE)
		hash ^= DatumGetUInt32(hash_uint32((uint32) len));

	addHyperLogLog(&sss->full_card, hash);

	/* Hash abbreviated key */
#if SIZEOF_DATUM == 8
	{
		uint32		lohalf,
					hihalf;

		lohalf = (uint32) res;
		hihalf = (uint32) (res >> 32);
		hash = DatumGetUInt32(hash_uint32(lohalf ^ hihalf));
	}
#else							/* SIZEOF_DATUM != 8 */
	hash = DatumGetUInt32(hash_uint32((uint32) res));
#endif

	addHyperLogLog(&sss->abbr_card, hash);

	/* Cache result, perhaps saving an expensive strxfrm() call next time */
	sss->cache_blob = true;
done:

	/*
	 * Byteswap on little-endian machines.
	 *
	 * This is needed so that ssup_datum_unsigned_cmp() (an unsigned integer
	 * 3-way comparator) works correctly on all platforms.  If we didn't do
	 * this, the comparator would have to call memcmp() with a pair of
	 * pointers to the first byte of each abbreviated key, which is slower.
	 */
	res = DatumBigEndianToNative(res);

	/* Don't leak memory here */
	if (PointerGetDatum(authoritative) != original)
		pfree(authoritative);

	return res;
}

/*
 * Callback for estimating effectiveness of abbreviated key optimization, using
 * heuristic rules.  Returns value indicating if the abbreviation optimization
 * should be aborted, based on its projected effectiveness.
 */
static bool
varstr_abbrev_abort(int memtupcount, SortSupport ssup)
{
	VarStringSortSupport *sss = (VarStringSortSupport *) ssup->ssup_extra;
	double		abbrev_distinct,
				key_distinct;

	Assert(ssup->abbreviate);

	/* Have a little patience */
	if (memtupcount < 100)
		return false;

	abbrev_distinct = estimateHyperLogLog(&sss->abbr_card);
	key_distinct = estimateHyperLogLog(&sss->full_card);

	/*
	 * Clamp cardinality estimates to at least one distinct value.  While
	 * NULLs are generally disregarded, if only NULL values were seen so far,
	 * that might misrepresent costs if we failed to clamp.
	 */
	if (abbrev_distinct <= 1.0)
		abbrev_distinct = 1.0;

	if (key_distinct <= 1.0)
		key_distinct = 1.0;

	/*
	 * In the worst case all abbreviated keys are identical, while at the same
	 * time there are differences within full key strings not captured in
	 * abbreviations.
	 */
	if (trace_sort)
	{
		double		norm_abbrev_card = abbrev_distinct / (double) memtupcount;

		elog(LOG, "varstr_abbrev: abbrev_distinct after %d: %f "
			 "(key_distinct: %f, norm_abbrev_card: %f, prop_card: %f)",
			 memtupcount, abbrev_distinct, key_distinct, norm_abbrev_card,
			 sss->prop_card);
	}

	/*
	 * If the number of distinct abbreviated keys approximately matches the
	 * number of distinct authoritative original keys, that's reason enough to
	 * proceed.  We can win even with a very low cardinality set if most
	 * tie-breakers only memcmp().  This is by far the most important
	 * consideration.
	 *
	 * While comparisons that are resolved at the abbreviated key level are
	 * considerably cheaper than tie-breakers resolved with memcmp(), both of
	 * those two outcomes are so much cheaper than a full strcoll() once
	 * sorting is underway that it doesn't seem worth it to weigh abbreviated
	 * cardinality against the overall size of the set in order to more
	 * accurately model costs.  Assume that an abbreviated comparison, and an
	 * abbreviated comparison with a cheap memcmp()-based authoritative
	 * resolution are equivalent.
	 */
	if (abbrev_distinct > key_distinct * sss->prop_card)
	{
		/*
		 * When we have exceeded 10,000 tuples, decay required cardinality
		 * aggressively for next call.
		 *
		 * This is useful because the number of comparisons required on
		 * average increases at a linearithmic rate, and at roughly 10,000
		 * tuples that factor will start to dominate over the linear costs of
		 * string transformation (this is a conservative estimate).  The decay
		 * rate is chosen to be a little less aggressive than halving -- which
		 * (since we're called at points at which memtupcount has doubled)
		 * would never see the cost model actually abort past the first call
		 * following a decay.  This decay rate is mostly a precaution against
		 * a sudden, violent swing in how well abbreviated cardinality tracks
		 * full key cardinality.  The decay also serves to prevent a marginal
		 * case from being aborted too late, when too much has already been
		 * invested in string transformation.
		 *
		 * It's possible for sets of several million distinct strings with
		 * mere tens of thousands of distinct abbreviated keys to still
		 * benefit very significantly.  This will generally occur provided
		 * each abbreviated key is a proxy for a roughly uniform number of the
		 * set's full keys. If it isn't so, we hope to catch that early and
		 * abort.  If it isn't caught early, by the time the problem is
		 * apparent it's probably not worth aborting.
		 */
		if (memtupcount > 10000)
			sss->prop_card *= 0.65;

		return false;
	}

	/*
	 * Abort abbreviation strategy.
	 *
	 * The worst case, where all abbreviated keys are identical while all
	 * original strings differ will typically only see a regression of about
	 * 10% in execution time for small to medium sized lists of strings.
	 * Whereas on modern CPUs where cache stalls are the dominant cost, we can
	 * often expect very large improvements, particularly with sets of strings
	 * of moderately high to high abbreviated cardinality.  There is little to
	 * lose but much to gain, which our strategy reflects.
	 */
	if (trace_sort)
		elog(LOG, "varstr_abbrev: aborted abbreviation at %d "
			 "(abbrev_distinct: %f, key_distinct: %f, prop_card: %f)",
			 memtupcount, abbrev_distinct, key_distinct, sss->prop_card);

	return true;
}

/*
 * Generic equalimage support function for character type's operator classes.
 * Disables the use of deduplication with nondeterministic collations.
 */
Datum
btvarstrequalimage(PG_FUNCTION_ARGS)
{
	/* Oid		opcintype = PG_GETARG_OID(0); */
	Oid			collid = PG_GET_COLLATION();
	pg_locale_t locale;

	check_collation_set(collid);

	locale = pg_newlocale_from_collation(collid);

	PG_RETURN_BOOL(locale->deterministic);
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

/*
 * byteacat -
 *	  takes two bytea* and returns a bytea* that is the concatenation of
 *	  the two.
 *
 * Cloned from textcat and modified as required.
 */
Datum
byteacat(PG_FUNCTION_ARGS)
{
	bytea	   *t1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *t2 = PG_GETARG_BYTEA_PP(1);

	PG_RETURN_BYTEA_P(bytea_catenate(t1, t2));
}

/*
 * bytea_catenate
 *	Guts of byteacat(), broken out so it can be used by other functions
 *
 * Arguments can be in short-header form, but not compressed or out-of-line
 */
static bytea *
bytea_catenate(bytea *t1, bytea *t2)
{
	bytea	   *result;
	int			len1,
				len2,
				len;
	char	   *ptr;

	len1 = VARSIZE_ANY_EXHDR(t1);
	len2 = VARSIZE_ANY_EXHDR(t2);

	/* paranoia ... probably should throw error instead? */
	if (len1 < 0)
		len1 = 0;
	if (len2 < 0)
		len2 = 0;

	len = len1 + len2 + VARHDRSZ;
	result = (bytea *) palloc(len);

	/* Set size of result string... */
	SET_VARSIZE(result, len);

	/* Fill data field of result string... */
	ptr = VARDATA(result);
	if (len1 > 0)
		memcpy(ptr, VARDATA_ANY(t1), len1);
	if (len2 > 0)
		memcpy(ptr + len1, VARDATA_ANY(t2), len2);

	return result;
}

#define PG_STR_GET_BYTEA(str_) \
	DatumGetByteaPP(DirectFunctionCall1(byteain, CStringGetDatum(str_)))

/*
 * bytea_substr()
 * Return a substring starting at the specified position.
 * Cloned from text_substr and modified as required.
 *
 * Input:
 *	- string
 *	- starting position (is one-based)
 *	- string length (optional)
 *
 * If the starting position is zero or less, then return from the start of the string
 * adjusting the length to be consistent with the "negative start" per SQL.
 * If the length is less than zero, an ERROR is thrown. If no third argument
 * (length) is provided, the length to the end of the string is assumed.
 */
Datum
bytea_substr(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(bytea_substring(PG_GETARG_DATUM(0),
									  PG_GETARG_INT32(1),
									  PG_GETARG_INT32(2),
									  false));
}

/*
 * bytea_substr_no_len -
 *	  Wrapper to avoid opr_sanity failure due to
 *	  one function accepting a different number of args.
 */
Datum
bytea_substr_no_len(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(bytea_substring(PG_GETARG_DATUM(0),
									  PG_GETARG_INT32(1),
									  -1,
									  true));
}

static bytea *
bytea_substring(Datum str,
				int S,
				int L,
				bool length_not_specified)
{
	int32		S1;				/* adjusted start position */
	int32		L1;				/* adjusted substring length */
	int32		E;				/* end position */

	/*
	 * The logic here should generally match text_substring().
	 */
	S1 = Max(S, 1);

	if (length_not_specified)
	{
		/*
		 * Not passed a length - DatumGetByteaPSlice() grabs everything to the
		 * end of the string if we pass it a negative value for length.
		 */
		L1 = -1;
	}
	else if (L < 0)
	{
		/* SQL99 says to throw an error for E < S, i.e., negative length */
		ereport(ERROR,
				(errcode(ERRCODE_SUBSTRING_ERROR),
				 errmsg("negative substring length not allowed")));
		L1 = -1;				/* silence stupider compilers */
	}
	else if (pg_add_s32_overflow(S, L, &E))
	{
		/*
		 * L could be large enough for S + L to overflow, in which case the
		 * substring must run to end of string.
		 */
		L1 = -1;
	}
	else
	{
		/*
		 * A zero or negative value for the end position can happen if the
		 * start was negative or one. SQL99 says to return a zero-length
		 * string.
		 */
		if (E < 1)
			return PG_STR_GET_BYTEA("");

		L1 = E - S1;
	}

	/*
	 * If the start position is past the end of the string, SQL99 says to
	 * return a zero-length string -- DatumGetByteaPSlice() will do that for
	 * us.  We need only convert S1 to zero-based starting position.
	 */
	return DatumGetByteaPSlice(str, S1 - 1, L1);
}

/*
 * byteaoverlay
 *	Replace specified substring of first string with second
 *
 * The SQL standard defines OVERLAY() in terms of substring and concatenation.
 * This code is a direct implementation of what the standard says.
 */
Datum
byteaoverlay(PG_FUNCTION_ARGS)
{
	bytea	   *t1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *t2 = PG_GETARG_BYTEA_PP(1);
	int			sp = PG_GETARG_INT32(2);	/* substring start position */
	int			sl = PG_GETARG_INT32(3);	/* substring length */

	PG_RETURN_BYTEA_P(bytea_overlay(t1, t2, sp, sl));
}

Datum
byteaoverlay_no_len(PG_FUNCTION_ARGS)
{
	bytea	   *t1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *t2 = PG_GETARG_BYTEA_PP(1);
	int			sp = PG_GETARG_INT32(2);	/* substring start position */
	int			sl;

	sl = VARSIZE_ANY_EXHDR(t2); /* defaults to length(t2) */
	PG_RETURN_BYTEA_P(bytea_overlay(t1, t2, sp, sl));
}

static bytea *
bytea_overlay(bytea *t1, bytea *t2, int sp, int sl)
{
	bytea	   *result;
	bytea	   *s1;
	bytea	   *s2;
	int			sp_pl_sl;

	/*
	 * Check for possible integer-overflow cases.  For negative sp, throw a
	 * "substring length" error because that's what should be expected
	 * according to the spec's definition of OVERLAY().
	 */
	if (sp <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_SUBSTRING_ERROR),
				 errmsg("negative substring length not allowed")));
	if (pg_add_s32_overflow(sp, sl, &sp_pl_sl))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	s1 = bytea_substring(PointerGetDatum(t1), 1, sp - 1, false);
	s2 = bytea_substring(PointerGetDatum(t1), sp_pl_sl, -1, true);
	result = bytea_catenate(s1, t2);
	result = bytea_catenate(result, s2);

	return result;
}

/*
 * bit_count
 */
Datum
bytea_bit_count(PG_FUNCTION_ARGS)
{
	bytea	   *t1 = PG_GETARG_BYTEA_PP(0);

	PG_RETURN_INT64(pg_popcount(VARDATA_ANY(t1), VARSIZE_ANY_EXHDR(t1)));
}

/*
 * byteapos -
 *	  Return the position of the specified substring.
 *	  Implements the SQL POSITION() function.
 * Cloned from textpos and modified as required.
 */
Datum
byteapos(PG_FUNCTION_ARGS)
{
	bytea	   *t1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *t2 = PG_GETARG_BYTEA_PP(1);
	int			pos;
	int			px,
				p;
	int			len1,
				len2;
	char	   *p1,
			   *p2;

	len1 = VARSIZE_ANY_EXHDR(t1);
	len2 = VARSIZE_ANY_EXHDR(t2);

	if (len2 <= 0)
		PG_RETURN_INT32(1);		/* result for empty pattern */

	p1 = VARDATA_ANY(t1);
	p2 = VARDATA_ANY(t2);

	pos = 0;
	px = (len1 - len2);
	for (p = 0; p <= px; p++)
	{
		if ((*p2 == *p1) && (memcmp(p1, p2, len2) == 0))
		{
			pos = p + 1;
			break;
		};
		p1++;
	};

	PG_RETURN_INT32(pos);
}

/*-------------------------------------------------------------
 * byteaGetByte
 *
 * this routine treats "bytea" as an array of bytes.
 * It returns the Nth byte (a number between 0 and 255).
 *-------------------------------------------------------------
 */
Datum
byteaGetByte(PG_FUNCTION_ARGS)
{
	bytea	   *v = PG_GETARG_BYTEA_PP(0);
	int32		n = PG_GETARG_INT32(1);
	int			len;
	int			byte;

	len = VARSIZE_ANY_EXHDR(v);

	if (n < 0 || n >= len)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("index %d out of valid range, 0..%d",
						n, len - 1)));

	byte = ((unsigned char *) VARDATA_ANY(v))[n];

	PG_RETURN_INT32(byte);
}

/*-------------------------------------------------------------
 * byteaGetBit
 *
 * This routine treats a "bytea" type like an array of bits.
 * It returns the value of the Nth bit (0 or 1).
 *
 *-------------------------------------------------------------
 */
Datum
byteaGetBit(PG_FUNCTION_ARGS)
{
	bytea	   *v = PG_GETARG_BYTEA_PP(0);
	int64		n = PG_GETARG_INT64(1);
	int			byteNo,
				bitNo;
	int			len;
	int			byte;

	len = VARSIZE_ANY_EXHDR(v);

	if (n < 0 || n >= (int64) len * 8)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("index %lld out of valid range, 0..%lld",
						(long long) n, (long long) len * 8 - 1)));

	/* n/8 is now known < len, so safe to cast to int */
	byteNo = (int) (n / 8);
	bitNo = (int) (n % 8);

	byte = ((unsigned char *) VARDATA_ANY(v))[byteNo];

	if (byte & (1 << bitNo))
		PG_RETURN_INT32(1);
	else
		PG_RETURN_INT32(0);
}

/*-------------------------------------------------------------
 * byteaSetByte
 *
 * Given an instance of type 'bytea' creates a new one with
 * the Nth byte set to the given value.
 *
 *-------------------------------------------------------------
 */
Datum
byteaSetByte(PG_FUNCTION_ARGS)
{
	bytea	   *res = PG_GETARG_BYTEA_P_COPY(0);
	int32		n = PG_GETARG_INT32(1);
	int32		newByte = PG_GETARG_INT32(2);
	int			len;

	len = VARSIZE(res) - VARHDRSZ;

	if (n < 0 || n >= len)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("index %d out of valid range, 0..%d",
						n, len - 1)));

	/*
	 * Now set the byte.
	 */
	((unsigned char *) VARDATA(res))[n] = newByte;

	PG_RETURN_BYTEA_P(res);
}

/*-------------------------------------------------------------
 * byteaSetBit
 *
 * Given an instance of type 'bytea' creates a new one with
 * the Nth bit set to the given value.
 *
 *-------------------------------------------------------------
 */
Datum
byteaSetBit(PG_FUNCTION_ARGS)
{
	bytea	   *res = PG_GETARG_BYTEA_P_COPY(0);
	int64		n = PG_GETARG_INT64(1);
	int32		newBit = PG_GETARG_INT32(2);
	int			len;
	int			oldByte,
				newByte;
	int			byteNo,
				bitNo;

	len = VARSIZE(res) - VARHDRSZ;

	if (n < 0 || n >= (int64) len * 8)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("index %lld out of valid range, 0..%lld",
						(long long) n, (long long) len * 8 - 1)));

	/* n/8 is now known < len, so safe to cast to int */
	byteNo = (int) (n / 8);
	bitNo = (int) (n % 8);

	/*
	 * sanity check!
	 */
	if (newBit != 0 && newBit != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("new bit must be 0 or 1")));

	/*
	 * Update the byte.
	 */
	oldByte = ((unsigned char *) VARDATA(res))[byteNo];

	if (newBit == 0)
		newByte = oldByte & (~(1 << bitNo));
	else
		newByte = oldByte | (1 << bitNo);

	((unsigned char *) VARDATA(res))[byteNo] = newByte;

	PG_RETURN_BYTEA_P(res);
}

/*
 * SplitIdentifierString --- parse a string containing identifiers
 * AALEKSEEV TODO DELETE?
 *
 * This is the guts of textToQualifiedNameList, and is exported for use in
 * other situations such as parsing GUC variables.  In the GUC case, it's
 * important to avoid memory leaks, so the API is designed to minimize the
 * amount of stuff that needs to be allocated and freed.
 *
 * Inputs:
 *	rawstring: the input string; must be overwritable!	On return, it's
 *			   been modified to contain the separated identifiers.
 *	separator: the separator punctuation expected between identifiers
 *			   (typically '.' or ',').  Whitespace may also appear around
 *			   identifiers.
 * Outputs:
 *	namelist: filled with a palloc'd list of pointers to identifiers within
 *			  rawstring.  Caller should list_free() this even on error return.
 *
 * Returns true if okay, false if there is a syntax error in the string.
 *
 * Note that an empty string is considered okay here, though not in
 * textToQualifiedNameList.
 */
bool
SplitIdentifierString(char *rawstring, char separator,
					  List **namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;

	*namelist = NIL;

	while (scanner_isspace(*nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '"')
		{
			/* Quoted name --- collapse quote-quote pairs, no downcasing */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					return false;	/* mismatched quotes */
				if (endp[1] != '"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			char	   *downname;
			int			len;

			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !scanner_isspace(*nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */

			/*
			 * Downcase the identifier, using same code as main lexer does.
			 *
			 * XXX because we want to overwrite the input in-place, we cannot
			 * support a downcasing transformation that increases the string
			 * length.  This is not a problem given the current implementation
			 * of downcase_truncate_identifier, but we'll probably have to do
			 * something about this someday.
			 */
			len = endp - curname;
			downname = downcase_truncate_identifier(curname, len, false);
			Assert(strlen(downname) <= len);
			strncpy(curname, downname, len);	/* strncpy is required here */
			pfree(downname);
		}

		while (scanner_isspace(*nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (scanner_isspace(*nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/* Truncate name if it's overlength */
		truncate_identifier(curname, strlen(curname), false);

		/*
		 * Finished isolating current name --- add it to list
		 */
		*namelist = lappend(*namelist, curname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}


/*
 * SplitDirectoriesString --- parse a string containing file/directory names
 *
 * This works fine on file names too; the function name is historical.
 *
 * This is similar to SplitIdentifierString, except that the parsing
 * rules are meant to handle pathnames instead of identifiers: there is
 * no downcasing, embedded spaces are allowed, the max length is MAXPGPATH-1,
 * and we apply canonicalize_path() to each extracted string.  Because of the
 * last, the returned strings are separately palloc'd rather than being
 * pointers into rawstring --- but we still scribble on rawstring.
 *
 * Inputs:
 *	rawstring: the input string; must be modifiable!
 *	separator: the separator punctuation expected between directories
 *			   (typically ',' or ';').  Whitespace may also appear around
 *			   directories.
 * Outputs:
 *	namelist: filled with a palloc'd list of directory names.
 *			  Caller should list_free_deep() this even on error return.
 *
 * Returns true if okay, false if there is a syntax error in the string.
 *
 * Note that an empty string is considered okay here.
 */
bool
SplitDirectoriesString(char *rawstring, char separator,
					   List **namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;

	*namelist = NIL;

	while (scanner_isspace(*nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new directory. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '"')
		{
			/* Quoted name --- collapse quote-quote pairs */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					return false;	/* mismatched quotes */
				if (endp[1] != '"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or end of string */
			curname = endp = nextp;
			while (*nextp && *nextp != separator)
			{
				/* trailing whitespace should not be included in name */
				if (!scanner_isspace(*nextp))
					endp = nextp + 1;
				nextp++;
			}
			if (curname == endp)
				return false;	/* empty unquoted name not allowed */
		}

		while (scanner_isspace(*nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (scanner_isspace(*nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/* Truncate path if it's overlength */
		if (strlen(curname) >= MAXPGPATH)
			curname[MAXPGPATH - 1] = '\0';

		/*
		 * Finished isolating current name --- add it to list
		 */
		curname = pstrdup(curname);
		canonicalize_path(curname);
		*namelist = lappend(*namelist, curname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}


/*
 * SplitGUCList --- parse a string containing identifiers or file names
 *
 * This is used to split the value of a GUC_LIST_QUOTE GUC variable, without
 * presuming whether the elements will be taken as identifiers or file names.
 * We assume the input has already been through flatten_set_variable_args(),
 * so that we need never downcase (if appropriate, that was done already).
 * Nor do we ever truncate, since we don't know the correct max length.
 * We disallow embedded whitespace for simplicity (it shouldn't matter,
 * because any embedded whitespace should have led to double-quoting).
 * Otherwise the API is identical to SplitIdentifierString.
 *
 * XXX it's annoying to have so many copies of this string-splitting logic.
 * However, it's not clear that having one function with a bunch of option
 * flags would be much better.
 *
 * XXX there is a version of this function in src/bin/pg_dump/dumputils.c.
 * Be sure to update that if you have to change this.
 *
 * Inputs:
 *	rawstring: the input string; must be overwritable!	On return, it's
 *			   been modified to contain the separated identifiers.
 *	separator: the separator punctuation expected between identifiers
 *			   (typically '.' or ',').  Whitespace may also appear around
 *			   identifiers.
 * Outputs:
 *	namelist: filled with a palloc'd list of pointers to identifiers within
 *			  rawstring.  Caller should list_free() this even on error return.
 *
 * Returns true if okay, false if there is a syntax error in the string.
 */
bool
SplitGUCList(char *rawstring, char separator,
			 List **namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;

	*namelist = NIL;

	while (scanner_isspace(*nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '"')
		{
			/* Quoted name --- collapse quote-quote pairs */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					return false;	/* mismatched quotes */
				if (endp[1] != '"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !scanner_isspace(*nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */
		}

		while (scanner_isspace(*nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (scanner_isspace(*nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/*
		 * Finished isolating current name --- add it to list
		 */
		*namelist = lappend(*namelist, curname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}


/*****************************************************************************
 *	Comparison Functions used for bytea
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 *****************************************************************************/

Datum
byteaeq(PG_FUNCTION_ARGS)
{
	Datum		arg1 = PG_GETARG_DATUM(0);
	Datum		arg2 = PG_GETARG_DATUM(1);
	bool		result;
	Size		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = toast_raw_datum_size(arg1);
	len2 = toast_raw_datum_size(arg2);
	if (len1 != len2)
		result = false;
	else
	{
		bytea	   *barg1 = DatumGetByteaPP(arg1);
		bytea	   *barg2 = DatumGetByteaPP(arg2);

		result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2),
						 len1 - VARHDRSZ) == 0);

		PG_FREE_IF_COPY(barg1, 0);
		PG_FREE_IF_COPY(barg2, 1);
	}

	PG_RETURN_BOOL(result);
}

Datum
byteane(PG_FUNCTION_ARGS)
{
	Datum		arg1 = PG_GETARG_DATUM(0);
	Datum		arg2 = PG_GETARG_DATUM(1);
	bool		result;
	Size		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = toast_raw_datum_size(arg1);
	len2 = toast_raw_datum_size(arg2);
	if (len1 != len2)
		result = true;
	else
	{
		bytea	   *barg1 = DatumGetByteaPP(arg1);
		bytea	   *barg2 = DatumGetByteaPP(arg2);

		result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2),
						 len1 - VARHDRSZ) != 0);

		PG_FREE_IF_COPY(barg1, 0);
		PG_FREE_IF_COPY(barg2, 1);
	}

	PG_RETURN_BOOL(result);
}

Datum
bytealt(PG_FUNCTION_ARGS)
{
	bytea	   *arg1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *arg2 = PG_GETARG_BYTEA_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

Datum
byteale(PG_FUNCTION_ARGS)
{
	bytea	   *arg1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *arg2 = PG_GETARG_BYTEA_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

Datum
byteagt(PG_FUNCTION_ARGS)
{
	bytea	   *arg1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *arg2 = PG_GETARG_BYTEA_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

Datum
byteage(PG_FUNCTION_ARGS)
{
	bytea	   *arg1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *arg2 = PG_GETARG_BYTEA_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

Datum
byteacmp(PG_FUNCTION_ARGS)
{
	bytea	   *arg1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *arg2 = PG_GETARG_BYTEA_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
	if ((cmp == 0) && (len1 != len2))
		cmp = (len1 < len2) ? -1 : 1;

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_INT32(cmp);
}

Datum
bytea_larger(PG_FUNCTION_ARGS)
{
	bytea	   *arg1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *arg2 = PG_GETARG_BYTEA_PP(1);
	bytea	   *result;
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
	result = ((cmp > 0) || ((cmp == 0) && (len1 > len2)) ? arg1 : arg2);

	PG_RETURN_BYTEA_P(result);
}

Datum
bytea_smaller(PG_FUNCTION_ARGS)
{
	bytea	   *arg1 = PG_GETARG_BYTEA_PP(0);
	bytea	   *arg2 = PG_GETARG_BYTEA_PP(1);
	bytea	   *result;
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
	result = ((cmp < 0) || ((cmp == 0) && (len1 < len2)) ? arg1 : arg2);

	PG_RETURN_BYTEA_P(result);
}

Datum
bytea_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

	/* Use generic string SortSupport, forcing "C" collation */
	varstr_sortsupport(ssup, BYTEAOID, C_COLLATION_OID);

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_VOID();
}

/*
 * Return the size of a datum, possibly compressed
 * AALEKSEEV TODO keep only in varlana.c?
 *
 * Works on any data type
 */
Datum
pg_column_size(PG_FUNCTION_ARGS)
{
	Datum		value = PG_GETARG_DATUM(0);
	int32		result;
	int			typlen;

	/* On first call, get the input type's typlen, and save at *fn_extra */
	if (fcinfo->flinfo->fn_extra == NULL)
	{
		/* Lookup the datatype of the supplied argument */
		Oid			argtypeid = get_fn_expr_argtype(fcinfo->flinfo, 0);

		typlen = get_typlen(argtypeid);
		if (typlen == 0)		/* should not happen */
			elog(ERROR, "cache lookup failed for type %u", argtypeid);

		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(int));
		*((int *) fcinfo->flinfo->fn_extra) = typlen;
	}
	else
		typlen = *((int *) fcinfo->flinfo->fn_extra);

	if (typlen == -1)
	{
		/* varlena type, possibly toasted */
		result = toast_datum_size(value);
	}
	else if (typlen == -2)
	{
		/* cstring */
		result = strlen(DatumGetCString(value)) + 1;
	}
	else
	{
		/* ordinary fixed-width type */
		result = typlen;
	}

	PG_RETURN_INT32(result);
}

/*
 * Return the compression method stored in the compressed attribute.  Return
 * NULL for non varlena type or uncompressed data.
 */
Datum
pg_column_compression(PG_FUNCTION_ARGS)
{
	int			typlen;
	char	   *result;
	ToastCompressionId cmid;

	/* On first call, get the input type's typlen, and save at *fn_extra */
	if (fcinfo->flinfo->fn_extra == NULL)
	{
		/* Lookup the datatype of the supplied argument */
		Oid			argtypeid = get_fn_expr_argtype(fcinfo->flinfo, 0);

		typlen = get_typlen(argtypeid);
		if (typlen == 0)		/* should not happen */
			elog(ERROR, "cache lookup failed for type %u", argtypeid);

		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(int));
		*((int *) fcinfo->flinfo->fn_extra) = typlen;
	}
	else
		typlen = *((int *) fcinfo->flinfo->fn_extra);

	if (typlen != -1)
		PG_RETURN_NULL();

	/* get the compression method id stored in the compressed varlena */
	cmid = toast_get_compression_id((struct varlena *)
									DatumGetPointer(PG_GETARG_DATUM(0)));
	if (cmid == TOAST_INVALID_COMPRESSION_ID)
		PG_RETURN_NULL();

	/* convert compression method id to compression method name */
	switch (cmid)
	{
		case TOAST_PGLZ_COMPRESSION_ID:
			result = "pglz";
			break;
		case TOAST_LZ4_COMPRESSION_ID:
			result = "lz4";
			break;
		default:
			elog(ERROR, "invalid compression method id %d", cmid);
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Return the chunk_id of the on-disk TOASTed value.  Return NULL if the value
 * is un-TOASTed or not on-disk.
 */
Datum
pg_column_toast_chunk_id(PG_FUNCTION_ARGS)
{
	int			typlen;
	struct varlena *attr;
	struct varatt_external toast_pointer;

	/* On first call, get the input type's typlen, and save at *fn_extra */
	if (fcinfo->flinfo->fn_extra == NULL)
	{
		/* Lookup the datatype of the supplied argument */
		Oid			argtypeid = get_fn_expr_argtype(fcinfo->flinfo, 0);

		typlen = get_typlen(argtypeid);
		if (typlen == 0)		/* should not happen */
			elog(ERROR, "cache lookup failed for type %u", argtypeid);

		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(int));
		*((int *) fcinfo->flinfo->fn_extra) = typlen;
	}
	else
		typlen = *((int *) fcinfo->flinfo->fn_extra);

	if (typlen != -1)
		PG_RETURN_NULL();

	attr = (struct varlena *) DatumGetPointer(PG_GETARG_DATUM(0));

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		PG_RETURN_NULL();

	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

	PG_RETURN_OID(toast_pointer.va_valueid);
}

/*
 * string_agg_combine
 *		Aggregate combine function for string_agg(text) and string_agg(bytea)
 * AALEKSEEV TODO hmmmmmm split steing_agg(text) and string_agg(bytea) 
 */
Datum
string_agg_combine(PG_FUNCTION_ARGS)
{
	StringInfo	state1;
	StringInfo	state2;
	MemoryContext agg_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (StringInfo) PG_GETARG_POINTER(1);

	if (state2 == NULL)
	{
		/*
		 * NULL state2 is easy, just return state1, which we know is already
		 * in the agg_context
		 */
		if (state1 == NULL)
			PG_RETURN_NULL();
		PG_RETURN_POINTER(state1);
	}

	if (state1 == NULL)
	{
		/* We must copy state2's data into the agg_context */
		MemoryContext old_context;

		old_context = MemoryContextSwitchTo(agg_context);
		state1 = makeStringAggState(fcinfo);
		appendBinaryStringInfo(state1, state2->data, state2->len);
		state1->cursor = state2->cursor;
		MemoryContextSwitchTo(old_context);
	}
	else if (state2->len > 0)
	{
		/* Combine ... state1->cursor does not change in this case */
		appendBinaryStringInfo(state1, state2->data, state2->len);
	}

	PG_RETURN_POINTER(state1);
}

/*
 * string_agg_serialize
 *		Aggregate serialize function for string_agg(text) and string_agg(bytea)
 *
 * This is strict, so we need not handle NULL input
 * AALEKSEEV TODO hmmmmmm split steing_agg(text) and string_agg(bytea) 
 */
Datum
string_agg_serialize(PG_FUNCTION_ARGS)
{
	StringInfo	state;
	StringInfoData buf;
	bytea	   *result;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = (StringInfo) PG_GETARG_POINTER(0);

	pq_begintypsend(&buf);

	/* cursor */
	pq_sendint(&buf, state->cursor, 4);

	/* data */
	pq_sendbytes(&buf, state->data, state->len);

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

/*
 * string_agg_deserialize
 *		Aggregate deserial function for string_agg(text) and string_agg(bytea)
 *
 * This is strict, so we need not handle NULL input
 * AALEKSEEV TODO hmmmmmm split steing_agg(text) and string_agg(bytea) 
 */
Datum
string_agg_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	StringInfo	result;
	StringInfoData buf;
	char	   *data;
	int			datalen;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	sstate = PG_GETARG_BYTEA_PP(0);

	/*
	 * Initialize a StringInfo so that we can "receive" it using the standard
	 * recv-function infrastructure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));

	result = makeStringAggState(fcinfo);

	/* cursor */
	result->cursor = pq_getmsgint(&buf, 4);

	/* data */
	datalen = VARSIZE_ANY_EXHDR(sstate) - 4;
	data = (char *) pq_getmsgbytes(&buf, datalen);
	appendBinaryStringInfo(result, data, datalen);

	pq_getmsgend(&buf);

	PG_RETURN_POINTER(result);
}

Datum
string_agg_finalfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	if (state != NULL)
	{
		/* As per comment in transfn, strip data before the cursor position */
		PG_RETURN_TEXT_P(cstring_to_text_with_len(&state->data[state->cursor],
												  state->len - state->cursor));
	}
	else
		PG_RETURN_NULL();
}

/*
 * Prepare cache with fmgr info for the output functions of the datatypes of
 * the arguments of a concat-like function, beginning with argument "argidx".
 * (Arguments before that will have corresponding slots in the resulting
 * FmgrInfo array, but we don't fill those slots.)
 *
 * AALEKSEEV TODO delete?
 */
static FmgrInfo *
build_concat_foutcache(FunctionCallInfo fcinfo, int argidx)
{
	FmgrInfo   *foutcache;
	int			i;

	/* We keep the info in fn_mcxt so it survives across calls */
	foutcache = (FmgrInfo *) MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
												PG_NARGS() * sizeof(FmgrInfo));

	for (i = argidx; i < PG_NARGS(); i++)
	{
		Oid			valtype;
		Oid			typOutput;
		bool		typIsVarlena;

		valtype = get_fn_expr_argtype(fcinfo->flinfo, i);
		if (!OidIsValid(valtype))
			elog(ERROR, "could not determine data type of concat() input");

		getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
		fmgr_info_cxt(typOutput, &foutcache[i], fcinfo->flinfo->fn_mcxt);
	}

	fcinfo->flinfo->fn_extra = foutcache;

	return foutcache;
}

/*
 * Implementation of both concat() and concat_ws().
 *
 * sepstr is the separator string to place between values.
 * argidx identifies the first argument to concatenate (counting from zero);
 * note that this must be constant across any one series of calls.
 *
 * Returns NULL if result should be NULL, else text value.
 *
 * AALEKSEEV TODO delete?
 */
static text *
concat_internal(const char *sepstr, int argidx,
				FunctionCallInfo fcinfo)
{
	text	   *result;
	StringInfoData str;
	FmgrInfo   *foutcache;
	bool		first_arg = true;
	int			i;

	/*
	 * concat(VARIADIC some-array) is essentially equivalent to
	 * array_to_text(), ie concat the array elements with the given separator.
	 * So we just pass the case off to that code.
	 */
	if (get_fn_expr_variadic(fcinfo->flinfo))
	{
		ArrayType  *arr;

		/* Should have just the one argument */
		Assert(argidx == PG_NARGS() - 1);

		/* concat(VARIADIC NULL) is defined as NULL */
		if (PG_ARGISNULL(argidx))
			return NULL;

		/*
		 * Non-null argument had better be an array.  We assume that any call
		 * context that could let get_fn_expr_variadic return true will have
		 * checked that a VARIADIC-labeled parameter actually is an array.  So
		 * it should be okay to just Assert that it's an array rather than
		 * doing a full-fledged error check.
		 */
		Assert(OidIsValid(get_base_element_type(get_fn_expr_argtype(fcinfo->flinfo, argidx))));

		/* OK, safe to fetch the array value */
		arr = PG_GETARG_ARRAYTYPE_P(argidx);

		/*
		 * And serialize the array.  We tell array_to_text to ignore null
		 * elements, which matches the behavior of the loop below.
		 */
		return array_to_text_internal(fcinfo, arr, sepstr, NULL);
	}

	/* Normal case without explicit VARIADIC marker */
	initStringInfo(&str);

	/* Get output function info, building it if first time through */
	foutcache = (FmgrInfo *) fcinfo->flinfo->fn_extra;
	if (foutcache == NULL)
		foutcache = build_concat_foutcache(fcinfo, argidx);

	for (i = argidx; i < PG_NARGS(); i++)
	{
		if (!PG_ARGISNULL(i))
		{
			Datum		value = PG_GETARG_DATUM(i);

			/* add separator if appropriate */
			if (first_arg)
				first_arg = false;
			else
				appendStringInfoString(&str, sepstr);

			/* call the appropriate type output function, append the result */
			appendStringInfoString(&str,
								   OutputFunctionCall(&foutcache[i], value));
		}
	}

	result = cstring_to_text_with_len(str.data, str.len);
	pfree(str.data);

	return result;
}

/*
 * Helper function for Levenshtein distance functions. Faster than memcmp(),
 * for this use case.
 *
 * AALEKSEEV TODO delete?
 */
static inline bool
rest_of_char_same(const char *s1, const char *s2, int len)
{
	while (len > 0)
	{
		len--;
		if (s1[len] != s2[len])
			return false;
	}
	return true;
}

/* Expand each Levenshtein distance variant */
#include "levenshtein.c"
#define LEVENSHTEIN_LESS_EQUAL
#include "levenshtein.c"


/*
 * The following *ClosestMatch() functions can be used to determine whether a
 * user-provided string resembles any known valid values, which is useful for
 * providing hints in log messages, among other things.  Use these functions
 * like so:
 *
 *		initClosestMatch(&state, source_string, max_distance);
 *
 *		for (int i = 0; i < num_valid_strings; i++)
 *			updateClosestMatch(&state, valid_strings[i]);
 *
 *		closestMatch = getClosestMatch(&state);
 */

/*
 * Initialize the given state with the source string and maximum Levenshtein
 * distance to consider.
 */
void
initClosestMatch(ClosestMatchState *state, const char *source, int max_d)
{
	Assert(state);
	Assert(max_d >= 0);

	state->source = source;
	state->min_d = -1;
	state->max_d = max_d;
	state->match = NULL;
}

/*
 * If the candidate string is a closer match than the current one saved (or
 * there is no match saved), save it as the closest match.
 *
 * If the source or candidate string is NULL, empty, or too long, this function
 * takes no action.  Likewise, if the Levenshtein distance exceeds the maximum
 * allowed or more than half the characters are different, no action is taken.
 */
void
updateClosestMatch(ClosestMatchState *state, const char *candidate)
{
	int			dist;

	Assert(state);

	if (state->source == NULL || state->source[0] == '\0' ||
		candidate == NULL || candidate[0] == '\0')
		return;

	/*
	 * To avoid ERROR-ing, we check the lengths here instead of setting
	 * 'trusted' to false in the call to varstr_levenshtein_less_equal().
	 */
	if (strlen(state->source) > MAX_LEVENSHTEIN_STRLEN ||
		strlen(candidate) > MAX_LEVENSHTEIN_STRLEN)
		return;

	dist = varstr_levenshtein_less_equal(state->source, strlen(state->source),
										 candidate, strlen(candidate), 1, 1, 1,
										 state->max_d, true);
	if (dist <= state->max_d &&
		dist <= strlen(state->source) / 2 &&
		(state->min_d == -1 || dist < state->min_d))
	{
		state->min_d = dist;
		state->match = candidate;
	}
}

/*
 * Return the closest match.  If no suitable candidates were provided via
 * updateClosestMatch(), return NULL.
 */
const char *
getClosestMatch(ClosestMatchState *state)
{
	Assert(state);

	return state->match;
}

