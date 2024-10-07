// Written in the D programming language.

module duckdb.appender;

import duckdb.c.duckdb;
import duckdb.common;

import std.bigint : BigInt;
import std.datetime : Date, SysTime;
import std.exception : basicExceptionCtors;

class DuckDBAppenderException : DuckDBException
{
    mixin basicExceptionCtors;
}

class Appender
{
  private:
    duckdb_appender _app;

  public @trusted:
    this(duckdb_appender app)
    {
        _app = app;
    }

    ~this()
    {
        destroy();
    }

    void destroy() nothrow
    {
        if (_app)
            duckdb_appender_destroy(&_app);
        _app = null;
    }

    size_t numColumns() nothrow
    {
        return duckdb_appender_column_count(_app);
    }

    void endRow() nothrow
    {
        duckdb_appender_end_row(_app);
    }

    void flush()
    {
        import std.conv : text;
        import std.string : fromStringz;

        scope(failure) destroy();
        if (duckdb_appender_flush(_app) == DuckDBError)
            onAppenderException(text("Failed to flush appender. This appender is destroyed: error = ", duckdb_appender_error(_app).fromStringz));
    }

    void append(T)(auto ref T value)
    {
        import std.traits;

        static if (isBoolean!T) {
            appendBool(value);
        } else static if (isIntegral!T) {
            static if (is(T == byte))
                appendInt8(value);
            else static if (is(T == short))
                appendInt16(value);
            else static if (is(T == int))
                appendInt32(value);
            else static if (is(T == long))
                appendInt64(value);
            else static if (is(T == ubyte))
                appendUint8(value);
            else static if (is(T == ushort))
                appendUint16(value);
            else static if (is(T == uint))
                appendUint32(value);
            else static if (is(T == ulong))
                appendUint64(value);
            else
                onAppenderException("Unsupported type: type = " ~ T.stringof);
        } else static if (isFloatingPoint!T) {
            static if (is(T == float))
                appendFloat(value);
            else static if (is(T == double))
                appendDouble(value);
            else
                onAppenderException("Unsupported type: type = " ~ T.stringof);
        } else static if (is(T == BigInt)) {
            appendHugeint(value);
        } else static if (is(T == byte[])) {
            appendBlob(value);
        } else static if (isSomeString!T) {
            appendVarchar(value);
        } else static if (is(T == Date)) {
            appendDate(value);
        } else static if (is(T == SysTime)) {
            appendTimestamp(value);
        } else {
            onAppenderException("Unsupported type: type = " ~ T.stringof);
        }
    }

    void appendRow(Args...)(Args args)
    {
        foreach (ref arg; args)
            append(arg);

        endRow();
    }

    void appendNull()
    {
        if (duckdb_append_null(_app) == DuckDBError)
            onAppenderException("Failed to append NULL");
    }

    void appendBool(bool b)
    {
        if (duckdb_append_bool(_app, b) == DuckDBError)
            onAppenderException("Failed to append bool");
    }

    void appendInt8(byte b)
    {
        if (duckdb_append_int8(_app, b) == DuckDBError)
            onAppenderException("Failed to append int8");
    }

    void appendInt16(short s)
    {
        if (duckdb_append_int16(_app, s) == DuckDBError)
            onAppenderException("Failed to append int16");
    }

    void appendInt32(int i)
    {
        if (duckdb_append_int32(_app, i) == DuckDBError)
            onAppenderException("Failed to append int32");
    }

    void appendInt64(long l)
    {
        if (duckdb_append_int64(_app, l) == DuckDBError)
            onAppenderException("Failed to append int64");
    }

    void appendUint8(ubyte b)
    {
        if (duckdb_append_uint8(_app, b) == DuckDBError)
            onAppenderException("Failed to append uint8");
    }

    void appendUint16(ushort s)
    {
        if (duckdb_append_uint16(_app, s) == DuckDBError)
            onAppenderException("Failed to append uint16");
    }

    void appendUint32(uint i)
    {
        if (duckdb_append_uint32(_app, i) == DuckDBError)
            onAppenderException("Failed to append uint32");
    }

    void appendUint64(ulong l)
    {
        if (duckdb_append_uint64(_app, l) == DuckDBError)
            onAppenderException("Failed to append uint64");
    }

    private static immutable BigInt Divisor = (BigInt(1) << 64);

    void appendHugeint(in BigInt b)
    {
        import std.bigint : divMod;
        import std.stdio;

        BigInt q, r;
        divMod(b, Divisor, q, r);
        if (r < 0) { // Phobos's divMod generates minus remainder but hugeint doesn't allow it.
            q -= 1;
            r += Divisor;
        }
        duckdb_hugeint hint = {lower : cast(ulong)r, upper : cast(long)q};
        if (duckdb_append_hugeint(_app, hint) == DuckDBError)
            onAppenderException("Failed to append hugeint");
    }

    void appendUhugeint(in BigInt b)
    {
        import std.bigint : divMod;

        BigInt q, r;
        divMod(b, Divisor, q, r);

        duckdb_uhugeint uhint = {lower : cast(ulong)r, upper : cast(ulong)q};
        if (duckdb_append_uhugeint(_app, uhint) == DuckDBError)
            onAppenderException("Failed to append uhugeint");
    }

    void appendFloat(float f)
    {
        if (duckdb_append_float(_app, f) == DuckDBError)
            onAppenderException("Failed to append float");
    }

    void appendDouble(double d)
    {
        if (duckdb_append_double(_app, d) == DuckDBError)
            onAppenderException("Failed to append double");
    }

    void appendVarchar(string s)
    {
        if (s == null) {
            appendNull();
            return;
        }

        if (duckdb_append_varchar_length(_app, s.ptr, s.length) == DuckDBError)
            onAppenderException("Failed to append varchar");
    }

    void appendBlob(const(byte)[] b)
    {
        if (b == null) {
            appendNull();
            return;
        }

        if (duckdb_append_blob(_app, b.ptr, b.length) == DuckDBError)
            onAppenderException("Failed to append blob");
    }

    void appendDate(Date d)
    {
        duckdb_date_struct date = {year:d.year, month:d.month, day:d.day};
        if (duckdb_append_date(_app, duckdb_to_date(date)) == DuckDBError)
            onAppenderException("Failed to append date");
    }

    void appendTimestamp(SysTime t)
    {
        import std.datetime : unixTimeToStdTime;

        enum EpochOffset = unixTimeToStdTime(0);
        duckdb_timestamp ts = {(t.stdTime - EpochOffset) / 10};
        if (duckdb_append_timestamp(_app, ts) == DuckDBError)
            onAppenderException("Failed to append Date");
    }

    /*
      TODO: Support following APIs
      duckdb_append_time(duckdb_appender appender,  duckdb_time value);
      duckdb_append_interval(duckdb_appender appender,  duckdb_interval value);
      duckdb_append_data_chunk(duckdb_appender appender,  duckdb_data_chunk chunk);
    */
}

private:

noreturn onAppenderException(string msg) @safe pure
{
    onDuckDBException!DuckDBAppenderException(msg);
}
