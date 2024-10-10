// Written in the D programming language.

module duckdb.prepared_statement;

import duckdb.c.duckdb;
import duckdb.common;
import duckdb.result;

import std.bigint : BigInt;
import std.datetime : Date, SysTime;
import std.exception : basicExceptionCtors;

class PreparedStatementException : DuckDBException
{
    mixin basicExceptionCtors;
}

class PreparedStatement
{
  private:
    duckdb_prepared_statement _stmt;

  public @trusted:
    this(duckdb_prepared_statement stmt)
    {
        _stmt = stmt;
    }

    ~this()
    {
        destroy();
    }

    void destroy() nothrow
    {
        if (_stmt)
            duckdb_destroy_prepare(&_stmt);
        _stmt = null;
    }

    ulong nameToParameterIndex(string name)
    {
        import std.string : toStringz;

        ulong res;

        if (duckdb_bind_parameter_index(_stmt, &res, name.toStringz) == DuckDBError)
            onPreparedStatementException("Failed to retrive parameter index");

        return res;
    }

    @property string[] parameterNames() nothrow
    {
        import std.string : fromStringz;

        auto num = duckdb_nparams(_stmt);
        auto res = new string[](num);

        foreach (i; 0..num) {
            auto name = duckdb_parameter_name(_stmt, i + 1);
            res[i] = name.fromStringz.idup;
            duckdb_free(cast(void*)name);
        }

        return res;
    }

    @property duckdb_type[] parameterTypes() nothrow
    {
        import std.string : fromStringz;

        auto num = duckdb_nparams(_stmt);
        auto res = new duckdb_type[](num);

        foreach (i; 0..num)
            res[i] = duckdb_param_type(_stmt, i + 1);

        return res;
    }

    @property duckdb_statement_type statementType() nothrow
    {
        return duckdb_prepared_statement_type(_stmt);
    }

    void clearBindings()
    {
        if (duckdb_clear_bindings(_stmt) == DuckDBError)
            onPreparedStatementException("Failed to clear bindings");
    }

    Result execute()
    {
        import std.conv : text;
        import std.string : fromStringz;

        duckdb_result res;
        scope(failure) duckdb_destroy_result(&res);

        if (duckdb_execute_prepared(_stmt, &res) == DuckDBError)
            onPreparedStatementException(text("Failed to execute prepared statement : error = ", duckdb_result_error(&res).fromStringz));

        return new Result(res);
    }

    void bindValues(Args...)(Args args)
    {
        import std.stdio;

        if (args.length != duckdb_nparams(_stmt))
            onPreparedStatementException("The number of parameters and pass values are mismatched");

        foreach (ulong index, ref arg; args)
            bind(index + 1, arg);
    }

    void bind(T)(ulong index, auto ref T value)
    {
        import std.traits;

        static if (isBoolean!T) {
            bindBool(index, value);
        } else static if (isIntegral!T) {
            static if (is(T == byte))
                bindInt8(index, value);
            else static if (is(T == short))
                bindInt16(index, value);
            else static if (is(T == int))
                bindInt32(index, value);
            else static if (is(T == long))
                bindInt64(index, value);
            else static if (is(T == ubyte))
                bindUint8(index, value);
            else static if (is(T == ushort))
                bindUint16(index, value);
            else static if (is(T == uint))
                bindUint32(index, value);
            else static if (is(T == ulong))
                bindUint64(index, value);
            else
                onPreparedStatementException("Unsupported type: type = " ~ T.stringof);
        } else static if (isFloatingPoint!T) {
            static if (is(T == float))
                bindFloat(index, value);
            else static if (is(T == double))
                bindDouble(index, value);
            else
                onPreparedStatementException("Unsupported type: type = " ~ T.stringof);
        } else static if (is(T == BigInt)) {
            bindHugeint(index, value);
        } else static if (is(T == byte[])) {
            bindBlob(index, value);
        } else static if (isSomeString!T) {
            bindVarchar(index, value);
        } else static if (is(T == Date)) {
            bindDate(index, value);
        } else static if (is(T == SysTime)) {
            bindTimestamp(index, value);
        } else {
            onPreparedStatementException("Unsupported type: type = " ~ T.stringof);
        }
    }

    void bindNull(ulong index)
    {
        if (duckdb_bind_null(_stmt, index) == DuckDBError)
            onPreparedStatementException("Failed to bind NULL");
    }

    void bindBool(ulong index, bool b)
    {
        if (duckdb_bind_boolean(_stmt, index, b) == DuckDBError)
            onPreparedStatementException("Failed to bind bool");
    }

    void bindInt8(ulong index, byte b)
    {
        if (duckdb_bind_int8(_stmt, index, b) == DuckDBError)
            onPreparedStatementException("Failed to bind int8");
    }

    void bindInt16(ulong index, short s)
    {
        if (duckdb_bind_int16(_stmt, index, s) == DuckDBError)
            onPreparedStatementException("Failed to bind int16");
    }

    void bindInt32(ulong index, int i)
    {
        if (duckdb_bind_int32(_stmt, index, i) == DuckDBError)
            onPreparedStatementException("Failed to bind int32");
    }

    void bindInt64(ulong index, long l)
    {
        if (duckdb_bind_int64(_stmt, index, l) == DuckDBError)
            onPreparedStatementException("Failed to bind int64");
    }

    void bindUint8(ulong index, ubyte b)
    {
        if (duckdb_bind_uint8(_stmt, index, b) == DuckDBError)
            onPreparedStatementException("Failed to bind uint8");
    }

    void bindUint16(ulong index, ushort s)
    {
        if (duckdb_bind_uint16(_stmt, index, s) == DuckDBError)
            onPreparedStatementException("Failed to bind uint16");
    }

    void bindUint32(ulong index, uint i)
    {
        if (duckdb_bind_uint32(_stmt, index, i) == DuckDBError)
            onPreparedStatementException("Failed to bind uint32");
    }

    void bindUint64(ulong index, ulong l)
    {
        if (duckdb_bind_uint64(_stmt, index, l) == DuckDBError)
            onPreparedStatementException("Failed to bind uint64");
    }

    private static immutable BigInt Divisor = (BigInt(1) << 64);

    void bindHugeint(ulong index, in BigInt b)
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
        if (duckdb_bind_hugeint(_stmt, index, hint) == DuckDBError)
            onPreparedStatementException("Failed to bind hugeint");
    }

    void bindUhugeint(ulong index, in BigInt b)
    {
        import std.bigint : divMod;

        BigInt q, r;
        divMod(b, Divisor, q, r);

        duckdb_uhugeint uhint = {lower : cast(ulong)r, upper : cast(ulong)q};
        if (duckdb_bind_uhugeint(_stmt, index, uhint) == DuckDBError)
            onPreparedStatementException("Failed to bind uhugeint");
    }

    void bindFloat(ulong index, float f)
    {
        if (duckdb_bind_float(_stmt, index, f) == DuckDBError)
            onPreparedStatementException("Failed to bind float");
    }

    void bindDouble(ulong index, double d)
    {
        if (duckdb_bind_double(_stmt, index, d) == DuckDBError)
            onPreparedStatementException("Failed to bind double");
    }

    void bindVarchar(ulong index, string s)
    {
        if (s == null) {
            bindNull(index);
            return;
        }

        if (duckdb_bind_varchar_length(_stmt, index, s.ptr, s.length) == DuckDBError)
            onPreparedStatementException("Failed to bind varchar");
    }

    void bindBlob(ulong index, const(byte)[] b)
    {
        if (b == null) {
            bindNull(index);
            return;
        }

        if (duckdb_bind_blob(_stmt, index, b.ptr, b.length) == DuckDBError)
            onPreparedStatementException("Failed to bind blob");
    }

    void bindDate(ulong index, Date d)
    {
        duckdb_date_struct date = {year:d.year, month:d.month, day:d.day};
        if (duckdb_bind_date(_stmt, index, duckdb_to_date(date)) == DuckDBError)
            onPreparedStatementException("Failed to bind date");
    }

    void bindTimestamp(ulong index, SysTime t)
    {
        import std.datetime : unixTimeToStdTime;

        enum EpochOffset = unixTimeToStdTime(0);
        duckdb_timestamp ts = {(t.stdTime - EpochOffset) / 10};
        if (duckdb_bind_timestamp(_stmt, index, ts) == DuckDBError)
            onPreparedStatementException("Failed to bind Date");
    }

    /*
      TODO: Support following APIs
      duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement,  idx_t param_idx,  duckdb_time val);)
      duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement,  idx_t param_idx,  duckdb_interval val);)
     */
}

private:

noreturn onPreparedStatementException(string msg) @safe pure
{
    onDuckDBException!PreparedStatementException(msg);
}
