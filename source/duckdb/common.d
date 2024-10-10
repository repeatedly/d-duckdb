// Written in the D programming language.

module duckdb.common;

import duckdb.c.duckdb;

import std.exception : basicExceptionCtors;

class DuckDBException : Exception
{
    mixin basicExceptionCtors;
}

class DuckDBTypeException : DuckDBException
{
    mixin basicExceptionCtors;
}

@trusted:

noreturn onDuckDBException(EX = DuckDBException)(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) @safe pure
{
    throw new EX(msg, file, line, next);
}

alias onDuckDBTypeException = onDuckDBException!(DuckDBTypeException);

@property string duckdbVersion() nothrow
{
    import std.string : fromStringz;
    return cast(string)(duckdb_library_version().fromStringz);
}

import std.bigint : BigInt, divMod;

private static immutable BigInt Divisor = (BigInt(1) << 64);

duckdb_hugeint bigIntToHugeint(ref const BigInt b)
{
    BigInt q, r;

    divMod(b, Divisor, q, r);
    if (r < 0) { // Phobos's divMod generates minus remainder but hugeint doesn't allow it.
        q -= 1;
        r += Divisor;
    }

    return duckdb_hugeint(lower: cast(ulong)r, upper: cast(long)q);
}

duckdb_uhugeint bigIntToUhugeint(ref const BigInt b)
{
    BigInt q, r;

    divMod(b, Divisor, q, r);

    return duckdb_uhugeint(lower: cast(ulong)r, upper: cast(ulong)q);
}

import std.datetime : SysTime, unixTimeToStdTime;

duckdb_timestamp sysTimeToTimestamp(SysTime t)
{
    enum EpochOffset = unixTimeToStdTime(0);

    return duckdb_timestamp((t.stdTime - EpochOffset) / 10);
}
