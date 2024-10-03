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
