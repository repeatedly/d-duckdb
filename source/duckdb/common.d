// Written in the D programming language.

module duckdb.common;

import duckdb.c.duckdb;

enum DataType : byte {
    Invalid = duckdb_type.DUCKDB_TYPE_INVALID,
    Boolean = duckdb_type.DUCKDB_TYPE_BOOLEAN,
    Tinyint = duckdb_type.DUCKDB_TYPE_TINYINT,
    Smallint = duckdb_type.DUCKDB_TYPE_SMALLINT,
    Integer = duckdb_type.DUCKDB_TYPE_INTEGER,
    Bigint = duckdb_type.DUCKDB_TYPE_BIGINT,
    Utinyint = duckdb_type.DUCKDB_TYPE_UTINYINT,
    Usmallint = duckdb_type.DUCKDB_TYPE_USMALLINT,
    Uinteger = duckdb_type.DUCKDB_TYPE_UINTEGER,
    Ubigint = duckdb_type.DUCKDB_TYPE_UBIGINT,
    Float = duckdb_type.DUCKDB_TYPE_FLOAT,
    Double = duckdb_type.DUCKDB_TYPE_DOUBLE,
    Timestamp = duckdb_type.DUCKDB_TYPE_TIMESTAMP,
    Date = duckdb_type.DUCKDB_TYPE_DATE,
    Time = duckdb_type.DUCKDB_TYPE_TIME,
    Interval = duckdb_type.DUCKDB_TYPE_INTERVAL,
    Hugeint = duckdb_type.DUCKDB_TYPE_HUGEINT,
    Uhugeint = duckdb_type.DUCKDB_TYPE_UHUGEINT,
    Varchar = duckdb_type.DUCKDB_TYPE_VARCHAR,
    Blob = duckdb_type.DUCKDB_TYPE_BLOB,
    Decimal = duckdb_type.DUCKDB_TYPE_DECIMAL,
    TimestampS = duckdb_type.DUCKDB_TYPE_TIMESTAMP_S,
    TimestampMs = duckdb_type.DUCKDB_TYPE_TIMESTAMP_MS,
    TimestampNs = duckdb_type.DUCKDB_TYPE_TIMESTAMP_NS,
    Enum = duckdb_type.DUCKDB_TYPE_ENUM,
    List = duckdb_type.DUCKDB_TYPE_LIST,
    Struct = duckdb_type.DUCKDB_TYPE_STRUCT,
    Map = duckdb_type.DUCKDB_TYPE_MAP,
    Array = duckdb_type.DUCKDB_TYPE_ARRAY,
    Uuid = duckdb_type.DUCKDB_TYPE_UUID,
    Union = duckdb_type.DUCKDB_TYPE_UNION,
    Bit = duckdb_type.DUCKDB_TYPE_BIT,
    TimeTz = duckdb_type.DUCKDB_TYPE_TIME_TZ,
    TimestampTz = duckdb_type.DUCKDB_TYPE_TIMESTAMP_TZ,
    Any = duckdb_type.DUCKDB_TYPE_ANY,
    Varint = duckdb_type.DUCKDB_TYPE_VARINT,
    Sqlnull = duckdb_type.DUCKDB_TYPE_SQLNULL,
}
immutable DataType[duckdb_type] DataTypeMap;

enum ResultType : byte {
    Invalid = duckdb_result_type.DUCKDB_RESULT_TYPE_INVALID,
    ChangedRows = duckdb_result_type.DUCKDB_RESULT_TYPE_CHANGED_ROWS,
    Nothing = duckdb_result_type.DUCKDB_RESULT_TYPE_NOTHING,
    QueryResult = duckdb_result_type.DUCKDB_RESULT_TYPE_QUERY_RESULT,
}
immutable ResultType[duckdb_result_type] ResultTypeMap;

enum StatementType : byte {
    Invalid = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_INVALID,
    Select = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_SELECT,
    Insert = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_INSERT,
    Update = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_UPDATE,
    Explain = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_EXPLAIN,
    Delete = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_DELETE,
    Prepare = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_PREPARE,
    Create = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_CREATE,
    Execute = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_EXECUTE,
    Alter = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_ALTER,
    Transaction = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_TRANSACTION,
    Copy = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_COPY,
    Analyze = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_ANALYZE,
    VariableSet = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_VARIABLE_SET,
    CreateFunc = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_CREATE_FUNC,
    Drop = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_DROP,
    Export = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_EXPORT,
    Pragma = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_PRAGMA,
    Vacuum = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_VACUUM,
    Call = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_CALL,
    Set = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_SET,
    Load = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_LOAD,
    Relation = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_RELATION,
    Extension = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_EXTENSION,
    LogicalPlan = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN,
    Attach = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_ATTACH,
    Detach = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_DETACH,
    Multi = duckdb_statement_type.DUCKDB_STATEMENT_TYPE_MULTI,
}
immutable StatementType[duckdb_statement_type] StatementTypeMap;

shared static this()
{
    DataTypeMap = [
        duckdb_type.DUCKDB_TYPE_INVALID : DataType.Invalid,
        duckdb_type.DUCKDB_TYPE_BOOLEAN : DataType.Boolean,
        duckdb_type.DUCKDB_TYPE_TINYINT : DataType.Tinyint,
        duckdb_type.DUCKDB_TYPE_SMALLINT : DataType.Smallint,
        duckdb_type.DUCKDB_TYPE_INTEGER : DataType.Integer,
        duckdb_type.DUCKDB_TYPE_BIGINT : DataType.Bigint,
        duckdb_type.DUCKDB_TYPE_UTINYINT : DataType.Utinyint,
        duckdb_type.DUCKDB_TYPE_USMALLINT : DataType.Usmallint,
        duckdb_type.DUCKDB_TYPE_UINTEGER : DataType.Uinteger,
        duckdb_type.DUCKDB_TYPE_UBIGINT : DataType.Ubigint,
        duckdb_type.DUCKDB_TYPE_FLOAT : DataType.Float,
        duckdb_type.DUCKDB_TYPE_DOUBLE : DataType.Double,
        duckdb_type.DUCKDB_TYPE_TIMESTAMP : DataType.Timestamp,
        duckdb_type.DUCKDB_TYPE_DATE : DataType.Date,
        duckdb_type.DUCKDB_TYPE_TIME : DataType.Time,
        duckdb_type.DUCKDB_TYPE_INTERVAL : DataType.Interval,
        duckdb_type.DUCKDB_TYPE_HUGEINT : DataType.Hugeint,
        duckdb_type.DUCKDB_TYPE_UHUGEINT : DataType.Uhugeint,
        duckdb_type.DUCKDB_TYPE_VARCHAR : DataType.Varchar,
        duckdb_type.DUCKDB_TYPE_BLOB : DataType.Blob,
        duckdb_type.DUCKDB_TYPE_DECIMAL : DataType.Decimal,
        duckdb_type.DUCKDB_TYPE_TIMESTAMP_S : DataType.TimestampS,
        duckdb_type.DUCKDB_TYPE_TIMESTAMP_MS : DataType.TimestampMs,
        duckdb_type.DUCKDB_TYPE_TIMESTAMP_NS : DataType.TimestampNs,
        duckdb_type.DUCKDB_TYPE_ENUM : DataType.Enum,
        duckdb_type.DUCKDB_TYPE_LIST : DataType.List,
        duckdb_type.DUCKDB_TYPE_STRUCT : DataType.Struct,
        duckdb_type.DUCKDB_TYPE_MAP : DataType.Map,
        duckdb_type.DUCKDB_TYPE_ARRAY : DataType.Array,
        duckdb_type.DUCKDB_TYPE_UUID : DataType.Uuid,
        duckdb_type.DUCKDB_TYPE_UNION : DataType.Union,
        duckdb_type.DUCKDB_TYPE_BIT : DataType.Bit,
        duckdb_type.DUCKDB_TYPE_TIME_TZ : DataType.TimeTz,
        duckdb_type.DUCKDB_TYPE_TIMESTAMP_TZ : DataType.TimestampTz,
        duckdb_type.DUCKDB_TYPE_ANY : DataType.Any,
        duckdb_type.DUCKDB_TYPE_VARINT : DataType.Varint,
        duckdb_type.DUCKDB_TYPE_SQLNULL : DataType.Sqlnull,
    ];
    ResultTypeMap = [
        duckdb_result_type.DUCKDB_RESULT_TYPE_INVALID : ResultType.Invalid,
        duckdb_result_type.DUCKDB_RESULT_TYPE_CHANGED_ROWS : ResultType.ChangedRows,
        duckdb_result_type.DUCKDB_RESULT_TYPE_NOTHING : ResultType.Nothing,
        duckdb_result_type.DUCKDB_RESULT_TYPE_QUERY_RESULT : ResultType.QueryResult,
    ];
    StatementTypeMap = [
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_INVALID : StatementType.Invalid,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_SELECT : StatementType.Select,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_INSERT : StatementType.Insert,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_UPDATE : StatementType.Update,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_EXPLAIN : StatementType.Explain,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_DELETE : StatementType.Delete,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_PREPARE : StatementType.Prepare,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_CREATE : StatementType.Create,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_EXECUTE : StatementType.Execute,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_ALTER : StatementType.Alter,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_TRANSACTION : StatementType.Transaction,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_COPY : StatementType.Copy,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_ANALYZE : StatementType.Analyze,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_VARIABLE_SET : StatementType.VariableSet,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_CREATE_FUNC : StatementType.CreateFunc,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_DROP : StatementType.Drop,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_EXPORT : StatementType.Export,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_PRAGMA : StatementType.Pragma,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_VACUUM : StatementType.Vacuum,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_CALL : StatementType.Call,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_SET : StatementType.Set,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_LOAD : StatementType.Load,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_RELATION : StatementType.Relation,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_EXTENSION : StatementType.Extension,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN : StatementType.LogicalPlan,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_ATTACH : StatementType.Attach,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_DETACH : StatementType.Detach,
        duckdb_statement_type.DUCKDB_STATEMENT_TYPE_MULTI : StatementType.Multi,
    ];
}

import std.exception : basicExceptionCtors;

class DuckDBException : Exception
{
    mixin basicExceptionCtors;
}

@trusted:

noreturn onDuckDBException(EX = DuckDBException)(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) @safe pure
{
    throw new EX(msg, file, line, next);
}

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
