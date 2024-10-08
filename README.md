# d-duckdb

DuckDB client for D. This client is based on DuckDB's C API.

# Usage

## Iterate query result via foreach

```D
import duckdb;
import std;

void main()
{
    auto db = new Database(null);  // null for in-memory database
    auto conn = db.connect();

    writeln(duckdbVersion);

    conn.queryWithoutResult("CREATE TABLE integers (i INTEGER, j INTEGER);");
    conn.queryWithoutResult("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);");
    auto r = conn.query("SELECT * FROM integers;");
    foreach (int a, Nullable!int b; r)  // Specify argument types explicitly
        writeln(a, ", ",  b);

    conn.disconnect();
    db.close();
}
```

## Prepared Statement

```D
import duckdb;
import std;

void main()
{
    auto db = new Database(null);  // null for in-memory database
    auto conn = db.connect();

    writeln(duckdbVersion);

    conn.queryWithoutResult("CREATE TABLE integers (i SMALLINT, j BIGINT);");
    auto stmt1 = conn.prepare("INSERT INTO integers VALUES ($i,  $j)");

    foreach (i; 0..10) {
        short n1 = cast(short)i;
        long  n2 = long.max - i;
        stmt1.bindInt16(1, n1);
        stmt1.bindInt64(2, n2);
        stmt1.execute();
    }
    stmt1.destroy();

    auto stmt2 = conn.prepare("SELECT * FROM integers WHERE i BETWEEN ? AND ?;");
    stmt2.bindValues(3, 8);  // bindValues is shorter than calling each bindXXX
    foreach (short i, long j; stmt2.execute())
        writeln(i, " : ", j);
    stmt2.destroy();

    conn.disconnect();
    db.close();
}
```

## Appender: Faster bulk insertion

```D
import duckdb;
import std;

void main()
{
    auto db = new Database(null);  // null for in-memory database
    auto conn = db.connect();

    writeln(duckdbVersion);
    conn.queryWithoutResult("CREATE TABLE apps (i SMALLINT, str VARCHAR, ts TIMESTAMP);");
    auto appender = conn.appender("apps");
    foreach (i; 0..3)
        appender.appendRow(i, text("Name", i), Clock.currTime);
    appender.destroy();

    auto r = conn.query("SELECT * FROM apps;");
    foreach (short i, string str, SysTime ts; r)
        writeln("id: ", i, ", name: ", str, ", timestampe: ", ts);
    r.destroy();

    conn.disconnect();
    db.close();
}
```

# DuckDB and D Types

| DuckDB type  | D type                 | Description                                                           |
|--------------|------------------------|-----------------------------------------------------------------------|
| BOOLEAN      | bool                   |                                                                       |
| TINYINT      | byte                   |                                                                       |
| SMALLINT     | short                  |                                                                       |
| INTEGER      | int                    |                                                                       |
| BIGINT       | long                   |                                                                       |
| HUGEINT      | std.bigint.BigInt      |                                                                       |
| UTINYINT     | ubyte                  |                                                                       |
| USMALLINT    | ushort                 |                                                                       |
| UINTEGER     | uint                   |                                                                       |
| UBIGINT      | ulong                  |                                                                       |
| UHUGEINT     | std.bigint.BigInt      |                                                                       |
| FLOAT        | float                  |                                                                       |
| DOUBLE       | double                 |                                                                       |
| DECIMAL      |                        | TODO: Phobos doesn't have BigFloat/BigDecimal, so support is limited  |
| VARCHAR      | string/wstring/dstring |                                                                       |
| BLOB         | byte[]                 |                                                                       |
| BITSTRING    | string                 |                                                                       |
| ENUM         | string                 |                                                                       |
| DATE         | std.datetime.Date      |                                                                       |
| TIME         |                        | TODO but use `TIMESTAMP` instead                                      |
| TIMESTAMP    | std.datetime.SysTime   |                                                                       |
| TIMESTAMPTZ  | std.datetime.SysTime   |                                                                       |
| TIMESTAMP_S  | std.datetime.SysTime   |                                                                       |
| TIMESTAMP_MS | std.datetime.SysTime   |                                                                       |
| TIMESTAMP_NS |                        | `TIMESTAMP_NS` is not supported because `SysTime` is hnsecs precision |
| INTERVAL     |                        | TODO                                                                  |
| ARRAY        | T[]                    |                                                                       |
| LIST         | T[]                    |                                                                       |
| MAP          | V[K]                   |                                                                       |
| STRUCT       | struct                 | Current implementation doesn't check field names                      |
| UNION        |                        | TODO                                                                  |
| UUID         | std.uuid.UUID          |                                                                       |

## Special cases

- NULL

Use `std.typecons.Nullable` to accept NULL for basic types, e.g. `Nullable!int`.
Without `Nullable`, client returns `.init` value.

- Infinity for DATA and TIMESTAMP

`Infinity`/`-Infinity` is mapping to `.init` value, e.g. `Date.init` and `SysTime.init`.

# TODO

- Support TODO DuckDB types
- Support more APIs
- Improve API design
- Add unittests

# Links

* [DuckDB – An in-process SQL OLAP database management system](https://duckdb.org/)

  DuckDB official site

* [Overview – DuckDB](https://duckdb.org/docs/api/c/overview)

  DuckDB C API document

# Copyright

    Copyright (c) 2024- Masahiro Nakagawa

# License

Distributed under the [Boost Software License, Version 1.0](http://www.boost.org/users/license.html).
