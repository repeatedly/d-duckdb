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
    foreach (int a, Nullable!int b; r)  // Specify argument types implicitly
        writeln(a, ", ",  b);

    conn.disconnect();
    db.close();
}
```

# DuckDB and D Types

| DuckDB type | D type  | Description                                                           |
|-------------|---------|-----------------------------------------------------------------------|
| BOOLEAN     | bool    |                                                                       |
| TINYINT     | byte    |                                                                       |
| SMALLINT    | short   |                                                                       |
| INTEGER     | int     |                                                                       |
| BIGINT      | long    |                                                                       |
| HUGEINT     | BigInt  |                                                                       |
| UTINYINT    | ubyte   |                                                                       |
| USMALLINT   | ushort  |                                                                       |
| UINTEGER    | uint    |                                                                       |
| UBIGINT     | ulong   |                                                                       |
| UHUGEINT    | BigInt  |                                                                       |
| FLOAT       | float   |                                                                       |
| DOUBLE      | double  |                                                                       |
| DECIMAL     |         | TODO: Phobos doesn't have BigFloat/BigDecimal, so support is limited  |
| VARCHAR     | string  | `wstring` and `dstring` are also supported                            |
| BLOB        | byte[]  |                                                                       |
| BITSTRING   |         | TODO                                                                  |
| ENUM        | string  |                                                                       |
| DATE        | Date    |                                                                       |
| TIME        |         | TODO but use `TIMESTAMP` instead                                      |
| TIMESTAMP   | SysTime | `TIMESTAMP_NS` is not supported because `SysTime` is hnsecs precision |
| INTERVAL    |         | TODO                                                                  |
| ARRAY       | T[]     |                                                                       |
| LIST        | T[]     |                                                                       |
| MAP         | V[K]    |                                                                       |
| STRUCT      | struct  | Current implementation doesn't check field names                      |
| UNION       |         | TODO                                                                  |
| UUID        |         | TODO                                                                  |

## Special cases

- NULL

Use `std.typecons.Nullable` to accept NULL for basic types, e.g. `Nullable!int`.

- Infinity for DATA and TIMESTAMP

`Infinity`/`-Infinity` is mapping to `.init` value, e.g. `Date.init` and `SysTime.init`.

# TODO

- Support TODO DuckDB types
- Support more APIs
  - Prepared Statements
  - Appender
  - and more
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
