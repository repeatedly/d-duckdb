// Written in the D programming language.

module duckdb.connection;

import duckdb.c.duckdb;
import duckdb.common;
import duckdb.appender;
import duckdb.result;

class Connection
{
  private:
    duckdb_connection _conn;

  public:
    this(duckdb_connection conn)
    {
        _conn = conn;
    }

    ~this()
    {
        disconnect();
    }

    void disconnect() nothrow
    {
        if (_conn)
            duckdb_disconnect(&_conn);
        _conn = null;
    }

    Result query(in string q)
    {
        import std.conv : text;
        import std.string : toStringz, fromStringz;

        duckdb_result res;
        scope(failure) duckdb_destroy_result(&res);

        if (duckdb_query(_conn, q.toStringz, &res) == DuckDBError)
            onDuckDBException(text("Failed to query : query = ", q, ", error = ", duckdb_result_error(&res).fromStringz));

        return new Result(res);
    }

    void queryWithoutResult(in string q)
    {
        import std.conv : text;
        import std.string : toStringz, fromStringz;

        duckdb_result res;
        scope(exit) duckdb_destroy_result(&res);

        if (duckdb_query(_conn, q.toStringz, &res) == DuckDBError)
            onDuckDBException(text("Failed to query : query = ", q, ", error = ", duckdb_result_error(&res).fromStringz));
    }

    void interrupt() nothrow
    {
        duckdb_interrupt(_conn);
    }

    duckdb_query_progress_type queryProgress() nothrow
    {
        return duckdb_query_progress(_conn);
    }

    Appender appender(string table, string schema = null)
    {
        import std.conv : text;
        import std.string : toStringz, fromStringz;

        duckdb_appender app;
        scope(failure) duckdb_appender_destroy(&app);

        if (duckdb_appender_create(_conn, schema ? schema.toStringz : null, table.toStringz, &app) == DuckDBError)
            onDuckDBException(text("Failed to create appender: schema = ", schema, ", table = ", table, "error = ", duckdb_appender_error(app).fromStringz));

        return new Appender(app);
    }
}
