// Written in the D programming language.

module duckdb.database;

import duckdb.c.duckdb;
import duckdb.common;
import duckdb.connection;

class Config
{
  private:
    duckdb_config _conf;

  public:
    this()
    {
        if (duckdb_create_config(&_conf) == DuckDBError)
            onDuckDBException("Failed to create duckdb config");
    }

    ~this()
    {
        duckdb_destroy_config(&_conf);
    }

    @property duckdb_config config() @safe nothrow { return _conf; }

    void opIndexAssign(string value, string key)
    {
        import std.conv : text;
        import std.string : toStringz;

        if (duckdb_set_config(_conf, key.toStringz, value.toStringz) == DuckDBError)
            onDuckDBException(text("Failed to set config : key = ", key, ", value = ", value));
    }
}

unittest
{
    import std.exception : assertThrown;

    auto conf = new Config();
    conf["access_mode"] = "READ_WRITE";
    conf["threads"] = "8";

    assertThrown!DuckDBException(conf["default_order"] = "ZZZ");
}

class Database
{
  private:
    string _path;
    duckdb_database _db;

  public:
    this(string path, Config conf = null)
    {
        import std.string : toStringz;

        _path = path;
        if (conf is null) {
            if (duckdb_open(path ? path.toStringz : null, &_db) == DuckDBError) {
                if (path == null)
                    onDuckDBException("Failed to open in-process database");
                else
                    onDuckDBException("Failed to open database : path = " ~ path);
            }
        } else {
            if (duckdb_open_ext(path ? path.toStringz : null, &_db, conf.config, null) == DuckDBError) {
                if (path == null)
                    onDuckDBException("Failed to open in-process database with config");
                else
                    onDuckDBException("Failed to open database with config : path = " ~ path);
            }
        }
    }

    ~this()
    {
        close();
    }

    void close() nothrow
    {
        if (_db)
            duckdb_close(&_db);
        _db = null;
    }

    Connection connect()
    {
        duckdb_connection conn;
        if (duckdb_connect(_db, &conn) == DuckDBError)
            onDuckDBException("Failed to create connection");

        return new Connection(conn);
    }

    void executeTasks(ulong maxTasks)
    {
        duckdb_execute_tasks(_db, maxTasks);
    }

    duckdb_task_state createTaskState()
    {
        return duckdb_create_task_state(_db);
    }

    void addReplacementScan(ReplacementScanCallback callback)
    {
        import std.functional : toDelegate;

        auto del = toDelegate(callback);
        duckdb_add_replacement_scan(_db, &replacementCallback, del.ptr, &delete_callback);
    }
}

// Replacement Scan

import std.variant : Variant;

// TODO : Delegate support
alias ReplacementScanCallback = Variant function(string tableName, out string funcName);

extern(C): @trusted:

void delete_callback(void* data) nothrow {}

void replacementCallback(duckdb_replacement_scan_info info, const(char)* tableName, void* data)
{
    import std.string : fromStringz, toStringz;

    static void setNumParameter(ref duckdb_replacement_scan_info info, int num)
    {
        auto val = duckdb_create_int64(num);
        duckdb_replacement_scan_add_parameter(info, val);
        duckdb_destroy_value(&val);
    }

    string funcName;
    ReplacementScanCallback cb = cast(ReplacementScanCallback)data;

    auto res = cb(cast(string)tableName.fromStringz, funcName);

    duckdb_replacement_scan_set_function_name(info, funcName.toStringz);

    // Support int and int[] for "range" table function.
    // TODO : Support other table functions if needed.
    int* n = res.peek!(int);
    if (n) {
        setNumParameter(info, *n);
        return;
    }
    int[]* nums = res.peek!(int[]);
    if (nums) {
        foreach (num; *nums)
            setNumParameter(info, num);
        return;
    }
}
