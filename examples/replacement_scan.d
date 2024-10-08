import duckdb;
import std;

void main()
{
    static Variant cb(string tableName, out string funcName) {
        funcName = "range";
        return Variant([2, 10, 3]);
    }

    auto db = new Database(null);  // null for in-memory database
    db.addReplacementScan(&cb);
    auto conn = db.connect();

    writeln(duckdbVersion);

    auto r = conn.query("SELECT * FROM unknown_table;");
    foreach (int n; r)
        writeln(n);
    r.destroy();

    conn.disconnect();
    db.close();
}
