import duckdb;
import std;

void main()
{
    auto db = new Database(null);
    auto conn = db.connect();

    writeln(duckdbVersion);
    {
        writeln("> integer and NULL");
        conn.queryWithoutResult("CREATE TABLE integers (i INTEGER, j INTEGER);");
        conn.queryWithoutResult("INSERT INTO integers VALUES (3, 4),  (5, 6),  (7, NULL);");
        auto r = conn.query("SELECT * FROM integers;");
        foreach (int a, Nullable!int b; r)
            writeln(a, ", ",  b);
    }
    {
        writeln("> hugeints");
        conn.queryWithoutResult("CREATE TABLE people (i HUGEINT, k UHUGEINT);");
        conn.queryWithoutResult("INSERT INTO people VALUES (-17014118346046923173168730371588410572, 1), (17014118346046923173168730371588410572, 34028236692093846346337460743176821145);");
        auto r = conn.query("SELECT * FROM people;");
        foreach (BigInt a, BigInt b; r)
            writeln(a, ", ",  b);
    }
    {
        writeln("> list and array");
        conn.queryWithoutResult("CREATE TABLE array_table (names VARCHAR[], ints INTEGER[3]);");
        conn.queryWithoutResult("INSERT INTO array_table VALUES (['abc'], [1, 2, 3]), (['aaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'], [4, 5, 6]);");
        auto r = conn.query("SELECT * FROM array_table;");
        foreach (string[] a, int[] b; r)
            writeln(a, ", ",  b);
    }
    {
        writeln("> enum");
        conn.queryWithoutResult("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
        conn.queryWithoutResult("CREATE TABLE person (name TEXT, current_mood mood);");
        conn.queryWithoutResult("INSERT INTO person VALUES ('Pedro', 'happy'), ('Pagliacci', 'sad'), ('Mr. Mackey', 'ok');");
        auto r = conn.query("SELECT * FROM person;");
        foreach (string a, string b; r)
            writeln(a, ", ",  b);
    }
    {
        writeln("> map");
        conn.queryWithoutResult("CREATE TABLE maps (mp MAP(INTEGER,  VARCHAR));");
        conn.queryWithoutResult("INSERT INTO maps VALUES (MAP {10 : 'hello'}), (MAP {0 : 'hooooooooooooooooooooooooooi'});");
        auto r = conn.query("SELECT * FROM maps;");
        foreach (string[int] m; r)
            writeln(m);
    }
    {
        writeln("> struct");
        static struct S { string v; int i; }
        conn.queryWithoutResult("CREATE TABLE st_table (s STRUCT(v VARCHAR, i INTEGER));");
        conn.queryWithoutResult("INSERT INTO st_table VALUES (row('hello', 1)), (row('abcdefghijklmnopqrstuvwxyz', 100));");
        auto r = conn.query("SELECT * FROM st_table;");
        foreach (S s; r)
            writeln(s);
    }
    {
        writeln("> date");
        conn.queryWithoutResult("CREATE TABLE d_table (d DATE);");
        conn.queryWithoutResult("INSERT INTO d_table VALUES ('2024-11-11'), ('infinity'), ('epoch');");
        auto r = conn.query("SELECT * FROM d_table;");
        foreach (Date d; r)
            writeln(d);
    }
    {
        writeln("> timestamp types");
        conn.queryWithoutResult("CREATE TABLE ts_table (t1 TIMESTAMP, t2 TIMESTAMP_S, t3 TIMESTAMP_MS);");
        conn.queryWithoutResult("INSERT INTO ts_table VALUES ('2024-09-20 11:30:00.123456789', '2024-09-20 11:30:00.123456789', '2024-09-20 11:30:00.123456789'), ('epoch', 'epoch', 'epoch');");
        auto r = conn.query("SELECT * FROM ts_table;");
        foreach (SysTime t1, SysTime t2, SysTime t3; r)
            writeln(t1, ", ", t2, ", ", t3);
    }
    {
        writeln("> UUID");
        conn.queryWithoutResult("CREATE TABLE ids (id INTEGER, uid UUID);");
        conn.queryWithoutResult("INSERT INTO ids VALUES (1, '4ac7a9e9-607c-4c8a-84f3-843f0191e3fd'), (2, 'fac7a901-307c-4ce1-34fa-043f0a91e3f0');");
        auto r = conn.query("SELECT * FROM ids;");
        foreach (int a, UUID u; r)
            writeln(a, ", ",  u);
    }

    conn.disconnect();
    db.close();
}
