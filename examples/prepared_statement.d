import duckdb;
import std;

void main()
{
    static struct Data
    {
        short i;
        ulong ul;
        BigInt bi;
        double d;
        string str;
        byte[] blob;
        Date date;
        SysTime ts;
    }

    auto db = new Database(null);
    auto conn = db.connect();
    Data[] tests = [
        Data(-12345, uint.max, BigInt("-17014118346046923173168730371588410572"), 10.5, "hello", [0, 1, 2], Date(2024, 1, 1), Clock.currTime),
        Data(100, 1, BigInt("10000000000"), 123456.789, "foooooooooooooooooooooo!", null, Date(1999, 9, 9), SysTime(DateTime(2010, 1, 2, 3, 4, 5),  UTC())),
        Data(0, 1234567890, BigInt("0"), 0.0, null, cast(byte[])"abcdefghijklmnopqrstuvwxyz", Date.init, SysTime(DateTime(1980, 2, 22, 0, 30, 59),  UTC())),
    ];

    writeln(duckdbVersion);

    conn.queryWithoutResult("CREATE TABLE apps (i SMALLINT, ul UBIGINT, hi HUGEINT, d DOUBLE, str VARCHAR, b BLOB, date Date, ts TIMESTAMP);");
    auto stmt = conn.prepare("INSERT INTO apps VALUES ($i, $ul, $hi, $d, $str, $b, $date, $ts)");
    foreach (ref t; tests) {
        stmt.bindValues(t.i, t.ul, t.bi, t.d, t.str, t.blob, t.date, t.ts);
        stmt.execute();
    }

    size_t index;
    foreach (short i, ulong ul, BigInt bi, double d, string str, byte[] blob, Date date, SysTime ts; conn.query("SELECT * FROM apps;")) {
        auto t = tests[index++];
        writeln(">-----");
        assert(i == t.i);       writeln("short : ", i);
        assert(ul == t.ul);     writeln("ulong : ", ul);
        assert(bi == t.bi);     writeln("BigInt : ", bi);
        assert(d == t.d);       writeln("double : ", d);
        assert(str == t.str);   writeln("string : ", str);
        assert(blob == t.blob); writeln("byte[] : ", blob);
        assert(date == t.date); writeln("Date : ", date);
        assert(ts == t.ts);     writeln("SysTime : ", ts);
    }

    auto r = conn.prepare("SELECT * FROM apps WHERE i = ?;");
    r.bind(1, 100);
    foreach (short i, ulong ul, BigInt bi, double d, string str, byte[] blob, Date date, SysTime ts; r.execute()) {
        writeln("> select with prepare");
        writeln(i, " ", ul, " ", bi, " ", d, " ", str, " ",  blob, " ", date, " ", ts);
    }
    r.destroy();

    conn.disconnect();
    db.close();
}
