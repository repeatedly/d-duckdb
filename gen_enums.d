// Generate duckdb D enums from C definitions

import duckdb.c.duckdb;
import std;

void main()
{
    string[] typeMaps, resultMaps, stmtMaps;

    writeln("enum DataType : byte {");
    foreach(e; EnumMembers!duckdb_type) {
        string dsym = e.stringof.split("_")[3..$].map!(a => a.capitalize).join("");
        string csym = text("duckdb_type.", e.to!string);
        writeln(text("    ", dsym, " = ", csym, ","));
        typeMaps ~= text(csym, " : DataType.", dsym, ",");
    }
    writeln("}");
    writeln("immutable DataType[duckdb_type] DataTypeMap;\n");
    writeln("enum ResultType : byte {");
    foreach(e; EnumMembers!duckdb_result_type) {
        string dsym = e.stringof.split("_")[5..$].map!(a => a.capitalize).join("");
        string csym = text("duckdb_result_type.", e.to!string);
        writeln(text("    ", dsym, " = ", csym, ","));
        resultMaps ~= text(csym, " : ResultType.", dsym, ",");
    }
    writeln("}");
    writeln("immutable ResultType[duckdb_result_type] ResultTypeMap;\n");
    writeln("enum StatementType : byte {");
    foreach(e; EnumMembers!duckdb_statement_type) {
        string dsym = e.stringof.split("_")[5..$].map!(a => a.capitalize).join("");
        string csym = text("duckdb_statement_type.", e.to!string);
        writeln(text("    ", dsym, " = ", csym, ","));
        stmtMaps ~= text(csym, " : StatementType.", dsym, ",");
    }
    writeln("}");
    writeln("immutable StatementType[duckdb_statement_type] StatementTypeMap;\n");

    writeln("shared static this()\n{");
    writeln("    DataTypeMap = [");
    foreach (s; typeMaps)
        writeln("        " ~ s);
    writeln("    ];");
    writeln("    ResultTypeMap = [");
    foreach (s; resultMaps)
        writeln("        " ~ s);
    writeln("    ];");
    writeln("    StatementTypeMap = [");
    foreach (s; stmtMaps)
        writeln("        " ~ s);
    writeln("    ];");
    writeln("}");
}
