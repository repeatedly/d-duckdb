// Written in the D programming language.

module duckdb.result;

import duckdb.c.duckdb;
import duckdb.common;

// struct is better?
class Result
{
  private:
    duckdb_result _res;

  public:
    this(duckdb_result res)
    {
        _res = res;
    }

    ~this()
    {
        destroy();
    }

    void destroy()
    {
        duckdb_destroy_result(&_res);
    }

    duckdb_result_type resultType() nothrow @nogc
    {
        return duckdb_result_return_type(_res);
    }

    duckdb_statement_type statementType() nothrow @nogc
    {
        return duckdb_result_statement_type(_res);
    }

    string[] columnNames() nothrow @trusted
    {
        import std.string : fromStringz;

        idx_t count = duckdb_column_count(&_res);
        string[] names = new string[](count);
        foreach (i; 0..count)
            names[i] = cast(string)duckdb_column_name(&_res, i).fromStringz.dup;

        return names;
    }

    static struct VectorInfo
    {
        duckdb_vector vec;
        duckdb_logical_type lt;
        ulong* validity;
        void* data;
    }

    int opApply(Args...)(scope int delegate(ref Args) dg)
    {
        import std.traits;
        import std.typecons : Nullable;

        int r;
        idx_t n = duckdb_column_count(&_res);
        if (n != Args.length)
            onDuckDBException("The number of columns and passed values are mismatched");

        while (true) {
            duckdb_data_chunk chunk = duckdb_fetch_chunk(_res);
            if (!chunk)
                break;
            scope(exit) duckdb_destroy_data_chunk(&chunk);

            VectorInfo[Args.length] cols;
            Args values;
            
            foreach (i, ref col; cols) {
                // TODO : Type check here
                col.vec = duckdb_data_chunk_get_vector(chunk, i);
                col.lt  = duckdb_vector_get_column_type(col.vec);
                col.data = duckdb_vector_get_data(col.vec);
                col.validity = duckdb_vector_get_validity(col.vec);
            }
            scope(exit) {
                foreach (ref col; cols)
                    duckdb_destroy_logical_type(&col.lt);
            }

            idx_t rowCount = duckdb_data_chunk_get_size(chunk);
            for (idx_t i = 0; i < rowCount; i++) {
                foreach (columnIndex, T; Args) {
                    auto col = cols[columnIndex];
                    if (!duckdb_validity_row_is_valid(col.validity, i)) {
                        values[columnIndex] = T.init;
                    } else {
                        static if (isInstanceOf!(Nullable, T))
                            values[columnIndex] = T(getVectorValue!(TemplateArgsOf!T)(col.vec, col.data, col.lt, i));
                        else
                            values[columnIndex] = getVectorValue!T(col.vec, col.data, col.lt, i);
                    }
                }
                r = dg(values);
                if (r)
                    return r;
            }
        }

        return r;
    }

    // TODO : Add InputRange support
}

// TODO : Split type check and extract value for the performance
auto getVectorValue(T)(duckdb_vector vector, void* data, duckdb_logical_type lt, idx_t rowIndex)
{
    import std.traits;
    import std.bigint;
    import std.datetime;
    import std.uuid;

    duckdb_type type = duckdb_get_type_id(lt);

    static if (isBoolean!T) {
        switch (type) {
        case DUCKDB_TYPE_BOOLEAN:
            return (cast(bool*)data)[rowIndex];
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (isIntegral!T) {
        static if (isSigned!T) {
            switch (type) {
            case DUCKDB_TYPE_TINYINT:
                return cast(T)(cast(byte*)data)[rowIndex];
            case DUCKDB_TYPE_SMALLINT:
                return cast(T)(cast(short*)data)[rowIndex];
            case DUCKDB_TYPE_INTEGER:
                return cast(T)(cast(int*)data)[rowIndex];
            case DUCKDB_TYPE_BIGINT:
                return cast(T)(cast(long*)data)[rowIndex];
            default:
                onVectorTypeMismatch(T.stringof, type);
            }
        } else {
            switch (type) {
            case DUCKDB_TYPE_UTINYINT:
                return cast(T)(cast(ubyte*)data)[rowIndex];
            case DUCKDB_TYPE_USMALLINT:
                return cast(T)(cast(ushort*)data)[rowIndex];
            case DUCKDB_TYPE_UINTEGER:
                return cast(T)(cast(uint*)data)[rowIndex];
            case DUCKDB_TYPE_UBIGINT:
                return cast(T)(cast(ulong*)data)[rowIndex];
            default:
                onVectorTypeMismatch(T.stringof, type);
            }
        }
    } else static if (isFloatingPoint!T) {
        switch (type) {
        case DUCKDB_TYPE_FLOAT:
            return cast(T)(cast(float*)data)[rowIndex];
        case DUCKDB_TYPE_DOUBLE:
            return cast(T)(cast(double*)data)[rowIndex];
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (is(T == BigInt)) {
        switch (type) {
        case DUCKDB_TYPE_HUGEINT:
            duckdb_hugeint hint = (cast(duckdb_hugeint*)data)[rowIndex];
            BigInt bi = hint.upper;
            bi <<= 64;
            bi += hint.lower;
            return bi;
        case DUCKDB_TYPE_UHUGEINT:
            duckdb_uhugeint uhint = (cast(duckdb_uhugeint*)data)[rowIndex];
            BigInt bi = uhint.upper;
            bi <<= 64;
            bi += uhint.lower;
            return bi;
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (is(T == byte[])) {
        switch (type) {
        case DUCKDB_TYPE_BLOB:
            return cast(byte[])getVectorCString(data, rowIndex);
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (isSomeString!T) {
        import std.conv : to;

        switch (type) {
        case DUCKDB_TYPE_VARCHAR:
            static if (is(immutable T == immutable C[], C) && (is(C == char)))
                return cast(T)getVectorCString(data, rowIndex);
            else
                return to!T(getVectorCString(data, rowIndex));
        case DUCKDB_TYPE_ENUM:
            import std.string : fromStringz;

            // duckdb_enum_internal_type supports only 3 uint types
            // https://github.com/duckdb/duckdb/blob/d58cc56f8554057f9646ea343d845e1b1ba6466e/src/main/capi/logical_types-c.cpp#L191
            uint index;
            duckdb_type internalType = duckdb_enum_internal_type(lt);
            switch (internalType) {
            case DUCKDB_TYPE_UTINYINT:
                index = (cast(ubyte*)data)[rowIndex];
                break;
            case DUCKDB_TYPE_USMALLINT:
                index = (cast(short*)data)[rowIndex];
                break;
            case DUCKDB_TYPE_UINTEGER:
                index = (cast(uint*)data)[rowIndex];
                break;
            default:
                onVectorTypeMismatch(to!string(internalType));
            }
            auto ptr = duckdb_enum_dictionary_value(lt, index);
            if (ptr) {
                scope (exit) duckdb_free(ptr);
                return cast(T)(ptr.fromStringz).dup;
            }
            onDuckDBTypeException("Can't get enum value by unexpected data");
        case DUCKDB_TYPE_BIT:
            auto str = (cast(duckdb_string_t*)data)[rowIndex];
            auto len = duckdb_string_t_length(str);
            auto src = duckdb_string_t_data(&str);
            auto padding = cast(ubyte)src[0];  // First byte is padding info. See bit.cpp
            auto bitLen = (len - 1) * 8 - padding;

            char[] res = new char[](bitLen);
            size_t resIndex;
            for (size_t i = padding; i < 8; i++)  // Second byte is padding affected area
                res[resIndex++] = src[1] & (1 << (7 - i)) ? '1' :  '0';
            for (size_t i = 2; i < len; i++)  // Remaining bytes
                for (size_t j = 0; j < 8; j++)
                    res[resIndex++] = src[i] & (1 << (7 - j)) ? '1' :  '0';

            return cast(string)res;
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (isArray!T) {
        switch (type) {
        case DUCKDB_TYPE_ARRAY:
            idx_t length = duckdb_array_type_array_size(lt);
            idx_t base = rowIndex * length;
            duckdb_vector childVec = duckdb_array_vector_get_child(vector);
            duckdb_logical_type childType = duckdb_array_type_child_type(lt);
            scope(exit) duckdb_destroy_logical_type(&childType);

            return createArrayFromVector!T(childVec, childType, length, base, base + length);
        case DUCKDB_TYPE_LIST:
            duckdb_list_entry list = (cast(duckdb_list_entry*)data)[rowIndex];
            idx_t base = list.offset;
            duckdb_vector childVec = duckdb_list_vector_get_child(vector);
            duckdb_logical_type childType = duckdb_list_type_child_type(lt);
            scope(exit) duckdb_destroy_logical_type(&childType);

            return createArrayFromVector!T(childVec, childType, list.length, base, base + list.length);
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (isAssociativeArray!T) {
        switch (type) {
        case DUCKDB_TYPE_MAP:
            duckdb_list_entry list = (cast(duckdb_list_entry*)data)[rowIndex];
            duckdb_vector childVec = duckdb_list_vector_get_child(vector);
            duckdb_vector keyVec = duckdb_struct_vector_get_child(childVec, 0);
            duckdb_vector valVec = duckdb_struct_vector_get_child(childVec, 1);
            void* keyData = duckdb_vector_get_data(keyVec);
            void* valData = duckdb_vector_get_data(valVec);
            duckdb_logical_type keyType = duckdb_map_type_key_type(lt);
            duckdb_logical_type valType = duckdb_map_type_value_type(lt);
            scope(exit) {
                duckdb_destroy_logical_type(&keyType);
                duckdb_destroy_logical_type(&valType);
            }

            T map;
            for (idx_t i = list.offset, end = list.offset + list.length; i < end; i++)
                map[getVectorValue!(KeyType!T)(keyVec, keyData, keyType, i)] = getVectorValue!(ValueType!T)(valVec, valData, valType, i);
            return map;
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (is(T == Date)) {
        switch (type) {
        case DUCKDB_TYPE_DATE:
            duckdb_date date = (cast(duckdb_date*)data)[rowIndex];
            if (duckdb_is_finite_date(date)) {
                duckdb_date_struct ds = duckdb_from_date(date);
                return Date(ds.year, ds.month, ds.day);
            } else {
                return Date.init;
            }
            break;
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (is(T == SysTime)) {
        static SysTime createSysTimeFromVector(void* data, idx_t index, SysTime delegate(long timestamp) callback)
        {
            duckdb_timestamp timestamp = (cast(duckdb_timestamp*)data)[index];
            if (duckdb_is_finite_timestamp(timestamp))
                return callback(timestamp.micros);
            else
                return SysTime.init;
        }
        // D's SysTime starts from "January 1st, 1" and DuckDB's Timestamp starts from Epoch(January 1st, 1970).
        enum EpochOffset = unixTimeToStdTime(0);

        switch (type) {
        case DUCKDB_TYPE_TIMESTAMP, DUCKDB_TYPE_TIMESTAMP_TZ:
            // "* 10" is for converting micro-seconds to hect-nano seconds(SysTime)
            return createSysTimeFromVector(data, rowIndex, ts => SysTime(ts * 10 + EpochOffset, UTC()));
        case DUCKDB_TYPE_TIMESTAMP_MS:
            // "* 10000" is for converting milli-seconds to hect-nano seconds(SysTime)
            return createSysTimeFromVector(data, rowIndex, ts => SysTime(ts * 10000 + EpochOffset, UTC()));
        case DUCKDB_TYPE_TIMESTAMP_S:
            return createSysTimeFromVector(data, rowIndex, ts => SysTime.fromUnixTime(ts, UTC()));
        case DUCKDB_TYPE_TIMESTAMP_NS:
            onDuckDBTypeException("TIMESTAMP_NS is not supported because D's SysTime doesn't support nano-second precision");
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (is(T == UUID)) {
        switch (type) {
        case DUCKDB_TYPE_UUID:
            @trusted nothrow static void numToBytes(ref ubyte[16] bytes, size_t base, ulong num)
            {
                ubyte* ptr = cast(ubyte*)&num;
                foreach (i; 0..8)
                    bytes[base + i] = *(ptr + i);
            }

            enum FlipForUpper = ulong(1) << 63;  // Need flip back. See uuid.cpp in duckdb code.
            duckdb_hugeint hint = (cast(duckdb_hugeint*)data)[rowIndex];
            ubyte[16] bytes;

            version (LittleEndian)
            {
                import core.bitop : bswap;
                numToBytes(bytes, 0, bswap(hint.upper ^ FlipForUpper));
                numToBytes(bytes, 8, bswap(hint.lower));
            }
            else
            {
                numToBytes(bytes, 0, hint.upper ^ FlipForUpper);
                numToBytes(bytes, 8, hint.lower);
            }

            return UUID(bytes);
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else static if (is(T == struct)) {
        switch (type) {
        case DUCKDB_TYPE_STRUCT:
            auto numFields = duckdb_struct_type_child_count(lt);
            if (numFields != T.tupleof.length)
                onDuckDBTypeException("D struct fields and duckdb struct fields are mismatched");

            Result.VectorInfo[T.tupleof.length] fields;
            foreach (i, ref field; fields) {
                field.vec = duckdb_struct_vector_get_child(vector, i);
                field.lt  = duckdb_struct_type_child_type(lt, i);
                field.data = duckdb_vector_get_data(field.vec);
            }
            scope(exit) {
                foreach (ref field; fields)
                    duckdb_destroy_logical_type(&field.lt);
            }

            T st;
            foreach (i, ref member; st.tupleof) {
                auto field = fields[i];
                member = getVectorValue!(typeof(member))(field.vec, field.data, field.lt, rowIndex);
            }
            return st;
        default:
            onVectorTypeMismatch(T.stringof, type);
        }
    } else {
        onVectorTypeMismatch(T.stringof);
    }
}

@trusted:

noreturn onVectorTypeMismatch(string dType, duckdb_type colType = DUCKDB_TYPE_INVALID)
{
    import std.conv : text;

    if (colType == DUCKDB_TYPE_INVALID)
        onDuckDBTypeException("Unsupported type: type = " ~ dType);
    else
        onDuckDBTypeException(text("Unmatched types: D type = ", dType, ", column type = ", colType));
}

const(char)[] getVectorCString(void* data, idx_t rowIndex) nothrow
{
    duckdb_string_t str = (cast(duckdb_string_t*)data)[rowIndex];
    return duckdb_string_t_data(&str)[0..duckdb_string_t_length(str)].dup;
}

T createArrayFromVector(T)(duckdb_vector vector, duckdb_logical_type type, size_t length, idx_t base, idx_t end)
{
    import std.traits : ForeachType;

    void* data = duckdb_vector_get_data(vector);
    T arr = new T(length);

    for (idx_t i = base; i < end; i++)
        arr[i - base] = getVectorValue!(ForeachType!T)(vector, data, type, i);

    return arr;
}
