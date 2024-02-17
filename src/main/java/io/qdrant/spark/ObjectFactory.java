package io.qdrant.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;

class ObjectFactory {
    public static Object object(InternalRow record, StructField field, int fieldIndex) {
        DataType dataType = field.dataType();

        switch (dataType.typeName()) {
            case "integer":
                return record.getInt(fieldIndex);
            case "float":
                return record.getFloat(fieldIndex);
            case "double":
                return record.getDouble(fieldIndex);
            case "long":
                return record.getLong(fieldIndex);
            case "boolean":
                return record.getBoolean(fieldIndex);
            case "string":
                return record.getString(fieldIndex);
            case "array":
                ArrayType arrayType = (ArrayType) dataType;
                ArrayData arrayData = record.getArray(fieldIndex);
                return object(arrayData, arrayType.elementType());
            case "struct":
                StructType structType = (StructType) dataType;
                InternalRow structData = record.getStruct(fieldIndex, structType.fields().length);
                return object(structData, structType);
            default:
                return null;
        }
    }

    public static Object object(ArrayData arrayData, DataType elementType) {

        switch (elementType.typeName()) {
            case "string": {
                int length = arrayData.numElements();
                String[] result = new String[length];
                for (int i = 0; i < length; i++) {
                    result[i] = arrayData.getUTF8String(i).toString();
                }
                return result;
            }

            case "struct": {
                StructType structType = (StructType) elementType;
                int length = arrayData.numElements();
                Object[] result = new Object[length];
                for (int i = 0; i < length; i++) {
                    InternalRow structData = arrayData.getStruct(i, structType.fields().length);
                    result[i] = object(structData, structType);
                }
                return result;
            }
            default:
                return arrayData.toObjectArray(elementType);
        }
    }

    public static Object object(InternalRow structData, StructType structType) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < structType.fields().length; i++) {
            StructField structField = structType.fields()[i];
            result.put(structField.name(), object(structData, structField, i));
        }
        return result;
    }
}