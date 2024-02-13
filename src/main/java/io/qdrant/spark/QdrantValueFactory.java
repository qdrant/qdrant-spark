package io.qdrant.spark;

import static io.qdrant.client.ValueFactory.list;
import static io.qdrant.client.ValueFactory.nullValue;

import io.qdrant.client.ValueFactory;
import io.qdrant.client.grpc.JsonWithInt.Struct;
import io.qdrant.client.grpc.JsonWithInt.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class QdrantValueFactory {
  private QdrantValueFactory() {}

  public static Value value(InternalRow record, StructField field, int fieldIndex) {
    DataType dataType = field.dataType();

    switch (dataType.typeName()) {
      case "integer":
        return ValueFactory.value(record.getInt(fieldIndex));
      case "float":
        return ValueFactory.value(record.getFloat(fieldIndex));
      case "double":
        return ValueFactory.value(record.getDouble(fieldIndex));
      case "long":
        return ValueFactory.value(record.getLong(fieldIndex));
      case "boolean":
        return ValueFactory.value(record.getBoolean(fieldIndex));
      case "string":
        return ValueFactory.value(record.getString(fieldIndex));
      case "array":
        ArrayType arrayType = (ArrayType) dataType;
        ArrayData arrayData = record.getArray(fieldIndex);
        return convertArrayToValue(arrayData, arrayType.elementType());
      case "struct":
        StructType structType = (StructType) dataType;
        InternalRow structData = record.getStruct(fieldIndex, structType.fields().length);
        return value(structData, structType);
      default:
        return nullValue();
    }
  }

  public static Value convertArrayToValue(ArrayData arrayData, DataType elementType) {

    List<Value> result = new ArrayList<>();
    switch (elementType.typeName()) {
      case "integer":
        {
          for (int element : arrayData.toIntArray()) {
            result.add(ValueFactory.value(element));
          }
          break;
        }
      case "float":
        {
          for (float element : arrayData.toFloatArray()) {
            result.add(ValueFactory.value(element));
          }
          break;
        }
      case "double":
        {
          for (double element : arrayData.toDoubleArray()) {
            result.add(ValueFactory.value(element));
          }
          break;
        }
      case "long":
        {
          for (long element : arrayData.toLongArray()) {
            result.add(ValueFactory.value(element));
          }
          break;
        }
      case "boolean":
        {
          for (boolean element : arrayData.toBooleanArray()) {
            result.add(ValueFactory.value(element));
          }
          break;
        }
      case "string":
        {
          int length = arrayData.numElements();
          for (int i = 0; i < length; i++) {
            result.add(ValueFactory.value(arrayData.getUTF8String(i).toString()));
          }
          break;
        }
      case "struct":
        {
          StructType structType = (StructType) elementType;
          int length = arrayData.numElements();
          for (int i = 0; i < length; i++) {
            InternalRow structData = arrayData.getStruct(i, structType.fields().length);
            result.add(value(structData, structType));
          }
          break;
        }
      default:
        return nullValue();
    }

    return list(result);
  }

  public static Value value(InternalRow structData, StructType structType) {
    Map<String, Value> result = new HashMap<>();
    for (int i = 0; i < structType.fields().length; i++) {
      StructField structField = structType.fields()[i];
      result.put(structField.name(), value(structData, structField, i));
    }
    Value value =
        Value.newBuilder().setStructValue(Struct.newBuilder().putAllFields(result)).build();
    return value;
  }
}
