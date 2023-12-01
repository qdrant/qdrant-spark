package io.qdrant.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DataWriter implementation that writes data to Qdrant, a vector search engine. This class takes
 * QdrantOptions and StructType as input and writes data to QdrantRest. It implements the DataWriter
 * interface and overrides its methods write, commit, abort and close. It also has a private method
 * write that is used to upload a batch of points to Qdrant. The class uses a Point class to
 * represent a data point and an ArrayList to store the points.
 */
public class QdrantDataWriter implements DataWriter<InternalRow>, Serializable {
  private final QdrantOptions options;
  private final StructType schema;
  private final QdrantRest qdrantRest;
  private final Logger LOG = LoggerFactory.getLogger(QdrantDataWriter.class);

  private final ArrayList<Point> points = new ArrayList<>();

  public QdrantDataWriter(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
    this.qdrantRest = new QdrantRest(this.options.qdrantUrl, this.options.apiKey);
  }

  @Override
  public void write(InternalRow record) {
    Point point = new Point();
    HashMap<String, Object> payload = new HashMap<>();

    if (this.options.idField == null) {
      point.id = UUID.randomUUID().toString();
    }
    for (StructField field : this.schema.fields()) {
      int fieldIndex = this.schema.fieldIndex(field.name());
      if (this.options.idField != null && field.name().equals(this.options.idField)) {
        point.id = record.get(fieldIndex, field.dataType()).toString();
      } else if (field.name().equals(this.options.embeddingField)) {
        float[] vector = record.getArray(fieldIndex).toFloatArray();
        point.vector = vector;
      } else {
        payload.put(field.name(), convertToJavaType(record, field, fieldIndex));
      }
    }

    point.payload = payload;
    this.points.add(point);

    if (this.points.size() >= this.options.batchSize) {
      this.write(this.options.retries);
    }
  }

  @Override
  public WriterCommitMessage commit() {
    this.write(this.options.retries);
    return new WriterCommitMessage() {
      @Override
      public String toString() {
        return "point committed to Qdrant";
      }
    };
  }

  public void write(int retries) {
    LOG.info("Upload batch of " + this.points.size() + " points to Qdrant");
    if (this.points.isEmpty()) {
      return;
    }
    try {
      this.qdrantRest.uploadBatch(this.options.collectionName, this.points);
      this.points.clear();
    } catch (Exception e) {
      LOG.error("Error while uploading batch to Qdrant: {}", e.getMessage());
      if (retries > 0) {
        LOG.info("Retrying upload batch to Qdrant");
        write(retries - 1);
      } else {
        LOG.error(e.getMessage());
      }
    }
  }

  @Override
  public void abort() {}

  @Override
  public void close() {}

  private Object convertToJavaType(InternalRow record, StructField field, int fieldIndex) {
    DataType dataType = field.dataType();

    if (dataType == DataTypes.StringType) {
      return record.getString(fieldIndex);
    } else if (dataType == DataTypes.IntegerType) {
      return record.getInt(fieldIndex);
    } else if (dataType == DataTypes.LongType) {
      return record.getLong(fieldIndex);
    } else if (dataType == DataTypes.FloatType) {
      return record.getFloat(fieldIndex);
    } else if (dataType == DataTypes.DoubleType) {
      return record.getDouble(fieldIndex);
    } else if (dataType == DataTypes.BooleanType) {
      return record.getBoolean(fieldIndex);
    } else if (dataType == DataTypes.DateType || dataType == DataTypes.TimestampType) {
      return record.getString(fieldIndex);
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      ArrayData arrayData = record.getArray(fieldIndex);
      return convertArrayToJavaType(arrayData, arrayType.elementType());
    } else if (dataType instanceof StructType) {
      StructType structType = (StructType) dataType;
      InternalRow structData = record.getStruct(fieldIndex, structType.fields().length);
      return convertStructToJavaType(structData, structType);
    }

    // Fall back to the generic get method
    // TODO: Add explicit parsings for other data types like maps
    return record.get(fieldIndex, dataType);
  }

  private Object convertArrayToJavaType(ArrayData arrayData, DataType elementType) {
    if (elementType == DataTypes.IntegerType) {
      return arrayData.toIntArray();
    } else if (elementType == DataTypes.FloatType) {
      return arrayData.toFloatArray();
    } else if (elementType == DataTypes.ShortType) {
      return arrayData.toShortArray();
    } else if (elementType == DataTypes.ByteType) {
      return arrayData.toByteArray();
    } else if (elementType == DataTypes.DoubleType) {
      return arrayData.toDoubleArray();
    } else if (elementType == DataTypes.LongType) {
      return arrayData.toLongArray();
    } else if (elementType == DataTypes.BooleanType) {
      return arrayData.toBooleanArray();
    } else if (elementType == DataTypes.StringType) {
      int length = arrayData.numElements();
      String[] result = new String[length];
      for (int i = 0; i < length; i++) {
        result[i] = arrayData.getUTF8String(i).toString();
      }
      return result;
    } else if (elementType instanceof StructType) {
      StructType structType = (StructType) elementType;
      int length = arrayData.numElements();
      Object[] result = new Object[length];
      for (int i = 0; i < length; i++) {
        InternalRow structData = arrayData.getStruct(i, structType.fields().length);
        result[i] = convertStructToJavaType(structData, structType);
      }
      return result;

    } else {
      throw new UnsupportedOperationException("Unsupported array type");
    }
  }

  private Object convertStructToJavaType(InternalRow structData, StructType structType) {
    Map<String, Object> result = new HashMap<>();
    for (int i = 0; i < structType.fields().length; i++) {
      StructField structField = structType.fields()[i];
      result.put(structField.name(), convertToJavaType(structData, structField, i));
    }
    return result;
  }
}

class Point {
  public String id;
  public float[] vector;
  public HashMap<String, Object> payload;
}
