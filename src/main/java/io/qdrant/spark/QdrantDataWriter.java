package io.qdrant.spark;

import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.list;
import static io.qdrant.client.ValueFactory.nullValue;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;

import io.qdrant.client.grpc.JsonWithInt.Struct;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.PointStruct;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
  private final QdrantGrpc qdrantRest;
  private final Logger LOG = LoggerFactory.getLogger(QdrantDataWriter.class);

  private final ArrayList<PointStruct> points = new ArrayList<>();

  public QdrantDataWriter(QdrantOptions options, StructType schema) throws Exception {
    this.options = options;
    this.schema = schema;
    this.qdrantRest = new QdrantGrpc(new URL(this.options.qdrantUrl), this.options.apiKey);
  }

  @Override
  public void write(InternalRow record) {
    PointStruct.Builder pointBuilder = PointStruct.newBuilder();
    Map<String, Value> payload = new HashMap<>();

    if (this.options.idField == null) {
      pointBuilder.setId(id(UUID.randomUUID()));
    }
    for (StructField field : this.schema.fields()) {
      int fieldIndex = this.schema.fieldIndex(field.name());
      if (this.options.idField != null && field.name().equals(this.options.idField)) {

        DataType dataType = field.dataType();
        switch (dataType.typeName()) {
          case "StringType":
            pointBuilder.setId(id(UUID.fromString(record.getString(fieldIndex))));
            break;

          case "IntegerType":
            pointBuilder.setId(id(record.getInt(fieldIndex)));
            break;
          default:
            break;
        }

      } else if (field.name().equals(this.options.embeddingField)) {
        float[] embeddings = record.getArray(fieldIndex).toFloatArray();
        pointBuilder.setVectors(vectors(embeddings));
      } else {
        payload.put(field.name(), convertToJavaType(record, field, fieldIndex));
      }
    }

    pointBuilder.putAllPayload(payload);

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
      this.qdrantRest.upsert(this.options.collectionName, this.points);
      this.points.clear();
    } catch (Exception e) {
      LOG.error("Exception while uploading batch to Qdrant: " + e.getMessage());
      if (retries > 0) {
        LOG.info("Retrying upload batch to Qdrant");
        write(retries - 1);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void abort() {}

  @Override
  public void close() {}

  private Value convertToJavaType(InternalRow record, StructField field, int fieldIndex) {
    DataType dataType = field.dataType();

    if (dataType == DataTypes.StringType
        || dataType == DataTypes.DateType
        || dataType == DataTypes.TimestampType) {
      return value(record.getString(fieldIndex));
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      ArrayData arrayData = record.getArray(fieldIndex);
      return convertArrayToValue(arrayData, arrayType.elementType());
    } else if (dataType instanceof StructType) {
      StructType structType = (StructType) dataType;
      InternalRow structData = record.getStruct(fieldIndex, structType.fields().length);
      return convertStructToValue(structData, structType);
    }

    return nullValue();
  }

  private Value convertArrayToValue(ArrayData arrayData, DataType elementType) {
    if (elementType == DataTypes.StringType) {
      int length = arrayData.numElements();
      List<Value> result = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        result.add(value(arrayData.getUTF8String(i).toString()));
      }
      return list((result));
    } else if (elementType instanceof StructType) {
      StructType structType = (StructType) elementType;
      int length = arrayData.numElements();
      List<Value> result = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        InternalRow structData = arrayData.getStruct(i, structType.fields().length);
        result.add(convertStructToValue(structData, structType));
      }
      return list(result);
    } else {
      return nullValue();
    }
  }

  private Value convertStructToValue(InternalRow structData, StructType structType) {
    Map<String, Value> result = new HashMap<>();
    for (int i = 0; i < structType.fields().length; i++) {
      StructField structField = structType.fields()[i];
      result.put(structField.name(), convertToJavaType(structData, structField, i));
    }

    Value value =
        Value.newBuilder().setStructValue(Struct.newBuilder().putAllFields(result)).build();
    return value;
  }
}
