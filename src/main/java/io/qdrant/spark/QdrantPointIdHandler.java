package io.qdrant.spark;

import static io.qdrant.client.PointIdFactory.id;

import io.qdrant.client.grpc.Points.PointId;
import java.util.UUID;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public class QdrantPointIdHandler {
  static PointId preparePointId(InternalRow record, StructType schema, QdrantOptions options) {
    String idField = options.idField;

    if (idField.isEmpty()) {
      return id(UUID.randomUUID());
    }

    int idFieldIndex = schema.fieldIndex(idField.trim());
    DataType idFieldType = schema.fields()[idFieldIndex].dataType();
    switch (idFieldType.typeName()) {
      case "string":
        return id(UUID.fromString(record.getString(idFieldIndex)));

      case "integer":
      case "long":
        return id(record.getInt(idFieldIndex));

      default:
        throw new IllegalArgumentException("Point ID should be of type string or integer");
    }
  }
}
