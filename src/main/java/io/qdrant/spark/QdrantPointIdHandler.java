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

    int idFieldIndex;
    try {
      idFieldIndex = schema.fieldIndex(idField);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Field '"
              + idField
              + "' specified in 'id_field' does not exist in the schema. "
              + "Available fields: "
              + String.join(", ", schema.fieldNames()),
          e);
    }

    DataType idFieldType = schema.fields()[idFieldIndex].dataType();
    switch (idFieldType.typeName()) {
      case "string":
        String idString = record.getString(idFieldIndex);
        if (idString == null) {
          throw new IllegalArgumentException(
              "The 'id_field' contains a null value. IDs cannot be null. "
                  + "Either provide valid ID values or remove 'id_field' option to use"
                  + " auto-generated UUIDs.");
        }
        try {
          return id(UUID.fromString(idString));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "The 'id_field' value '"
                  + idString
                  + "' is not a valid UUID. "
                  + "String IDs must be in UUID format (e.g.,"
                  + " '550e8400-e29b-41d4-a716-446655440000'). "
                  + "For non-UUID string IDs, consider using integer IDs or hashing your strings to"
                  + " UUIDs.",
              e);
        }

      case "integer":
        return id(record.getInt(idFieldIndex));

      case "long":
        return id(record.getLong(idFieldIndex));

      default:
        throw new IllegalArgumentException(
            "Point ID field '"
                + idField
                + "' has unsupported type '"
                + idFieldType.typeName()
                + "'. "
                + "Supported types: string (UUID format), integer, long");
    }
  }
}
