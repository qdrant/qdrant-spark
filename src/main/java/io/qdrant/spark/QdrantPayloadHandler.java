package io.qdrant.spark;

import static io.qdrant.spark.QdrantValueFactory.value;

import io.qdrant.client.grpc.JsonWithInt.Value;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class QdrantPayloadHandler {
  static Map<String, Value> preparePayload(
      InternalRow record, StructType schema, QdrantOptions options) {

    Map<String, Value> payload = new HashMap<>();
    for (StructField field : schema.fields()) {

      if (options.payloadFieldsToSkip.contains(field.name())) {
        continue;
      }
      int fieldIndex = schema.fieldIndex(field.name());
      payload.put(field.name(), value(record, field, fieldIndex));
    }

    return payload;
  }
}
