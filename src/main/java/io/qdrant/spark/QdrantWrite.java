package io.qdrant.spark;

import java.io.Serializable;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

public class QdrantWrite implements Write, Serializable {
  private final StructType schema;
  private final QdrantOptions options;

  public QdrantWrite(QdrantOptions options, StructType schema) {
    this.schema = schema;
    this.options = options;
  }

  @Override
  public BatchWrite toBatch() {
    return new QdrantBatchWriter(this.options, this.schema);
  }

  @Override
  public StreamingWrite toStreaming() {
    return new QdrantStreamingWriter(this.options, this.schema);
  }
}
