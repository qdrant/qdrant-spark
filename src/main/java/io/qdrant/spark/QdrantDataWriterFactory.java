package io.qdrant.spark;

import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;

/** Factory class for creating QdrantDataWriter instances for Spark data sources. */
public class QdrantDataWriterFactory implements StreamingDataWriterFactory, DataWriterFactory {

  private final QdrantOptions options;
  private final StructType schema;

  public QdrantDataWriterFactory(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public QdrantDataWriter createWriter(int partitionId, long taskId, long epochId) {
    return createWriter(partitionId, taskId);
  }

  @Override
  public QdrantDataWriter createWriter(int partitionId, long taskId) {
    try {
      return new QdrantDataWriter(options, schema);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
