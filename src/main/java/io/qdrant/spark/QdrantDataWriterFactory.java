package io.qdrant.spark;

import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;

/** Factory class for creating QdrantDataWriter instances for Spark Structured Streaming. */
public class QdrantDataWriterFactory implements StreamingDataWriterFactory, DataWriterFactory {
  private final QdrantOptions options;
  private final StructType schema;

  /**
   * Constructor for QdrantDataWriterFactory.
   *
   * @param options QdrantOptions instance containing configuration options for Qdrant.
   * @param schema StructType instance containing schema information for the data being written.
   */
  public QdrantDataWriterFactory(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public QdrantDataWriter createWriter(int partitionId, long taskId, long epochId) {
    return new QdrantDataWriter(this.options, this.schema);
  }

  @Override
  public QdrantDataWriter createWriter(int partitionId, long taskId) {
    return new QdrantDataWriter(this.options, this.schema);
  }
}
