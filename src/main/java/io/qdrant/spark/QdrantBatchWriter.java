package io.qdrant.spark;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/** Qdrant batch writer for Apache Spark. */
public class QdrantBatchWriter implements BatchWrite {

  private final QdrantOptions options;
  private final StructType schema;

  public QdrantBatchWriter(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new QdrantDataWriterFactory(options, schema);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {}

  @Override
  public void abort(WriterCommitMessage[] messages) {}
}
