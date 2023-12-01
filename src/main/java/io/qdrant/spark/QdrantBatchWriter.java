package io.qdrant.spark;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * QdrantBatchWriter class implements the BatchWrite interface and provides a factory for creating
 * QdrantDataWriterFactory instances. It also provides methods for committing or aborting write
 * operations.
 */
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
  public void commit(WriterCommitMessage[] messages) {
    // TODO Auto-generated method stub

  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // TODO Auto-generated method stub
  }
}
