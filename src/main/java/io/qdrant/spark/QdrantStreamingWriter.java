package io.qdrant.spark;

import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

/** A class that implements the Spark StreamingWrite interface for writing data to Qdrant. */
public class QdrantStreamingWriter implements StreamingWrite {

  private final QdrantOptions options;
  private final StructType schema;

  /**
   * Constructor for QdrantStreamingWriter.
   *
   * @param options The options for writing data to Qdrant.
   * @param schema The schema of the data to be written.
   */
  public QdrantStreamingWriter(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
  }

  /**
   * Creates a StreamingDataWriterFactory for writing data to Qdrant.
   *
   * @param info The PhysicalWriteInfo object containing information about the write operation.
   * @return A QdrantDataWriterFactory object for writing data to Qdrant.
   */
  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    return new QdrantDataWriterFactory(options, schema);
  }

  /**
   * Commits the write operation for the given epochId and messages.
   *
   * @param epochId The epochId of the write operation.
   * @param messages The WriterCommitMessage objects containing information about the write
   *     operation.
   */
  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    // TODO Auto-generated method stub

  }

  /**
   * Aborts the write operation for the given epochId and messages.
   *
   * @param epochId The epochId of the write operation.
   * @param messages The WriterCommitMessage objects containing information about the write
   *     operation.
   */
  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    // TODO Auto-generated method stub
  }
}
