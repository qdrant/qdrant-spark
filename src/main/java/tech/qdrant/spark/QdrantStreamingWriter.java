package tech.qdrant.spark;

import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

public class QdrantStreamingWriter implements StreamingWrite {

    private final QdrantOptions options;
    private final StructType schema;

    public QdrantStreamingWriter(QdrantOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return new QdrantDataWriterFactory(options, schema);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        // TODO Auto-generated method stub

    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        // TODO Auto-generated method stub
    }

}
