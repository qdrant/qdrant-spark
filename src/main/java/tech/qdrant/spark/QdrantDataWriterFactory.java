package tech.qdrant.spark;

import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class QdrantDataWriterFactory implements StreamingDataWriterFactory, DataWriterFactory {
    private final QdrantOptions options;
    private final StructType schema;

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
