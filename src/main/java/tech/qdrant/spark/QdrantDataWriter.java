package tech.qdrant.spark;

import java.io.Serializable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QdrantDataWriter implements DataWriter<InternalRow>, Serializable {
    private final QdrantOptions options;
    private final StructType schema;
    private static final Logger LOG = LoggerFactory.getLogger(QdrantDataWriter.class);

    public QdrantDataWriter(QdrantOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public void write(InternalRow record) {
        LOG.info("Writing record: ");

    }

    @Override
    public WriterCommitMessage commit() {
        return new WriterCommitMessage() {
            @Override
            public String toString() {
                return "Data committed to Qdrant";
            }
        };
    }

    @Override
    public void abort() {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

}
