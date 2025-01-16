package io.qdrant.spark;

import io.qdrant.client.grpc.Points.PointStruct;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DataWriter implementation for writing data to Qdrant. */
public class QdrantDataWriter implements DataWriter<InternalRow>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(QdrantDataWriter.class);

  private final QdrantOptions options;
  private final StructType schema;
  private final List<PointStruct> pointsBuffer = new ArrayList<>();

  public QdrantDataWriter(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public void write(InternalRow record) {
    PointStruct point = createPointStruct(record);
    pointsBuffer.add(point);

    if (pointsBuffer.size() >= options.batchSize) {
      writeBatch(options.retries);
    }
  }

  private PointStruct createPointStruct(InternalRow record) {
    PointStruct.Builder pointBuilder = PointStruct.newBuilder();
    pointBuilder.setId(QdrantPointIdHandler.preparePointId(record, schema, options));
    pointBuilder.setVectors(QdrantVectorHandler.prepareVectors(record, schema, options));
    pointBuilder.putAllPayload(QdrantPayloadHandler.preparePayload(record, schema, options));
    return pointBuilder.build();
  }

  private void writeBatch(int retries) {
    if (pointsBuffer.isEmpty()) {
      return;
    }

    try {
      doWriteBatch();
      pointsBuffer.clear();
    } catch (Exception e) {
      LOG.error("Exception while uploading batch to Qdrant: {}", e.getMessage());
      if (retries > 0) {
        LOG.info("Retrying upload batch to Qdrant");
        writeBatch(retries - 1);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private void doWriteBatch() throws Exception {
    LOG.info("Uploading batch of {} points to Qdrant", pointsBuffer.size());

    // Instantiate QdrantGrpc client for each batch to maintain serializability
    QdrantGrpc qdrant = new QdrantGrpc(new URL(options.qdrantUrl), options.apiKey);
    qdrant.upsert(options.collectionName, pointsBuffer, options.shardKeySelector, options.wait);
    qdrant.close();
  }

  @Override
  public WriterCommitMessage commit() {
    writeBatch(options.retries);
    return new WriterCommitMessage() {
      @Override
      public String toString() {
        return "point committed to Qdrant";
      }
    };
  }

  @Override
  public void abort() {}

  @Override
  public void close() {}
}
