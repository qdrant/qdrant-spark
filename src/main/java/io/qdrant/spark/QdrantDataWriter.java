package io.qdrant.spark;

import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.VectorFactory.vector;
import static io.qdrant.client.VectorsFactory.namedVectors;
import static io.qdrant.client.VectorsFactory.vectors;
import static io.qdrant.spark.QdrantValueFactory.value;

import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.PointStruct;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DataWriter implementation that writes data to Qdrant, a vector search engine. This class takes
 * QdrantOptions and StructType as input and writes data to QdrantGRPC. It implements the DataWriter
 * interface and overrides its methods write, commit, abort and close. It also has a private method
 * write that is used to upload a batch of points to Qdrant. The class uses a Point class to
 * represent a data point and an ArrayList to store the points.
 */
public class QdrantDataWriter implements DataWriter<InternalRow>, Serializable {
  private final QdrantOptions options;
  private final StructType schema;
  private final String qdrantUrl;
  private final String apiKey;
  private final Logger LOG = LoggerFactory.getLogger(QdrantDataWriter.class);

  private final ArrayList<PointStruct> points = new ArrayList<>();

  public QdrantDataWriter(QdrantOptions options, StructType schema) throws Exception {
    this.options = options;
    this.schema = schema;
    this.qdrantUrl = options.qdrantUrl;
    this.apiKey = options.apiKey;
  }

  @Override
  public void write(InternalRow record) {
    PointStruct.Builder pointBuilder = PointStruct.newBuilder();
    Map<String, Value> payload = new HashMap<>();

    if (this.options.idField == null) {
      pointBuilder.setId(id(UUID.randomUUID()));
    }
    for (StructField field : this.schema.fields()) {
      int fieldIndex = this.schema.fieldIndex(field.name());
      if (this.options.idField != null && field.name().equals(this.options.idField)) {

        DataType dataType = field.dataType();
        switch (dataType.typeName()) {
          case "string":
            pointBuilder.setId(id(UUID.fromString(record.getString(fieldIndex))));
            break;

          case "integer":
            pointBuilder.setId(id(record.getInt(fieldIndex)));
            break;
          default:
            break;
        }

      } else if (field.name().equals(this.options.embeddingField)) {
        float[] embeddings = record.getArray(fieldIndex).toFloatArray();
        if (options.vectorName != null) {
          pointBuilder.setVectors(
              namedVectors(Collections.singletonMap(options.vectorName, vector(embeddings))));
        } else {
          pointBuilder.setVectors(vectors(embeddings));
        }
      } else {
        payload.put(field.name(), value(record, field, fieldIndex));
      }
    }

    pointBuilder.putAllPayload(payload);
    this.points.add(pointBuilder.build());

    if (this.points.size() >= this.options.batchSize) {
      this.write(this.options.retries);
    }
  }

  @Override
  public WriterCommitMessage commit() {
    this.write(this.options.retries);
    return new WriterCommitMessage() {
      @Override
      public String toString() {
        return "point committed to Qdrant";
      }
    };
  }

  public void write(int retries) {
    LOG.info(
        String.join(
            "", "Uploading batch of ", Integer.toString(this.points.size()), " points to Qdrant"));

    if (this.points.isEmpty()) {
      return;
    }
    try {
      // Instantiate a new QdrantGrpc object to maintain serializability
      QdrantGrpc qdrant = new QdrantGrpc(new URL(this.qdrantUrl), this.apiKey);
      qdrant.upsert(this.options.collectionName, this.points);
      qdrant.close();
      this.points.clear();
    } catch (Exception e) {
      LOG.error(String.join("", "Exception while uploading batch to Qdrant: ", e.getMessage()));
      if (retries > 0) {
        LOG.info("Retrying upload batch to Qdrant");
        write(retries - 1);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void abort() {}
  public void abort() {}

  @Override
  public void close() {}
}
