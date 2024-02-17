package io.qdrant.spark;

import static io.qdrant.spark.ObjectFactory.object;

import java.io.Serializable;
import java.util.ArrayList;
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
 * QdrantOptions and StructType as input and writes data to QdrantRest. It implements the DataWriter
 * interface and overrides its methods write, commit, abort and close. It also has a private method
 * write that is used to upload a batch of points to Qdrant. The class uses a Point class to
 * represent a data point and an ArrayList to store the points.
 */
public class QdrantDataWriter implements DataWriter<InternalRow>, Serializable {
  private final QdrantOptions options;
  private final StructType schema;
  private final QdrantRest qdrantRest;
  private final Logger LOG = LoggerFactory.getLogger(QdrantDataWriter.class);

  private final ArrayList<Point> points = new ArrayList<>();

  public QdrantDataWriter(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
    this.qdrantRest = new QdrantRest(this.options.qdrantUrl, this.options.apiKey);
  }

  @Override
  public void write(InternalRow record) {
    Point point = new Point();
    HashMap<String, Object> payload = new HashMap<>();

    if (this.options.idField == null) {
      point.id = UUID.randomUUID().toString();
    }
    for (StructField field : this.schema.fields()) {
      int fieldIndex = this.schema.fieldIndex(field.name());
      if (this.options.idField != null && field.name().equals(this.options.idField)) {

        DataType dataType = field.dataType();
        switch (dataType.typeName()) {
          case "string":
            point.id = record.getString(fieldIndex);
            break;

          case "integer":
            point.id = record.getInt(fieldIndex);
            break;
          default:
            throw new IllegalArgumentException("Point ID should be of type string or integer");
        }
      } else if (field.name().equals(this.options.embeddingField)) {
        point.vector = record.getArray(fieldIndex).toFloatArray();

      } else {
        payload.put(field.name(), object(record, field, fieldIndex));
      }
    }

    point.payload = payload;
    this.points.add(point);

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
    LOG.info("Upload batch of " + this.points.size() + " points to Qdrant");
    if (this.points.isEmpty()) {
      return;
    }
    try {
      this.qdrantRest.uploadBatch(this.options.collectionName, this.points);
      this.points.clear();
    } catch (Exception e) {
      LOG.error("Exception while uploading batch to Qdrant: " + e.getMessage());
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

  @Override
  public void close() {}
}

class Point implements Serializable {
  public Object id;
  public float[] vector;
  public HashMap<String, Object> payload;
}
