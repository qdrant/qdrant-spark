package io.qdrant.spark;

import java.io.Serializable;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class QdrantWriteBuilder implements WriteBuilder, Serializable {

  private final StructType schema;
  private final QdrantOptions options;

  public QdrantWriteBuilder(QdrantOptions options, StructType schema) {
    this.schema = schema;
    this.options = options;
  }

  @Override
  public QdrantWrite build() {
    return new QdrantWrite(this.options, this.schema);
  }
}
