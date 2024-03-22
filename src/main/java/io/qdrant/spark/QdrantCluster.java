package io.qdrant.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/** QdrantCluster class implements the SupportsWrite interface. */
public class QdrantCluster implements SupportsWrite {

  private final StructType schema;
  private final QdrantOptions options;

  private static final Set<TableCapability> TABLE_CAPABILITY_SET =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(TableCapability.BATCH_WRITE, TableCapability.STREAMING_WRITE)));

  public QdrantCluster(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new QdrantWriteBuilder(this.options, this.schema);
  }

  @Override
  public String name() {
    return "qdrant";
  }

  @Override
  public StructType schema() {
    return this.schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return TABLE_CAPABILITY_SET;
  }
}
