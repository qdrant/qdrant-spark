package io.qdrant.spark;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/** Qdrant cluster implementation supporting batch writes. */
public class QdrantCluster implements SupportsWrite {

  private final StructType schema;
  private final QdrantOptions options;

  private static final Set<TableCapability> CAPABILITIES = EnumSet.of(TableCapability.BATCH_WRITE);

  public QdrantCluster(QdrantOptions options, StructType schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new QdrantWriteBuilder(options, schema);
  }

  @Override
  public String name() {
    return "qdrant";
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.unmodifiableSet(CAPABILITIES);
  }
}
