package io.qdrant.spark;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Qdrant datasource for Apache Spark. */
public class Qdrant implements TableProvider, DataSourceRegister {

  private static final String[] REQUIRED_FIELDS = {"schema", "collection_name", "qdrant_url"};

  /** Returns the short name of the data source. */
  @Override
  public String shortName() {
    return "qdrant";
  }

  /**
   * Validates and infers the schema from the provided options.
   *
   * @throws IllegalArgumentException if required options are missing.
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    validateOptions(options);
    return (StructType) StructType.fromJson(options.get("schema"));
  }

  private void validateOptions(CaseInsensitiveStringMap options) {
    for (String field : REQUIRED_FIELDS) {
      if (!options.containsKey(field)) {
        throw new IllegalArgumentException(String.format("%s option is required", field));
      }
    }
  }

  /**
   * Creates a Qdrant table instance with validated options.
   *
   * @throws IllegalArgumentException if options are invalid.
   */
  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    QdrantOptions options = new QdrantOptions(properties);
    return new QdrantCluster(options, schema);
  }
}
