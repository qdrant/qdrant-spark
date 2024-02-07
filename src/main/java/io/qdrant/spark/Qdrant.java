package io.qdrant.spark;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A class that implements the TableProvider and DataSourceRegister interfaces. Provides methods to
 * infer schema, get table, and check required options.
 */
public class Qdrant implements TableProvider, DataSourceRegister {

  private final String[] requiredFields =
      new String[] {"schema", "collection_name", "embedding_field", "qdrant_url"};

  /**
   * Returns the short name of the data source.
   *
   * @return The short name of the data source.
   */
  @Override
  public String shortName() {
    return "qdrant";
  }

  /**
   * Infers the schema of the data source based on the provided options.
   *
   * @param options The options used to infer the schema.
   * @return The inferred schema.
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {

    StructType schema = (StructType) StructType.fromJson(options.get("schema"));
    checkRequiredOptions(options, schema);

    return schema;
  }
  ;

  /**
   * Returns a table for the data source based on the provided schema, partitioning, and properties.
   *
   * @param schema The schema of the table.
   * @param partitioning The partitioning of the table.
   * @param properties The properties of the table.
   * @return The table for the data source.
   */
  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    QdrantOptions options = new QdrantOptions(properties);
    return new QdrantCluster(options, schema);
  }

  /**
   * Checks if the required options are present in the provided options and if the id_field and
   * embedding_field options are present in the provided schema.
   *
   * @param options The options to check.
   * @param schema The schema to check.
   */
  void checkRequiredOptions(CaseInsensitiveStringMap options, StructType schema) {
    for (String fieldName : requiredFields) {
      if (!options.containsKey(fieldName)) {
        throw new IllegalArgumentException(fieldName.concat(" option is required"));
      }
    }

    List<String> fieldNames = Arrays.asList(schema.fieldNames());

    if (options.containsKey("id_field")) {
      String idField = options.get("id_field").toString();

      if (!fieldNames.contains(idField)) {
        throw new IllegalArgumentException("id_field option is not present in the schema");
      }
    }

    String embeddingField = options.get("embedding_field").toString();

    if (!fieldNames.contains(embeddingField)) {
      throw new IllegalArgumentException("embedding_field option is not present in the schema");
    }
  }
}
