package io.qdrant.spark;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Assert;
import org.junit.Test;

public class TestQdrant {

  @Test
  public void testShortName() {
    Qdrant qdrant = new Qdrant();
    Assert.assertEquals("qdrant", qdrant.shortName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInferSchemaMissingOption() {
    Qdrant qdrant = new Qdrant();
    Map<String, String> options = new HashMap<>();
    CaseInsensitiveStringMap dataSourceOptions = new CaseInsensitiveStringMap(options);
    qdrant.inferSchema(dataSourceOptions);
  }

  @Test
  public void testInferSchema() {
    Qdrant qdrant = new Qdrant();
    StructType schema =
        new StructType()
            .add("id", DataTypes.StringType)
            .add("embedding", DataTypes.createArrayType(DataTypes.FloatType));
    Map<String, String> options = new HashMap<>();
    options.put("schema", schema.json());
    options.put("collection_name", "test_collection");
    options.put("embedding_field", "embedding");
    options.put("qdrant_url", "http://localhost:6334");
    CaseInsensitiveStringMap dataSourceOptions = new CaseInsensitiveStringMap(options);
    StructType inferredSchema = qdrant.inferSchema(dataSourceOptions);
    Assert.assertEquals(schema, inferredSchema);
  }

  @Test
  public void testGetTable() {
    Qdrant qdrant = new Qdrant();
    StructType schema =
        new StructType()
            .add("id", DataTypes.StringType)
            .add("embedding", DataTypes.createArrayType(DataTypes.FloatType));
    Map<String, String> options = new HashMap<>();
    options.put("schema", schema.json());
    options.put("collection_name", "test_collection");
    options.put("embedding_field", "embedding");
    options.put("qdrant_url", "http://localhost:6334");
    CaseInsensitiveStringMap dataSourceOptions = new CaseInsensitiveStringMap(options);
    Assert.assertTrue(qdrant.getTable(schema, null, dataSourceOptions) instanceof QdrantCluster);
  }

  @Test()
  public void testCheckRequiredOptions() {
    Qdrant qdrant = new Qdrant();
    StructType schema =
        new StructType()
            .add("id", DataTypes.StringType)
            .add("embedding", DataTypes.createArrayType(DataTypes.FloatType));
    Map<String, String> options = new HashMap<>();
    options.put("schema", schema.json());
    options.put("collection_name", "test_collection");
    options.put("embedding_field", "embedding");
    options.put("qdrant_url", "http://localhost:6334");
    CaseInsensitiveStringMap dataSourceOptions = new CaseInsensitiveStringMap(options);
    qdrant.validateOptions(dataSourceOptions, schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckRequiredOptionsMissingIdField() {
    Qdrant qdrant = new Qdrant();
    StructType schema =
        new StructType().add("embedding", DataTypes.createArrayType(DataTypes.FloatType));
    Map<String, String> options = new HashMap<>();
    options.put("schema", schema.json());
    options.put("collection_name", "test_collection");
    options.put("embedding_field", "embedding");
    options.put("qdrant_url", "http://localhost:6334");
    options.put("id_field", "id");
    CaseInsensitiveStringMap dataSourceOptions = new CaseInsensitiveStringMap(options);
    qdrant.validateOptions(dataSourceOptions, schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckRequiredOptionsMissingEmbeddingField() {
    Qdrant qdrant = new Qdrant();
    StructType schema = new StructType().add("id", DataTypes.StringType);
    Map<String, String> options = new HashMap<>();
    options.put("schema", schema.json());
    options.put("collection_name", "test_collection");
    options.put("qdrant_url", "http://localhost:6334");
    options.put("id_field", "id");
    CaseInsensitiveStringMap dataSourceOptions = new CaseInsensitiveStringMap(options);
    qdrant.inferSchema(dataSourceOptions);
  }
}
