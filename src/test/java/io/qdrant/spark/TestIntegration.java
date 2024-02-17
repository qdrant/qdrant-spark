package io.qdrant.spark;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class TestIntegration {

  String qdrantUrl;
  String apiKey;

  public TestIntegration() {
    qdrantUrl = System.getenv("QDRANT_URL");
    // The url cannot be set to null
    if (qdrantUrl == null) {
      qdrantUrl = "http://localhost:6333";
    }

    // The API key can be null
    apiKey = System.getenv("QDRANT_API_KEY");
  }

  @Test
  public void testSparkSession() {
    SparkSession spark =
        SparkSession.builder().master("local[1]").appName("qdrant-spark").getOrCreate();

    List<Row> data =
        Arrays.asList(
            RowFactory.create(
                1,
                1,
                new float[] {1.0f, 2.0f, 3.0f},
                "John Doe",
                new String[] {"Hello", "Hi"},
                RowFactory.create(99, "AnotherNestedStruct"),
                new int[] {4, 32, 323, 788}),
            RowFactory.create(
                2,
                2,
                new float[] {4.0f, 5.0f, 6.0f},
                "Jane Doe",
                new String[] {"Bonjour", "Salut"},
                RowFactory.create(99, "AnotherNestedStruct"),
                new int[] {1, 2, 3, 4, 5}));

    StructType structType =
        new StructType(
            new StructField[] {
              new StructField("nested_id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("nested_value", DataTypes.StringType, false, Metadata.empty())
            });

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("score", DataTypes.IntegerType, true, Metadata.empty()),
              new StructField(
                  "embedding",
                  DataTypes.createArrayType(DataTypes.FloatType),
                  true,
                  Metadata.empty()),
              new StructField("name", DataTypes.StringType, true, Metadata.empty()),
              new StructField(
                  "greetings",
                  DataTypes.createArrayType(DataTypes.StringType),
                  true,
                  Metadata.empty()),
              new StructField("struct_data", structType, true, Metadata.empty()),
              new StructField(
                  "numbers",
                  DataTypes.createArrayType(DataTypes.IntegerType),
                  true,
                  Metadata.empty()),
            });
    Dataset<Row> df = spark.createDataFrame(data, schema);

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("schema", df.schema().json())
        .option("collection_name", "qdrant-spark")
        .option("embedding_field", "embedding")
        .option("qdrant_url", qdrantUrl)
        .option("api_key", apiKey)
        .mode("append")
        .save();
    ;
  }
}
