package io.qdrant.spark;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.qdrant.QdrantContainer;

@Testcontainers
public class TestIntegration {

  private static String collectionName = "qdrant-spark-".concat(UUID.randomUUID().toString());
  private static int dimension = 4;
  private static int grpcPort = 6334;
  private static Distance distance = Distance.Cosine;

  @Rule public final QdrantContainer qdrant = new QdrantContainer("qdrant/qdrant");

  @Before
  public void setup() throws InterruptedException, ExecutionException {

    QdrantClient client =
        new QdrantClient(
            QdrantGrpcClient.newBuilder(qdrant.getHost(), qdrant.getMappedPort(grpcPort), false)
                .build());

    client
        .createCollectionAsync(
            collectionName,
            VectorParams.newBuilder().setDistance(distance).setSize(dimension).build())
        .get();

    client.close();
  }

  @Test
  public void testSparkSession() {
    SparkSession spark =
        SparkSession.builder().master("local[1]").appName("qdrant-spark").getOrCreate();

    List<Row> data =
        Arrays.asList(
            RowFactory.create(
                "3803333d-981d-4e76-8f08-f3ba72d006af",
                1,
                new float[] {1.0f, 2.0f, 3.0f, 0.3f},
                "John Doe",
                new String[] {"Hello", "Hi"},
                RowFactory.create(99f, "AnotherNestedStruct"),
                new int[] {4, 32, 323, 788}),
            RowFactory.create(
                "0ca61aca-c770-4f4e-9de6-41a58a98463b",
                2,
                new float[] {4.0f, 5.0f, 6.0f, 0.3f},
                "Jane Doe",
                new String[] {"Bonjour", "Salut"},
                RowFactory.create(99f, "NestedStruct"),
                new int[] {1, 2, 3, 4, 5}));

    StructType structType =
        new StructType(
            new StructField[] {
              new StructField("nested_id", DataTypes.FloatType, false, Metadata.empty()),
              new StructField("nested_value", DataTypes.StringType, false, Metadata.empty())
            });

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, false, Metadata.empty()),
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

    String qdrantUrl = String.join("", "http://", qdrant.getGrpcHostAddress());
    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", collectionName)
        .option("embedding_field", "embedding")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();
    ;
  }
}
