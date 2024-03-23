package io.qdrant.spark;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.CreateCollection;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.SparseVectorConfig;
import io.qdrant.client.grpc.Collections.SparseVectorParams;
import io.qdrant.client.grpc.Collections.VectorParams;
import io.qdrant.client.grpc.Collections.VectorParamsMap;
import io.qdrant.client.grpc.Collections.VectorsConfig;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.qdrant.QdrantContainer;

@Testcontainers
public class TestIntegration {
  private final int GRPC_PORT = 6334;
  private final int DIMENSION = 6;
  private final Distance DISTANCE = Distance.Cosine;
  private final String COLLECTION_NAME = UUID.randomUUID().toString();
  private String qdrantUrl;
  private QdrantClient client;
  private SparkSession spark;
  Dataset<Row> df;

  @Rule public final QdrantContainer qdrant = new QdrantContainer("qdrant/qdrant");

  @Before
  public void setup() throws InterruptedException, ExecutionException {

    qdrantUrl = String.join("", "http://", qdrant.getGrpcHostAddress());

    client =
        new QdrantClient(
            QdrantGrpcClient.newBuilder(qdrant.getHost(), qdrant.getMappedPort(GRPC_PORT), false)
                .build());

    CreateCollection collectionConfig =
        CreateCollection.newBuilder()
            .setCollectionName(COLLECTION_NAME)
            .setVectorsConfig(
                VectorsConfig.newBuilder()
                    .setParamsMap(
                        VectorParamsMap.newBuilder()
                            .putMap(
                                "",
                                VectorParams.newBuilder()
                                    .setDistance(DISTANCE)
                                    .setSize(DIMENSION)
                                    .build())
                            .putMap(
                                "dense",
                                VectorParams.newBuilder()
                                    .setDistance(DISTANCE)
                                    .setSize(DIMENSION)
                                    .build())
                            .putMap(
                                "another_dense",
                                VectorParams.newBuilder()
                                    .setDistance(DISTANCE)
                                    .setSize(DIMENSION)
                                    .build())
                            .build())
                    .build())
            .setSparseVectorsConfig(
                SparseVectorConfig.newBuilder()
                    .putMap("sparse", SparseVectorParams.getDefaultInstance())
                    .putMap("another_sparse", SparseVectorParams.getDefaultInstance()))
            .build();

    client.createCollectionAsync(collectionConfig).get();
    spark = SparkSession.builder().master("local[1]").appName("qdrant-spark").getOrCreate();

    df =
        spark
            .read()
            .schema(TestSchema.schema())
            .option("multiline", "true")
            .json(this.getClass().getResource("/users.json").toString());
  }

  @After
  public void tearDown() throws InterruptedException, ExecutionException {
    spark.stop();
    client.close();
  }

  @Test
  public void testUnnamedVector() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("embedding_field", "dense_vector")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testUnnamedVector()", (long) client.countAsync(COLLECTION_NAME).get(), df.count());
  }

  @Test
  public void testNamedVector() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("embedding_field", "dense_vector")
        .option("vector_name", "dense")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testNamedVector()", (long) client.countAsync(COLLECTION_NAME).get(), df.count());
  }

  @Test
  public void testMultipleNamedVectors() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("vector_fields", "dense_vector,dense_vector")
        .option("vector_names", "dense,another_dense")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testMultipleNamedVectors()", (long) client.countAsync(COLLECTION_NAME).get(), df.count());
  }

  @Test
  public void testSparseVector() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("sparse_vector_value_fields", "sparse_values")
        .option("sparse_vector_index_fields", "sparse_indices")
        .option("sparse_vector_names", "sparse")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testSparseVector()", (long) client.countAsync(COLLECTION_NAME).get(), df.count());
  }

  @Test
  public void testMultipleSparseVectors() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("sparse_vector_value_fields", "sparse_values,sparse_values")
        .option("sparse_vector_index_fields", "sparse_indices,sparse_indices")
        .option("sparse_vector_names", "sparse,another_sparse")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testMultipleSparseVectors()", (long) client.countAsync(COLLECTION_NAME).get(), df.count());
  }

  @Test
  public void testDenseAndSparseVector() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("vector_fields", "dense_vector")
        .option("vector_names", "dense")
        .option("sparse_vector_value_fields", "sparse_values")
        .option("sparse_vector_index_fields", "sparse_indices")
        .option("sparse_vector_names", "sparse")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testDenseAndSparseVector()", (long) client.countAsync(COLLECTION_NAME).get(), df.count());
  }

  @Test
  public void testUnnamedDenseAndSparseVector() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("embedding_field", "dense_vector")
        .option("sparse_vector_value_fields", "sparse_values")
        .option("sparse_vector_index_fields", "sparse_indices")
        .option("sparse_vector_names", "sparse")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testUnnamedDenseAndSparseVector()",
        (long) client.countAsync(COLLECTION_NAME).get(),
        df.count());
  }

  @Test
  public void testMultipleDenseAndSparseVectors() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("vector_fields", "dense_vector,dense_vector")
        .option("vector_names", "dense,another_dense")
        .option("sparse_vector_value_fields", "sparse_values,sparse_values")
        .option("sparse_vector_index_fields", "sparse_indices,sparse_indices")
        .option("sparse_vector_names", "sparse,another_sparse")
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testMultipleDenseAndSparseVectors()",
        (long) client.countAsync(COLLECTION_NAME).get(),
        df.count());
  }

  @Test
  public void testNoVectors() throws InterruptedException, ExecutionException {

    df.write()
        .format("io.qdrant.spark.Qdrant")
        .option("id_field", "id")
        .option("schema", df.schema().json())
        .option("collection_name", COLLECTION_NAME)
        .option("qdrant_url", qdrantUrl)
        .mode("append")
        .save();

    Assert.assertEquals(
        "testNoVectors()", (long) client.countAsync(COLLECTION_NAME).get(), df.count());
  }
}
