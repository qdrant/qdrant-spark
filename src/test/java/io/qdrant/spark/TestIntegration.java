package io.qdrant.spark;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.qdrant.QdrantContainer;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.CreateCollection;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.SparseVectorConfig;
import io.qdrant.client.grpc.Collections.SparseVectorParams;
import io.qdrant.client.grpc.Collections.VectorParams;
import io.qdrant.client.grpc.Collections.VectorParamsMap;
import io.qdrant.client.grpc.Collections.VectorsConfig;

@Testcontainers
public class TestIntegration {
    private final int GRPC_PORT = 6334;
    private final int DIMENSION = 6;
    private final Distance DISTANCE = Distance.Cosine;
    private String qdrantUrl;
    private String collectionName;
    private QdrantClient client;
    private SparkSession spark;
    Dataset < Row > df;

    @Rule
    public final QdrantContainer qdrant = new QdrantContainer("qdrant/qdrant");

    @Before
    public void setup() throws InterruptedException, ExecutionException {
        collectionName = UUID.randomUUID().toString();
        qdrantUrl = String.join("", "http://", qdrant.getGrpcHostAddress());

        client = new QdrantClient(
            QdrantGrpcClient.newBuilder(qdrant.getHost(), qdrant.getMappedPort(GRPC_PORT), false)
            .build());

        CreateCollection collectionConfig = CreateCollection.newBuilder()
            .setCollectionName(collectionName)
            .setVectorsConfig(VectorsConfig.newBuilder()
                .setParamsMap(VectorParamsMap.newBuilder()
                    .putMap("dense",
                        VectorParams.newBuilder().setDistance(DISTANCE).setSize(DIMENSION).build())
                    .putMap("another_dense",
                        VectorParams.newBuilder().setDistance(DISTANCE).setSize(DIMENSION).build())
                    .putMap("",
                        VectorParams.newBuilder().setDistance(DISTANCE).setSize(DIMENSION).build())
                    .build())
                .build())
            .setSparseVectorsConfig(
                SparseVectorConfig.newBuilder()
                .putMap("sparse", SparseVectorParams.getDefaultInstance())
                .putMap("another_sparse", SparseVectorParams.getDefaultInstance()))
            .build();

        client
            .createCollectionAsync(collectionConfig).get();
        spark = SparkSession.builder().master("local[1]").appName("qdrant-spark").getOrCreate();

        df = spark.read().schema(TestSchema.schema()).option("multiline", "true")
            .json(this.getClass().getResource("/users.json").toString());

    }

    @Test
    public void testUnnamedVector() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("embedding_field", "dense_vector")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testUnnamedVector()", client.countAsync(collectionName).get() == df.count());
    }

    @Test
    public void testNamedVector() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("embedding_field", "dense_vector")
            .option("vector_name", "dense")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testNamedVector()", client.countAsync(collectionName).get() == df.count());
    }

    @Test
    public void testMultipleNamedVectors() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("vector_fields", "dense_vector,dense_vector")
            .option("vector_names", "dense,another_dense")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testMultipleNamedVectors()", client.countAsync(collectionName).get() == df.count());
    }

    @Test
    public void testSparseVector() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("sparse_vector_value_fields", "sparse_values")
            .option("sparse_vector_index_fields", "sparse_indices")
            .option("sparse_vector_names", "sparse")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testSparseVector()", client.countAsync(collectionName).get() == df.count());
    }

    @Test
    public void testMultipleSparseVectors() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("sparse_vector_value_fields", "sparse_values,sparse_values")
            .option("sparse_vector_index_fields", "sparse_indices,sparse_indices")
            .option("sparse_vector_names", "sparse,another_sparse")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testMultipleSparseVectors()", client.countAsync(collectionName).get() == df.count());
    }

    @Test
    public void testDenseAndSparseVector() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("vector_fields", "dense_vector")
            .option("vector_names", "dense")
            .option("sparse_vector_value_fields", "sparse_values")
            .option("sparse_vector_index_fields", "sparse_indices")
            .option("sparse_vector_names", "sparse")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testDenseAndSparseVector()", client.countAsync(collectionName).get() == df.count());
    }

    @Test
    public void testUnnamedDenseAndSparseVector() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("embedding_field", "dense_vector")
            .option("sparse_vector_value_fields", "sparse_values")
            .option("sparse_vector_index_fields", "sparse_indices")
            .option("sparse_vector_names", "sparse")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testUnnamedDenseAndSparseVector()", client.countAsync(collectionName).get() == df.count());
    }

    @Test
    public void testMultipleDenseAndSparseVectors() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("vector_fields", "dense_vector,dense_vector")
            .option("vector_names", "dense,another_dense")
            .option("sparse_vector_value_fields", "sparse_values,sparse_values")
            .option("sparse_vector_index_fields", "sparse_indices,sparse_indices")
            .option("sparse_vector_names", "sparse,another_sparse")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testMultipleDenseAndSparseVectors()", client.countAsync(collectionName).get() == df.count());
    }

    @Test
    public void testNoVectors() throws InterruptedException, ExecutionException {

        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();

        Assert.assertTrue("testMultipleDenseAndSparseVectors()", client.countAsync(collectionName).get() == df.count());
    }
}
