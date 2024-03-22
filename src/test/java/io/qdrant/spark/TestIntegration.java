package io.qdrant.spark;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.qdrant.QdrantContainer;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;

@Testcontainers
public class TestIntegration {

    private static String collectionName = "qdrant-spark-".concat(UUID.randomUUID().toString());
    private static int dimension = 4;
    private static int grpcPort = 6334;
    private static Distance distance = Distance.Cosine;

    @Rule
    public final QdrantContainer qdrant = new QdrantContainer("qdrant/qdrant");

    @Before
    public void setup() throws InterruptedException, ExecutionException {

        QdrantClient client = new QdrantClient(
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
        SparkSession spark = SparkSession.builder().master("local[1]").appName("qdrant-spark").getOrCreate();

        Dataset < Row > df = spark.read().schema(TestSchema.schema()).option("multiline", "true")
            .json(this.getClass().getResource("/users.json").toString());

        String qdrantUrl = String.join("", "http://", qdrant.getGrpcHostAddress());
        df.write()
            .format("io.qdrant.spark.Qdrant")
            .option("id_field", "id")
            .option("schema", df.schema().json())
            .option("collection_name", collectionName)
            .option("embedding_field", "dense_vector")
            .option("qdrant_url", qdrantUrl)
            .mode("append")
            .save();
    }
}