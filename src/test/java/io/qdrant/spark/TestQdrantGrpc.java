package io.qdrant.spark;

import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;
import io.qdrant.client.grpc.Points.PointStruct;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.qdrant.QdrantContainer;

@Testcontainers
public class TestQdrantGrpc {
  private static String collectionName = "qdrant-spark-".concat(UUID.randomUUID().toString());
  private static int dimension = 3;
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
  public void testUploadBatch() throws Exception {
    String qdrantUrl = String.join("", "http://", qdrant.getGrpcHostAddress());
    QdrantGrpc qdrantGrpc = new QdrantGrpc(new URL(qdrantUrl), null);

    List<PointStruct> points = new ArrayList<>();

    PointStruct.Builder point1Builder = PointStruct.newBuilder();
    point1Builder.setId(id(UUID.randomUUID()));
    point1Builder.setVectors(vectors(1.0f, 2.0f, 3.0f));

    point1Builder.putPayload("name", value("point1 "));
    point1Builder.putPayload("rand_number", value(53));
    points.add(point1Builder.build());

    PointStruct.Builder point2Builder = PointStruct.newBuilder();
    point2Builder.setId(id(UUID.randomUUID()));
    point2Builder.setVectors(vectors(4.0f, 5.0f, 6.0f));

    point2Builder.putPayload("name", value("point2"));
    point2Builder.putPayload("rand_number", value(89));
    points.add(point2Builder.build());

    // call the uploadBatch method
    qdrantGrpc.upsert(collectionName, points);

    qdrantGrpc.close();
  }
}
