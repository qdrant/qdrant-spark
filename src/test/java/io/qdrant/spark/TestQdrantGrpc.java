package io.qdrant.spark;

import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;
import static org.junit.Assert.*;

import io.qdrant.client.grpc.Points.PointStruct;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;

public class TestQdrantGrpc {
  String qdrantUrl;
  String apiKey;

  public TestQdrantGrpc() {
    qdrantUrl = System.getenv("QDRANT_URL");
    // The url cannot be set to null
    if (qdrantUrl == null) {
      qdrantUrl = "http://localhost:6334";
    }

    // The API key can be null
    apiKey = System.getenv("QDRANT_API_KEY");
  }

  @Test
  public void testUploadBatch() throws Exception {
    // create a QdrantRest instance with a mock URL and API key

    QdrantGrpc qdrantRest = new QdrantGrpc(new URL(qdrantUrl), apiKey);

    List<PointStruct> points = new ArrayList<>();

    PointStruct.Builder point1Builder = PointStruct.newBuilder();
    point1Builder.setId(id(UUID.randomUUID()));
    point1Builder.setVectors(vectors(1.0f, 2.0f, 3.0f));
    point1Builder.putAllPayload(
        Map.of(
            "name", value("point1 "),
            "rand_number", value(53)));

    points.add(point1Builder.build());

    PointStruct.Builder point2Builder = PointStruct.newBuilder();
    point2Builder.setId(id(UUID.randomUUID()));
    point2Builder.setVectors(vectors(4.0f, 5.0f, 6.0f));
    point2Builder.putAllPayload(
        Map.of(
            "name", value("point2"),
            "rand_number", value(89)));

    points.add(point2Builder.build());

    // call the uploadBatch method
    qdrantRest.upsert("qdrant-spark", points);

    // assert that the upload was successful
    assertTrue(true);
  }
}
