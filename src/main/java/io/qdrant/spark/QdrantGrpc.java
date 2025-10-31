package io.qdrant.spark;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.ShardKeySelector;
import io.qdrant.client.grpc.Points.UpsertPoints;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Client for interacting with the Qdrant GRPC API. */
public class QdrantGrpc implements Serializable {

  private final QdrantClient client;

  public QdrantGrpc(URL url, String apiKey) {
    if (url == null) {
      throw new IllegalArgumentException("URL cannot be null");
    }
    String host = url.getHost();
    if (host == null || host.isEmpty()) {
      throw new IllegalArgumentException("Invalid URL: host is missing. Provided URL: " + url);
    }
    int port = url.getPort() == -1 ? 6334 : url.getPort();
    boolean useTls = url.getProtocol().equalsIgnoreCase("https");
    client =
        new QdrantClient(
            QdrantGrpcClient.newBuilder(host, port, useTls).withApiKey(apiKey).build());
  }

  public void upsert(
      String collectionName,
      List<PointStruct> points,
      ShardKeySelector shardKeySelector,
      boolean wait)
      throws InterruptedException, ExecutionException {
    UpsertPoints.Builder upsertPoints =
        UpsertPoints.newBuilder()
            .setCollectionName(collectionName)
            .setWait(wait)
            .addAllPoints(points);
    if (shardKeySelector != null) {
      upsertPoints.setShardKeySelector(shardKeySelector);
    }
    client.upsertAsync(upsertPoints.build()).get();
  }

  public void close() {
    client.close();
  }
}
