package io.qdrant.spark;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.ShardKeySelector;
import io.qdrant.client.grpc.Points.UpsertPoints;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/** A class that provides methods to interact with Qdrant GRPC API. */
public class QdrantGrpc implements Serializable {
  private final QdrantClient client;

  /**
   * Constructor for QdrantRest class.
   *
   * @param url The URL of the Qdrant instance.
   * @param apiKey The API key to authenticate with Qdrant.
   * @throws MalformedURLException If the URL is invalid.
   */
  public QdrantGrpc(URL url, String apiKey) throws MalformedURLException {

    String host = url.getHost();
    int port = url.getPort() == -1 ? 6334 : url.getPort();
    boolean useTls = url.getProtocol().equalsIgnoreCase("https");

    this.client =
        new QdrantClient(
            QdrantGrpcClient.newBuilder(host, port, useTls).withApiKey(apiKey).build());
  }

  /**
   * Uploads a batch of points to a Qdrant collection.
   *
   * @param collectionName The name of the collection to upload the points to.
   * @param points The list of points to upload.
   * @param shardKeySelector The shard key selector to use for the upsert.
   * @throws InterruptedException If there was an error uploading the batch to Qdrant.
   * @throws ExecutionException If there was an error uploading the batch to Qdrant.
   */
  public void upsert(
      String collectionName, List<PointStruct> points, @Nullable ShardKeySelector shardKeySelector)
      throws InterruptedException, ExecutionException {

    UpsertPoints.Builder upsertPoints =
        UpsertPoints.newBuilder().setCollectionName(collectionName).addAllPoints(points);

    if (shardKeySelector != null) {
      upsertPoints.setShardKeySelector(shardKeySelector);
    }

    this.client.upsertAsync(upsertPoints.build()).get();
  }

  public void close() {
    this.client.close();
  }
}
