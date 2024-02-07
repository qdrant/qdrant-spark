package io.qdrant.spark;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Points.PointStruct;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/** A class that provides methods to interact with Qdrant REST API. */
public class QdrantGrpc implements Serializable {
  private final QdrantClient client;

  /**
   * Constructor for QdrantRest class.
   *
   * @param qdrantUrl The URL of the Qdrant instance.
   * @param apiKey The API key to authenticate with Qdrant.
   */
  public QdrantGrpc(URL url, @Nullable String apiKey) throws MalformedURLException {

    String host = url.getHost();
    int port = url.getPort() == -1 ? 6334 : url.getPort();
    boolean useTls = url.getProtocol().equalsIgnoreCase("https");

    QdrantGrpcClient.Builder qdrantGrpcClientBuilder =
        QdrantGrpcClient.newBuilder(host, port, useTls);

    if (apiKey != null) {
      qdrantGrpcClientBuilder.withApiKey(apiKey);
    }

    this.client = new QdrantClient(qdrantGrpcClientBuilder.build());
  }

  /**
   * Uploads a batch of points to a Qdrant collection.
   *
   * @param collectionName The name of the collection to upload the points to.
   * @param points The list of points to upload.
   * @throws IOException If there was an error uploading the batch to Qdrant.
   * @throws RuntimeException If there was an error uploading the batch to Qdrant.
   * @throws MalformedURLException If the Qdrant URL is malformed.
   */
  public void upsert(String collectionName, List<PointStruct> points)
      throws InterruptedException, ExecutionException {
    this.client.upsertAsync(collectionName, points).get();
    return;
  }

  public void close() {
    this.client.close();
  }
}
