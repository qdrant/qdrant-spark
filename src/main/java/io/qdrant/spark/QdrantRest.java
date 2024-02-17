package io.qdrant.spark;

import com.google.gson.Gson;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/** A class that provides methods to interact with Qdrant REST API. */
public class QdrantRest implements Serializable {
  private final String qdrantUrl;
  private final String apiKey;

  /**
   * Constructor for QdrantRest class.
   *
   * @param qdrantUrl The URL of the Qdrant instance.
   * @param apiKey The API key to authenticate with Qdrant.
   */
  public QdrantRest(String qdrantUrl, String apiKey) {
    this.qdrantUrl = qdrantUrl;
    this.apiKey = (apiKey != null) ? apiKey : "";
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
  public void uploadBatch(String collectionName, List<Point> points)
      throws IOException, MalformedURLException, RuntimeException {
    Gson gson = new Gson();
    OkHttpClient client = new OkHttpClient();
    RequestData requestBody = new RequestData(points);
    URL url = new URL(new URL(qdrantUrl), "/collections/" + collectionName + "/points");
    String json = gson.toJson(requestBody);
    MediaType mediaType = MediaType.get("application/json");
    RequestBody body = RequestBody.create(mediaType, json);

    Request request = new Request.Builder().url(url).header("api-key", apiKey).put(body).build();
    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new RuntimeException(response.body().string());
      }
    } catch (IOException e) {
      throw e;
    }
  }
}

class RequestData {
  public List<Point> points;

  public RequestData(List<Point> points) {
    this.points = points;
  }
}
