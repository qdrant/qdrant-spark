package io.qdrant.spark;

import com.google.gson.Gson;
import java.io.Serializable;
import java.util.List;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A class that provides methods to interact with Qdrant REST API. */
public class QdrantRest implements Serializable {
  private final String qdrantUrl;
  private final String apiKey;
  private final Logger LOG = LoggerFactory.getLogger(QdrantRest.class);

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
   * @throws Exception If there was an error uploading the batch to Qdrant.
   */
  public void uploadBatch(String collectionName, List<Point> points) throws Exception {
    Gson gson = new Gson();
    OkHttpClient client = new OkHttpClient();
    RequestData requestBody = new RequestData(points);
    String url = qdrantUrl + "/collections/" + collectionName + "/points";
    String json = gson.toJson(requestBody);
    MediaType mediaType = MediaType.get("application/json");
    RequestBody body = RequestBody.create(mediaType, json);

    Request request = new Request.Builder().url(url).header("api-key", apiKey).put(body).build();
    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        LOG.error("Error uploading batch to Qdrant {}", response.message());
        throw new Exception(response.message());
      }
    } catch (Exception e) {
      LOG.error("Error uploading batch to Qdrant", e.getMessage());
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
