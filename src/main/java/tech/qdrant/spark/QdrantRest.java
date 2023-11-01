package tech.qdrant.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import okhttp3.*;
import java.io.Serializable;
import java.util.List;

public class QdrantRest implements Serializable {
    private final String qdrantUrl;
    private final String apiKey;
    private final Logger LOG = LoggerFactory.getLogger(QdrantRest.class);

    public QdrantRest(String qdrantUrl, String apiKey) {
        this.qdrantUrl = qdrantUrl;
        this.apiKey = (apiKey != null) ? apiKey : "";
    }

    public void uploadBatch(String collectionName, List<Point> points) throws Exception {
        Gson gson = new Gson();
        OkHttpClient client = new OkHttpClient();
        RequestData requestBody = new RequestData(points);
        String url = qdrantUrl + "/collections/" + collectionName + "/points";
        String json = gson.toJson(requestBody);
        MediaType mediaType = MediaType.get("application/json");
        RequestBody body = RequestBody.create(json, mediaType);

        Request request = new Request.Builder()
                .url(url)
                .header("api-key", apiKey)
                .put(body)
                .build();
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
