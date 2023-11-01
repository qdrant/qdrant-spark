package tech.qdrant.spark;

public class QdrantRest {
    private final String url;
    private final String apiKey;
    private final String collectionName;

    public QdrantRest(String url, String apiKey, String collectionName) {
        this.url = url;
        this.apiKey = apiKey;
        this.collectionName = collectionName;
    }
}
