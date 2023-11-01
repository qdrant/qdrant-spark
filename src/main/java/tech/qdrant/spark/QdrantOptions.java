package tech.qdrant.spark;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.types.StructType;

public class QdrantOptions implements Serializable {
    private int DEFAULT_BATCH_SIZE = 100;
    private int DEFAULT_RETRIES = 2;
    private int DEFAULT_RETRIES_BACKOFF = 2;
    private int DEFAULT_TIMEOUT_SECONDS = 60;

    private String url;
    private String collectionName;
    private String embeddingField;
    private StructType schema;

    public QdrantOptions(Map<String, String> options) {

    }
}
