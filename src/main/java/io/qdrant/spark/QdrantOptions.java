package io.qdrant.spark;

import java.io.Serializable;
import java.util.Map;

/**
 * This class represents the options for connecting to a Qdrant instance.
 */
public class QdrantOptions implements Serializable {
    public String qdrantUrl;
    public String apiKey;
    public String collectionName;
    public String embeddingField;
    public String idField;
    public int batchSize = 100;
    public int retries = 3;

    /**
     * Constructor for QdrantOptions.
     * 
     * @param options A map of options for connecting to a Qdrant instance.
     */
    public QdrantOptions(Map<String, String> options) {
        this.qdrantUrl = options.get("qdrant_url");
        this.collectionName = options.get("collection_name");
        this.embeddingField = options.get("embedding_field");
        this.idField = options.get("id_field");
        this.apiKey = options.get("api_key");

        if (options.containsKey("batch_size")) {
            this.batchSize = Integer.parseInt(options.get("batch_size"));
        }

        if (options.containsKey("retries")) {
            this.retries = Integer.parseInt(options.get("retries"));
        }

    }
}
