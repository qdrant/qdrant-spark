package tech.qdrant.spark;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QdrantOptions implements Serializable {
    private final Logger LOG = LoggerFactory.getLogger(QdrantDataWriter.class);
    public String qdrantUrl;
    public String apiKey;
    public String collectionName;
    public String embeddingField;
    public String idField;
    public int batchSize = 100;
    public int retries = 3;

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

        LOG.info(
                "Qdrant options: URL {}, collection name {}, embedding field {}, id field {}, batch size {}, retries {}",
                this.qdrantUrl, this.collectionName, this.embeddingField, this.idField, this.batchSize, this.retries);
    }
}
