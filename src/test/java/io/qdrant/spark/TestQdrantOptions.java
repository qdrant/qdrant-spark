package io.qdrant.spark;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class TestQdrantOptions {

  @Test
  public void testQdrantOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("qdrant_url", "http://localhost:6334");
    options.put("api_key", "my-api-key");
    options.put("collection_name", "my-collection");
    options.put("embedding_field", "my-embedding-field");
    options.put("id_field", "my-id-field");

    QdrantOptions qdrantOptions = new QdrantOptions(options);

    assertEquals("http://localhost:6334", qdrantOptions.qdrantUrl);
    assertEquals("my-api-key", qdrantOptions.apiKey);
    assertEquals("my-collection", qdrantOptions.collectionName);
    assertEquals("my-embedding-field", qdrantOptions.embeddingField);
    assertEquals("my-id-field", qdrantOptions.idField);

    // Test default values
    assertEquals(qdrantOptions.batchSize, 64);
    assertEquals(qdrantOptions.retries, 3);
  }
}
