package io.qdrant.spark;

import static io.qdrant.client.ShardKeyFactory.shardKey;
import static io.qdrant.client.ShardKeySelectorFactory.shardKeySelector;

import io.qdrant.client.grpc.Collections.ShardKey;
import io.qdrant.client.grpc.Points.ShardKeySelector;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

public class QdrantOptions implements Serializable {
  private static final int DEFAULT_BATCH_SIZE = 64;
  private static final int DEFAULT_RETRIES = 3;

  public final String qdrantUrl;
  public final String apiKey;
  public final String collectionName;
  public final String idField;
  public final int batchSize;
  public final int retries;
  public final String embeddingField;
  public final String vectorName;
  public final String[] sparseVectorValueFields;
  public final String[] sparseVectorIndexFields;
  public final String[] sparseVectorNames;
  public final String[] vectorFields;
  public final String[] vectorNames;
  public final List<String> payloadFieldsToSkip;
  public final ShardKeySelector shardKeySelector;

  public QdrantOptions(Map<String, String> options) {
    Objects.requireNonNull(options);

    qdrantUrl = options.get("qdrant_url");
    collectionName = options.get("collection_name");
    batchSize =
        Integer.parseInt(options.getOrDefault("batch_size", String.valueOf(DEFAULT_BATCH_SIZE)));
    retries = Integer.parseInt(options.getOrDefault("retries", String.valueOf(DEFAULT_RETRIES)));
    idField = options.getOrDefault("id_field", "");
    apiKey = options.getOrDefault("api_key", "");
    embeddingField = options.getOrDefault("embedding_field", "");
    vectorName = options.getOrDefault("vector_name", "");

    sparseVectorValueFields = parseArray(options.get("sparse_vector_value_fields"));
    sparseVectorIndexFields = parseArray(options.get("sparse_vector_index_fields"));
    sparseVectorNames = parseArray(options.get("sparse_vector_names"));
    vectorFields = parseArray(options.get("vector_fields"));
    vectorNames = parseArray(options.get("vector_names"));

    shardKeySelector = parseShardKeys(options.get("shard_key_selector"));

    validateSparseVectorFields();
    validateVectorFields();

    payloadFieldsToSkip = new ArrayList<>();
    payloadFieldsToSkip.add(idField);
    payloadFieldsToSkip.add(embeddingField);
    payloadFieldsToSkip.addAll(Arrays.asList(sparseVectorValueFields));
    payloadFieldsToSkip.addAll(Arrays.asList(sparseVectorIndexFields));
    payloadFieldsToSkip.addAll(Arrays.asList(sparseVectorNames));
    payloadFieldsToSkip.addAll(Arrays.asList(vectorFields));
    payloadFieldsToSkip.addAll(Arrays.asList(vectorNames));
  }

  private String[] parseArray(String input) {
    if (input != null) {
      String[] parts = input.split(",");
      for (int i = 0; i < parts.length; i++) {
        parts[i] = parts[i].trim();
      }
      return parts;
    } else {
      return new String[0];
    }
  }

  private void validateSparseVectorFields() {
    if (sparseVectorValueFields.length != sparseVectorIndexFields.length
        || sparseVectorValueFields.length != sparseVectorNames.length) {
      throw new IllegalArgumentException(
          "Sparse vector value fields, index fields, and names should have the same length");
    }
  }

  private void validateVectorFields() {
    if (vectorFields.length != vectorNames.length) {
      throw new IllegalArgumentException("Vector fields and names should have the same length");
    }
  }

  private ShardKeySelector parseShardKeys(@Nullable String shardKeys) {
    if (shardKeys == null) {
      return null;
    }
    String[] keys = shardKeys.split(",");

    ShardKey[] shardKeysArray = new ShardKey[keys.length];

    for (int i = 0; i < keys.length; i++) {
      String key = keys[i];
      if (isInt(key.trim())) {
        shardKeysArray[i] = shardKey(Integer.parseInt(key.trim()));
      } else {
        shardKeysArray[i] = shardKey(key.trim());
      }
    }

    return shardKeySelector(shardKeysArray);
  }

  boolean isInt(String s) {
    try {
      Integer.parseInt(s);
      return true;
    } catch (NumberFormatException er) {
      return false;
    }
  }
}
