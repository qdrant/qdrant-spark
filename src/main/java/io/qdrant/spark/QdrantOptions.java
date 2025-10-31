package io.qdrant.spark;

import static io.qdrant.client.ShardKeyFactory.shardKey;

import io.qdrant.client.grpc.Points.ShardKeySelector;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class QdrantOptions implements Serializable {
  private static final int DEFAULT_BATCH_SIZE = 64;
  private static final int DEFAULT_RETRIES = 3;
  private static final boolean DEFAULT_WAIT = true;

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
  public final String[] multiVectorFields;
  public final String[] multiVectorNames;
  public final List<String> payloadFieldsToSkip;
  public final ShardKeySelector shardKeySelector;
  public final boolean wait;

  public QdrantOptions(Map<String, String> options) {
    Objects.requireNonNull(options);

    qdrantUrl = options.get("qdrant_url");
    if (qdrantUrl == null || qdrantUrl.isEmpty()) {
      throw new IllegalArgumentException("qdrant_url option is required and cannot be empty");
    }

    collectionName = options.get("collection_name");
    if (collectionName == null || collectionName.isEmpty()) {
      throw new IllegalArgumentException("collection_name option is required and cannot be empty");
    }

    batchSize = getIntOption(options, "batch_size", DEFAULT_BATCH_SIZE);
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batch_size must be positive, got: " + batchSize);
    }

    retries = getIntOption(options, "retries", DEFAULT_RETRIES);
    if (retries < 0) {
      throw new IllegalArgumentException("retries cannot be negative, got: " + retries);
    }

    idField = options.getOrDefault("id_field", "");
    apiKey = options.getOrDefault("api_key", "");
    embeddingField = options.getOrDefault("embedding_field", "");
    vectorName = options.getOrDefault("vector_name", "");
    wait = getBooleanOption(options, "wait", DEFAULT_WAIT);

    sparseVectorValueFields = parseArray(options.get("sparse_vector_value_fields"));
    sparseVectorIndexFields = parseArray(options.get("sparse_vector_index_fields"));
    sparseVectorNames = parseArray(options.get("sparse_vector_names"));
    vectorFields = parseArray(options.get("vector_fields"));
    vectorNames = parseArray(options.get("vector_names"));
    multiVectorFields = parseArray(options.get("multi_vector_fields"));
    multiVectorNames = parseArray(options.get("multi_vector_names"));

    shardKeySelector = parseShardKeys(options.get("shard_key_selector"));

    validateSparseVectorFields();
    validateVectorFields();
    validateMultiVectorFields();

    payloadFieldsToSkip = buildPayloadFieldsToSkip();
  }

  private int getIntOption(Map<String, String> options, String key, int defaultValue) {
    String value = options.getOrDefault(key, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid value for option '" + key + "': '" + value + "'. Expected an integer.", e);
    }
  }

  private boolean getBooleanOption(Map<String, String> options, String key, boolean defaultValue) {
    return Boolean.parseBoolean(options.getOrDefault(key, String.valueOf(defaultValue)));
  }

  private String[] parseArray(String input) {
    if (input == null || input.trim().isEmpty()) {
      return new String[0];
    }
    return Arrays.stream(input.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);
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

  private void validateMultiVectorFields() {
    if (multiVectorFields.length != multiVectorNames.length) {
      throw new IllegalArgumentException(
          "Multi vector fields and names should have the same length");
    }
  }

  private ShardKeySelector parseShardKeys(@Nullable String shardKeys) {
    if (shardKeys == null) {
      return null;
    }

    return ShardKeySelector.newBuilder()
        .addAllShardKeys(
            Arrays.stream(shardKeys.split(","))
                .map(String::trim)
                .map(
                    (key) -> {
                      if (isInt(key)) {
                        return shardKey(Integer.parseInt(key));
                      } else {
                        return shardKey(key);
                      }
                    })
                .collect(Collectors.toList()))
        .build();
  }

  boolean isInt(String s) {
    try {
      Integer.parseInt(s);
      return true;
    } catch (NumberFormatException er) {
      return false;
    }
  }

  private List<String> buildPayloadFieldsToSkip() {
    List<String> fields = new ArrayList<>();
    fields.add(idField);
    fields.add(embeddingField);
    fields.addAll(Arrays.asList(sparseVectorValueFields));
    fields.addAll(Arrays.asList(sparseVectorIndexFields));
    fields.addAll(Arrays.asList(sparseVectorNames));
    fields.addAll(Arrays.asList(vectorFields));
    fields.addAll(Arrays.asList(vectorNames));
    fields.addAll(Arrays.asList(multiVectorFields));
    fields.addAll(Arrays.asList(multiVectorNames));
    return fields;
  }
}
