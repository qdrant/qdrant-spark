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
    collectionName = options.get("collection_name");
    batchSize = getIntOption(options, "batch_size", DEFAULT_BATCH_SIZE);
    retries = getIntOption(options, "retries", DEFAULT_RETRIES);
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

    payloadFieldsToSkip = buildPayloadFieldsToSkip();
  }

  private int getIntOption(Map<String, String> options, String key, int defaultValue) {
    return Integer.parseInt(options.getOrDefault(key, String.valueOf(defaultValue)));
  }

  private boolean getBooleanOption(Map<String, String> options, String key, boolean defaultValue) {
    return Boolean.parseBoolean(options.getOrDefault(key, String.valueOf(defaultValue)));
  }

  private String[] parseArray(String input) {
    return input == null
        ? new String[0]
        : Arrays.stream(input.split(",")).map(String::trim).toArray(String[]::new);
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
