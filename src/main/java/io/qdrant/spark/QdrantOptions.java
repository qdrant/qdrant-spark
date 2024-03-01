package io.qdrant.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** This class represents the options for connecting to a Qdrant instance. */
public class QdrantOptions implements Serializable {
  public String qdrantUrl;
  public String apiKey;
  public String collectionName;
  public String idField;

  public int batchSize = 64;
  public int retries = 3;

  // Should've named the option 'vectorField'. But too late now.
  public String embeddingField;
  public String vectorName;

  public String[] sparseVectorValueFields;
  public String[] sparseVectorIndexFields;
  public String[] sparseVectorNames;

  public String[] vectorFields;
  public String[] vectorNames;

  public List<String> payloadFieldsToSkip = new ArrayList<String>();

  /**
   * Constructor for QdrantOptions.
   *
   * @param options A map of options for connecting to a Qdrant instance.
   */
  public QdrantOptions(Map<String, String> options) {
    qdrantUrl = options.get("qdrant_url");
    collectionName = options.get("collection_name");
    embeddingField = options.getOrDefault("embedding_field", "");
    idField = options.getOrDefault("id_field", "");
    apiKey = options.getOrDefault("api_key", "");
    vectorName = options.getOrDefault("vector_name", "");
    sparseVectorValueFields = options.getOrDefault("sparse_vector_value_fields", "").split(",");
    sparseVectorIndexFields = options.getOrDefault("sparse_vector_index_fields", "").split(",");
    sparseVectorNames = options.getOrDefault("sparse_vector_names", "").split(",");
    vectorFields = options.getOrDefault("vector_fields", "").split(",");
    vectorNames = options.getOrDefault("vector_names", "").split(",");

    if (sparseVectorValueFields.length != sparseVectorIndexFields.length
        || sparseVectorValueFields.length != sparseVectorNames.length) {
      throw new IllegalArgumentException(
          "Sparse vector value fields, index fields and names should be of same length");
    }

    if (vectorFields.length != vectorNames.length) {
      throw new IllegalArgumentException("Vector fields and names should be of same length");
    }

    if (options.containsKey("batch_size")) {
      batchSize = Integer.parseInt(options.get("batch_size"));
    }

    if (options.containsKey("retries")) {
      retries = Integer.parseInt(options.get("retries"));
    }

    payloadFieldsToSkip.add(idField);
    payloadFieldsToSkip.add(embeddingField);

    payloadFieldsToSkip.addAll(Arrays.asList(sparseVectorValueFields));
    payloadFieldsToSkip.addAll(Arrays.asList(sparseVectorIndexFields));
    payloadFieldsToSkip.addAll(Arrays.asList(sparseVectorNames));

    payloadFieldsToSkip.addAll(Arrays.asList(vectorFields));
    payloadFieldsToSkip.addAll(Arrays.asList(vectorNames));

  }
}
