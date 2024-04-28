package io.qdrant.spark;

import static io.qdrant.client.VectorFactory.vector;
import static io.qdrant.client.VectorsFactory.namedVectors;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.qdrant.client.grpc.Points.Vector;
import io.qdrant.client.grpc.Points.Vectors;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public class QdrantVectorHandler {

  public static Vectors prepareVectors(
      InternalRow record, StructType schema, QdrantOptions options) {
    Vectors.Builder vectorsBuilder = Vectors.newBuilder();

    // Combine sparse and dense vectors
    vectorsBuilder.mergeFrom(prepareSparseVectors(record, schema, options));
    vectorsBuilder.mergeFrom(prepareDenseVectors(record, schema, options));

    // Maitaining support for the "embedding_field" and "vector_name" options
    if (!options.embeddingField.isEmpty()) {
      float[] embeddings = extractFloatArray(record, schema, options.embeddingField);
      // 'options.vectorName' defaults to ""
      vectorsBuilder.mergeFrom(
          namedVectors(Collections.singletonMap(options.vectorName, vector(embeddings))));
    }

    return vectorsBuilder.build();
  }

  private static Vectors prepareSparseVectors(
      InternalRow record, StructType schema, QdrantOptions options) {
    Map<String, Vector> sparseVectors = new HashMap<>();

    for (int i = 0; i < options.sparseVectorNames.length; i++) {
      String name = options.sparseVectorNames[i];
      float[] values = extractFloatArray(record, schema, options.sparseVectorValueFields[i]);
      int[] indices = extractIntArray(record, schema, options.sparseVectorIndexFields[i]);

      sparseVectors.put(name, vector(Floats.asList(values), Ints.asList(indices)));
    }

    return namedVectors(sparseVectors);
  }

  private static Vectors prepareDenseVectors(
      InternalRow record, StructType schema, QdrantOptions options) {
    Map<String, Vector> denseVectors = new HashMap<>();

    for (int i = 0; i < options.vectorNames.length; i++) {
      String name = options.vectorNames[i];
      float[] values = extractFloatArray(record, schema, options.vectorFields[i]);
      denseVectors.put(name, vector(values));
    }

    return namedVectors(denseVectors);
  }

  private static float[] extractFloatArray(
      InternalRow record, StructType schema, String fieldName) {
    int fieldIndex = schema.fieldIndex(fieldName);
    return record.getArray(fieldIndex).toFloatArray();
  }

  private static int[] extractIntArray(InternalRow record, StructType schema, String fieldName) {
    int fieldIndex = schema.fieldIndex(fieldName);
    return record.getArray(fieldIndex).toIntArray();
  }
}
