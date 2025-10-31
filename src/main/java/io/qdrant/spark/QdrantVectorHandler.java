package io.qdrant.spark;

import static io.qdrant.client.VectorFactory.multiVector;
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
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class QdrantVectorHandler {

  public static Vectors prepareVectors(
      InternalRow record, StructType schema, QdrantOptions options) {
    Vectors.Builder vectorsBuilder = Vectors.newBuilder();
    // Combine sparse, dense and multi vectors
    vectorsBuilder.mergeFrom(prepareSparseVectors(record, schema, options));
    vectorsBuilder.mergeFrom(prepareDenseVectors(record, schema, options));
    vectorsBuilder.mergeFrom(prepareMultiVectors(record, schema, options));

    // Maitaining support for the "embedding_field" and "vector_name" options
    if (!options.embeddingField.isEmpty()) {
      int fieldIndex;
      try {
        fieldIndex = schema.fieldIndex(options.embeddingField);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Embedding field '"
                + options.embeddingField
                + "' does not exist in the schema. "
                + "Available fields: "
                + String.join(", ", schema.fieldNames()),
            e);
      }
      StructField field = schema.fields()[fieldIndex];
      float[] embeddings = extractFloatArray(record, fieldIndex, field.dataType());
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
      int fieldIndex;
      try {
        fieldIndex = schema.fieldIndex(options.sparseVectorValueFields[i]);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Sparse vector value field '"
                + options.sparseVectorValueFields[i]
                + "' does not exist in the schema. "
                + "Available fields: "
                + String.join(", ", schema.fieldNames()),
            e);
      }
      StructField field = schema.fields()[fieldIndex];
      float[] values = extractFloatArray(record, fieldIndex, field.dataType());

      try {
        fieldIndex = schema.fieldIndex(options.sparseVectorIndexFields[i]);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Sparse vector index field '"
                + options.sparseVectorIndexFields[i]
                + "' does not exist in the schema. "
                + "Available fields: "
                + String.join(", ", schema.fieldNames()),
            e);
      }
      field = schema.fields()[fieldIndex];
      int[] indices = extractIntArray(record, fieldIndex, field.dataType());

      if (values.length != indices.length) {
        throw new IllegalArgumentException(
            "Sparse vector '"
                + options.sparseVectorNames[i]
                + "' has mismatched dimensions: "
                + "values array has "
                + values.length
                + " elements but indices array has "
                + indices.length
                + " elements. They must be equal.");
      }

      String name = options.sparseVectorNames[i];
      sparseVectors.put(name, vector(Floats.asList(values), Ints.asList(indices)));
    }

    return namedVectors(sparseVectors);
  }

  private static Vectors prepareDenseVectors(
      InternalRow record, StructType schema, QdrantOptions options) {
    Map<String, Vector> denseVectors = new HashMap<>();

    for (int i = 0; i < options.vectorNames.length; i++) {
      int fieldIndex;
      try {
        fieldIndex = schema.fieldIndex(options.vectorFields[i]);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Vector field '"
                + options.vectorFields[i]
                + "' does not exist in the schema. "
                + "Available fields: "
                + String.join(", ", schema.fieldNames()),
            e);
      }
      StructField field = schema.fields()[fieldIndex];
      float[] values = extractFloatArray(record, fieldIndex, field.dataType());

      String name = options.vectorNames[i];
      denseVectors.put(name, vector(values));
    }

    return namedVectors(denseVectors);
  }

  private static Vectors prepareMultiVectors(
      InternalRow record, StructType schema, QdrantOptions options) {
    Map<String, Vector> multiVectors = new HashMap<>();

    for (int i = 0; i < options.multiVectorNames.length; i++) {
      int fieldIndex;
      try {
        fieldIndex = schema.fieldIndex(options.multiVectorFields[i]);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Multi-vector field '"
                + options.multiVectorFields[i]
                + "' does not exist in the schema. "
                + "Available fields: "
                + String.join(", ", schema.fieldNames()),
            e);
      }
      StructField field = schema.fields()[fieldIndex];
      float[][] vectors = extractMultiVecArray(record, fieldIndex, field.dataType());

      String name = options.multiVectorNames[i];
      multiVectors.put(name, multiVector(vectors));
    }

    return namedVectors(multiVectors);
  }

  private static float[] extractFloatArray(InternalRow record, int fieldIndex, DataType dataType) {

    if (!dataType.typeName().equalsIgnoreCase("array")) {
      throw new IllegalArgumentException("Vector field must be of type ArrayType");
    }

    ArrayType arrayType = (ArrayType) dataType;

    if (!arrayType.elementType().typeName().equalsIgnoreCase("float")) {
      throw new IllegalArgumentException("Expected array elements to be of FloatType");
    }

    if (record.isNullAt(fieldIndex)) {
      throw new IllegalArgumentException(
          "Vector field at index "
              + fieldIndex
              + " is null. "
              + "Vector fields cannot be null. Please ensure all vector fields have valid values.");
    }

    float[] array = record.getArray(fieldIndex).toFloatArray();
    if (array.length == 0) {
      throw new IllegalArgumentException(
          "Vector field at index "
              + fieldIndex
              + " is empty. "
              + "Vectors must have at least one dimension.");
    }
    return array;
  }

  private static int[] extractIntArray(InternalRow record, int fieldIndex, DataType dataType) {

    if (!dataType.typeName().equalsIgnoreCase("array")) {
      throw new IllegalArgumentException("Vector field must be of type ArrayType");
    }

    ArrayType arrayType = (ArrayType) dataType;

    if (!arrayType.elementType().typeName().equalsIgnoreCase("integer")) {
      throw new IllegalArgumentException("Expected array elements to be of IntegerType");
    }

    if (record.isNullAt(fieldIndex)) {
      throw new IllegalArgumentException(
          "Vector index field at index "
              + fieldIndex
              + " is null. "
              + "Vector index fields cannot be null. Please ensure all index fields have valid"
              + " values.");
    }

    int[] array = record.getArray(fieldIndex).toIntArray();
    if (array.length == 0) {
      throw new IllegalArgumentException(
          "Vector index field at index "
              + fieldIndex
              + " is empty. "
              + "Index arrays must have at least one element.");
    }
    return array;
  }

  private static float[][] extractMultiVecArray(
      InternalRow record, int fieldIndex, DataType dataType) {

    if (!dataType.typeName().equalsIgnoreCase("array")) {
      throw new IllegalArgumentException("Multi Vector field must be of type ArrayType");
    }

    ArrayType arrayType = (ArrayType) dataType;

    if (!arrayType.elementType().typeName().equalsIgnoreCase("array")) {
      throw new IllegalArgumentException("Multi Vector elements must be of type ArrayType");
    }

    if (record.isNullAt(fieldIndex)) {
      throw new IllegalArgumentException(
          "Multi-vector field at index "
              + fieldIndex
              + " is null. "
              + "Multi-vector fields cannot be null. Please ensure all multi-vector fields have"
              + " valid values.");
    }

    ArrayData arrayData = record.getArray(fieldIndex);
    int numRows = arrayData.numElements();

    if (numRows == 0) {
      return new float[0][0];
    }

    float[][] multiVecArray = new float[numRows][];
    for (int i = 0; i < numRows; i++) {
      multiVecArray[i] = arrayData.getArray(i).toFloatArray();
    }

    int expectedCols = multiVecArray[0].length;
    for (int i = 1; i < numRows; i++) {
      if (multiVecArray[i].length != expectedCols) {
        throw new IllegalArgumentException(
            "Multi-vector rows must have uniform dimensions. Row 0 has "
                + expectedCols
                + " elements, but row "
                + i
                + " has "
                + multiVecArray[i].length
                + " elements.");
      }
    }

    return multiVecArray;
  }
}
