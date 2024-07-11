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
      int fieldIndex = schema.fieldIndex(options.embeddingField);
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
      int fieldIndex = schema.fieldIndex(options.sparseVectorValueFields[i]);
      StructField field = schema.fields()[fieldIndex];
      float[] values = extractFloatArray(record, fieldIndex, field.dataType());

      fieldIndex = schema.fieldIndex(options.sparseVectorIndexFields[i]);
      field = schema.fields()[fieldIndex];
      int[] indices = extractIntArray(record, fieldIndex, field.dataType());

      String name = options.sparseVectorNames[i];
      sparseVectors.put(name, vector(Floats.asList(values), Ints.asList(indices)));
    }

    return namedVectors(sparseVectors);
  }

  private static Vectors prepareDenseVectors(
      InternalRow record, StructType schema, QdrantOptions options) {
    Map<String, Vector> denseVectors = new HashMap<>();

    for (int i = 0; i < options.vectorNames.length; i++) {
      int fieldIndex = schema.fieldIndex(options.vectorFields[i]);
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
      int fieldIndex = schema.fieldIndex(options.multiVectorFields[i]);
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

    return record.getArray(fieldIndex).toFloatArray();
  }

  private static int[] extractIntArray(InternalRow record, int fieldIndex, DataType dataType) {

    if (!dataType.typeName().equalsIgnoreCase("array")) {
      throw new IllegalArgumentException("Vector field must be of type ArrayType");
    }

    ArrayType arrayType = (ArrayType) dataType;

    if (!arrayType.elementType().typeName().equalsIgnoreCase("integer")) {
      throw new IllegalArgumentException("Expected array elements to be of IntegerType");
    }

    return record.getArray(fieldIndex).toIntArray();
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

    ArrayData arrayData = record.getArray(fieldIndex);
    int numRows = arrayData.numElements();
    ArrayData firstRow = arrayData.getArray(0);
    int numCols = firstRow.numElements();

    float[][] multiVecArray = new float[numRows][numCols];
    for (int i = 0; i < numRows; i++) {
      multiVecArray[i] = arrayData.getArray(i).toFloatArray();
    }

    return multiVecArray;
  }
}
