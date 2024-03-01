package io.qdrant.spark;

import static io.qdrant.client.VectorFactory.vector;
import static io.qdrant.client.VectorsFactory.namedVectors;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

import io.qdrant.client.grpc.Points.Vector;
import io.qdrant.client.grpc.Points.Vectors;

public class QdrantVectorHandler {
    static Vectors prepareVectors(InternalRow record, StructType schema, QdrantOptions options) {

        Vectors.Builder vectorsBuilder = Vectors.newBuilder();
        Vectors sparseVectors = prepareSparseVectors(record, schema, options);
        Vectors denseVectors = prepareDenseVectors(record, schema, options);

        vectorsBuilder.mergeFrom(sparseVectors).mergeFrom(denseVectors);

        if (options.embeddingField.isEmpty()) {
            return vectorsBuilder.build();
        }

        int vectorFieldIndex = schema.fieldIndex(options.embeddingField.trim());
        float[] embeddings = record.getArray(vectorFieldIndex).toFloatArray();

        // The vector name defaults to ""
        return vectorsBuilder
                .mergeFrom(namedVectors(Collections.singletonMap(options.vectorName, vector(embeddings)))).build();

    }

    private static Vectors prepareSparseVectors(InternalRow record, StructType schema, QdrantOptions options) {
        Map<String, Vector> sparseVectors = new HashMap<>();

        for (int i = 0; i < options.sparseVectorNames.length; i++) {
            String sparseVectorName = options.sparseVectorNames[i];
            String sparseVectorValueField = options.sparseVectorValueFields[i];
            String sparseVectorIndexField = options.sparseVectorIndexFields[i];
            int sparseVectorValueFieldIndex = schema.fieldIndex(sparseVectorValueField.trim());
            int sparseVectorIndexFieldIndex = schema.fieldIndex(sparseVectorIndexField.trim());
            List<Float> sparseVectorValues = Floats.asList(record.getArray(sparseVectorValueFieldIndex).toFloatArray());
            List<Integer> sparseVectorIndices = Ints.asList(record.getArray(sparseVectorIndexFieldIndex).toIntArray());

            sparseVectors.put(sparseVectorName, vector(sparseVectorValues, sparseVectorIndices));
        }

        return namedVectors(sparseVectors);
    }

    private static Vectors prepareDenseVectors(InternalRow record, StructType schema, QdrantOptions options) {
        Map<String, Vector> denseVectors = new HashMap<>();

        for (int i = 0; i < options.vectorNames.length; i++) {
            String vectorName = options.vectorNames[i];
            String vectorField = options.vectorFields[i];
            int vectorFieldIndex = schema.fieldIndex(vectorField.trim());
            float[] vectorValues = record.getArray(vectorFieldIndex).toFloatArray();

            denseVectors.put(vectorName, vector(vectorValues));
        }

        return namedVectors(denseVectors);
    }
}
