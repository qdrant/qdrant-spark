package tech.qdrant.spark;

import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Qdrant implements TableProvider, DataSourceRegister {

    private final String[] requiredFields = new String[] {
            "schema",
            "collection_name",
            "embedding_field",
            "qdrant_url"
    };

    @Override
    public String shortName() {
        return "qdrant";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {

        StructType schema = (StructType) StructType.fromJson(options.get("schema"));
        checkRequiredOptions(options, schema);

        return schema;
    };

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        QdrantOptions options = new QdrantOptions(properties);
        return new QdrantCluster(options, schema);
    }

    private void checkRequiredOptions(CaseInsensitiveStringMap options, StructType schema) {
        for (String fieldName : requiredFields) {
            if (!options.containsKey(fieldName)) {
                throw new IllegalArgumentException(fieldName + " option is required");
            }
        }

        List<String> fieldNames = Arrays.asList(schema.fieldNames());

        if (options.containsKey("id_field")) {
            String idField = options.get("id_field").toString();

            if (!fieldNames.contains(idField)) {
                throw new IllegalArgumentException("id_field option is not present in the schema");
            }
        }

        String embeddingField = options.get("embedding_field").toString();

        if (!fieldNames.contains(embeddingField)) {
            throw new IllegalArgumentException("embedding_field option is not present in the schema");
        }
    }
}