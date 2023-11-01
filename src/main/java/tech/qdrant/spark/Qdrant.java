package tech.qdrant.spark;

import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
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

        checkRequiredOptions(options);
        return (StructType) StructType.fromJson(options.get("schema"));
    };

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        QdrantOptions options = new QdrantOptions(properties);
        return new QdrantCluster(options, schema);
    }

    public void checkRequiredOptions(CaseInsensitiveStringMap options) {
        for (String fieldName : requiredFields) {
            if (!options.containsKey(fieldName)) {
                throw new IllegalArgumentException(fieldName + " option is required");
            }
        }
    }
}