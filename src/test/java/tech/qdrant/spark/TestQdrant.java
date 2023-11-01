package tech.qdrant.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class TestQdrant {
        @Test
        public void testSparkSession() {
                SparkSession spark = SparkSession
                                .builder()
                                .master("local[1]")
                                .appName("qdrant-spark")
                                .getOrCreate();

                List<Row> data = Arrays.asList(
                                RowFactory.create(1, "row1", 1),
                                RowFactory.create(2, "row2", 2));
                StructType schema = new StructType(new StructField[] {
                                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                                new StructField("score", DataTypes.IntegerType, true, Metadata.empty())
                });
                Dataset<Row> df = spark.createDataFrame(data, schema);

                df.write().format("tech.qdrant.spark.Qdrant").option("schema", df.schema().json())
                                .option("collection_name", 0)
                                .option("embedding_field", "embedding")
                                .option("qdrant_url", "http://localhost:6333")
                                .mode("append")
                                .save();
                ;
        }
}
