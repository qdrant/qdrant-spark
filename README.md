# Qdrant-Spark Connector

[Apache Spark](https://spark.apache.org/) is a distributed computing framework designed for big data processing and analytics. This connector enables [Qdrant](https://qdrant.tech/) to be a storage destination in Spark.

## Installation

To integrate the connector into your Spark environment, get the JAR file from one of the sources listed below.

> [!IMPORTANT]  
> Ensure your system is running Java 8.

### GitHub Releases

The packaged `jar` file can be found [here](https://github.com/qdrant/qdrant-spark/releases).

### Building from source

To build the `jar` from source, you need [JDK@8](https://www.azul.com/downloads/#zulu) and [Maven](https://maven.apache.org/) installed.
Once the requirements have been satisfied, run the following command in the project root.

```bash
mvn package
```

The JAR file will be written into the `target` directory by default.

### Maven Central

Find the project on Maven Central [here](https://central.sonatype.com/artifact/io.qdrant/spark).

## Usage

### Creating a Spark session (Single-node) with Qdrant support

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(
        "spark.jars",
        "spark-VERSION.jar",  # Specify the downloaded JAR file
    )
    .master("local[*]")
    .appName("qdrant")
    .getOrCreate()
```

### Loading data

> [!IMPORTANT]
> Before loading the data using this connector, a collection has to be [created](https://qdrant.tech/documentation/concepts/collections/#create-a-collection) in advance with the appropriate vector dimensions and configurations.

The connector supports ingesting multiple named/unnamed, dense/sparse vectors.

_Click each to expand._

<details>
  <summary><b>Unnamed/Default vector</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", <QDRANT_GRPC_URL>)
   .option("collection_name", <QDRANT_COLLECTION_NAME>)
   .option("embedding_field", <EMBEDDING_FIELD_NAME>)  # Expected to be a field of type ArrayType(FloatType)
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

</details>

<details>
  <summary><b>Named vector</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", <QDRANT_GRPC_URL>)
   .option("collection_name", <QDRANT_COLLECTION_NAME>)
   .option("embedding_field", <EMBEDDING_FIELD_NAME>)  # Expected to be a field of type ArrayType(FloatType)
   .option("vector_name", <VECTOR_NAME>)
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

> #### NOTE
>
> The `embedding_field` and `vector_name` options are maintained for backward compatibility. It is recommended to use `vector_fields` and `vector_names` for named vectors as shown below.

</details>

<details>
  <summary><b>Multiple named vectors</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", "<QDRANT_GRPC_URL>")
   .option("collection_name", "<QDRANT_COLLECTION_NAME>")
   .option("vector_fields", "<COLUMN_NAME>,<ANOTHER_COLUMN_NAME>")
   .option("vector_names", "<VECTOR_NAME>,<ANOTHER_VECTOR_NAME>")
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

</details>

<details>
  <summary><b>Sparse vectors</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", "<QDRANT_GRPC_URL>")
   .option("collection_name", "<QDRANT_COLLECTION_NAME>")
   .option("sparse_vector_value_fields", "<COLUMN_NAME>")
   .option("sparse_vector_index_fields", "<COLUMN_NAME>")
   .option("sparse_vector_names", "<SPARSE_VECTOR_NAME>")
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

</details>

<details>
  <summary><b>Multiple sparse vectors</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", "<QDRANT_GRPC_URL>")
   .option("collection_name", "<QDRANT_COLLECTION_NAME>")
   .option("sparse_vector_value_fields", "<COLUMN_NAME>,<ANOTHER_COLUMN_NAME>")
   .option("sparse_vector_index_fields", "<COLUMN_NAME>,<ANOTHER_COLUMN_NAME>")
   .option("sparse_vector_names", "<SPARSE_VECTOR_NAME>,<ANOTHER_SPARSE_VECTOR_NAME>")
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

</details>

<details>
  <summary><b>Combination of named dense and sparse vectors</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", "<QDRANT_GRPC_URL>")
   .option("collection_name", "<QDRANT_COLLECTION_NAME>")
   .option("vector_fields", "<COLUMN_NAME>,<ANOTHER_COLUMN_NAME>")
   .option("vector_names", "<VECTOR_NAME>,<ANOTHER_VECTOR_NAME>")
   .option("sparse_vector_value_fields", "<COLUMN_NAME>,<ANOTHER_COLUMN_NAME>")
   .option("sparse_vector_index_fields", "<COLUMN_NAME>,<ANOTHER_COLUMN_NAME>")
   .option("sparse_vector_names", "<SPARSE_VECTOR_NAME>,<ANOTHER_SPARSE_VECTOR_NAME>")
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

</details>

<details>
  <summary><b>Multi-vectors</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", "<QDRANT_GRPC_URL>")
   .option("collection_name", "<QDRANT_COLLECTION_NAME>")
   .option("multi_vector_fields", "<COLUMN_NAME>")
   .option("multi_vector_names", "<MULTI_VECTOR_NAME>")
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

</details>

<details>
  <summary><b>Multiple Multi-vectors</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", "<QDRANT_GRPC_URL>")
   .option("collection_name", "<QDRANT_COLLECTION_NAME>")
   .option("multi_vector_fields", "<COLUMN_NAME>,<ANOTHER_COLUMN_NAME>")
   .option("multi_vector_names", "<MULTI_VECTOR_NAME>,<ANOTHER_MULTI_VECTOR_NAME>")
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

</details>

<details>
  <summary><b>No vectors - Entire dataframe is stored as payload</b></summary>

```python
  <pyspark.sql.DataFrame>
   .write
   .format("io.qdrant.spark.Qdrant")
   .option("qdrant_url", "<QDRANT_GRPC_URL>")
   .option("collection_name", "<QDRANT_COLLECTION_NAME>")
   .option("schema", <pyspark.sql.DataFrame>.schema.json())
   .mode("append")
   .save()
```

</details>

## Databricks

> [!TIP]
> Check out our [example](https://qdrant.tech/documentation/examples/databricks/) of using the Spark connector with Databricks.

You can use the connector as a library in Databricks to ingest data into Qdrant.

- Go to the `Libraries` section in your cluster dashboard.
- Select `Install New` to open the library installation modal.
- Search for `io.qdrant:spark:VERSION` in the Maven packages and click `Install`.

<img width="704" alt="Screenshot 2024-04-28 at 11 34 17 AM" src="https://github.com/qdrant/qdrant-spark/assets/46051506/0c1bd356-3fba-436a-90ce-d8ff39b02d1f">

## Datatype support

The appropriate Spark data types are mapped to the Qdrant payload based on the provided `schema`.

## Options and Spark types

| Option                       | Description                                                                          | Column DataType                   | Required |
| :--------------------------- | :----------------------------------------------------------------------------------- | :-------------------------------- | :------- |
| `qdrant_url`                 | gRPC URL of the Qdrant instance. Eg: <http://localhost:6334>                         | -                                 | ✅       |
| `collection_name`            | Name of the collection to write data into                                            | -                                 | ✅       |
| `schema`                     | JSON string of the dataframe schema                                                  | -                                 | ✅       |
| `embedding_field`            | Name of the column holding the embeddings (Deprecated - Use `vector_fields` instead) | `ArrayType(FloatType)`            | ❌       |
| `id_field`                   | Name of the column holding the point IDs. Default: Random UUID                       | `StringType` or `IntegerType`     | ❌       |
| `batch_size`                 | Max size of the upload batch. Default: 64                                            | -                                 | ❌       |
| `retries`                    | Number of upload retries. Default: 3                                                 | -                                 | ❌       |
| `api_key`                    | Qdrant API key for authentication                                                    | -                                 | ❌       |
| `vector_name`                | Name of the vector in the collection.                                                | -                                 | ❌       |
| `vector_fields`              | Comma-separated names of columns holding the vectors.                                | `ArrayType(FloatType)`            | ❌       |
| `vector_names`               | Comma-separated names of vectors in the collection.                                  | -                                 | ❌       |
| `sparse_vector_index_fields` | Comma-separated names of columns holding the sparse vector indices.                  | `ArrayType(IntegerType)`          | ❌       |
| `sparse_vector_value_fields` | Comma-separated names of columns holding the sparse vector values.                   | `ArrayType(FloatType)`            | ❌       |
| `sparse_vector_names`        | Comma-separated names of the sparse vectors in the collection.                       | -                                 | ❌       |
| `multi_vector_fields`        | Comma-separated names of columns holding the multi-vector values.                    | `ArrayType(ArrayType(FloatType))` | ❌       |
| `multi_vector_names`         | Comma-separated names of the multi-vectors in the collection.                        | -                                 | ❌       |
| `shard_key_selector`         | Comma-separated names of custom shard keys to use during upsert.                     | -                                 | ❌       |
| `wait`                       | Wait for each batch upsert to complete. `true` or `false`. Defaults to `true`.       | -                                 | ❌       |

## LICENSE

Apache 2.0 © [2024](https://github.com/qdrant/qdrant-spark/blob/master/LICENSE)
