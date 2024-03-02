# Qdrant-Spark Connector

[Apache Spark](https://spark.apache.org/) is a distributed computing framework designed for big data processing and analytics. This connector enables [Qdrant](https://qdrant.tech/) to be a storage destination in Spark.

## Installation 🚀

> [!IMPORTANT]  
> Requires Java 8 or above.

### GitHub Releases 📦

The packaged `jar` file can be found [here](https://github.com/qdrant/qdrant-spark/releases).

### Building from source 🛠️

To build the `jar` from source, you need [JDK@8](https://www.azul.com/downloads/#zulu) and [Maven](https://maven.apache.org/) installed.
Once the requirements have been satisfied, run the following command in the project root. 🛠️

```bash
mvn package
```

This will build and store the fat JAR in the `target` directory by default.

### Maven Central 📚

For use with Java and Scala projects, the package can be found [here](https://central.sonatype.com/artifact/io.qdrant/spark).

## Usage 📝

### Creating a Spark session (Single-node) with Qdrant support

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(
        "spark.jars",
        "spark-2.1.0.jar",  # specify the downloaded JAR file
    )
    .master("local[*]")
    .appName("qdrant")
    .getOrCreate()
```

### Loading data 📊

> [!IMPORTANT]
> Before loading the data using this connector, a collection has to be [created](https://qdrant.tech/documentation/concepts/collections/#create-a-collection) in advance with the appropriate vector dimensions and configurations.

The connector supports ingesting multiple named/unnamed, dense/sparse vectors.

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

You can use the connector as a library in Databricks to ingest data into Qdrant.

- Go to the `Libraries` section in your cluster dashboard.
- Select `Install New` to open the library installation modal.
- Search for `io.qdrant:spark:2.1.0` in the Maven packages and click `Install`.

<img width="1064" alt="Screenshot 2024-01-05 at 17 20 01 (1)" src="https://github.com/qdrant/qdrant-spark/assets/46051506/d95773e0-c5c6-4ff2-bf50-8055bb08fd1b">

## Datatype support 📋

Qdrant supports all the Spark data types. The appropriate types are mapped based on the provided `schema`.

## Options and Spark types 🛠️

| Option                       | Description                                                         | Column DataType               | Required |
| :--------------------------- | :------------------------------------------------------------------ | :---------------------------- | :------- |
| `qdrant_url`                 | GRPC URL of the Qdrant instance. Eg: <http://localhost:6334>        | -                             | ✅       |
| `collection_name`            | Name of the collection to write data into                           | -                             | ✅       |
| `schema`                     | JSON string of the dataframe schema                                 | -                             | ✅       |
| `embedding_field`            | Name of the column holding the embeddings                           | `ArrayType(FloatType)`        | ❌       |
| `id_field`                   | Name of the column holding the point IDs. Default: Random UUID      | `StringType` or `IntegerType` | ❌       |
| `batch_size`                 | Max size of the upload batch. Default: 64                           | -                             | ❌       |
| `retries`                    | Number of upload retries. Default: 3                                | -                             | ❌       |
| `api_key`                    | Qdrant API key for authentication                                   | -                             | ❌       |
| `vector_name`                | Name of the vector in the collection.                               | -                             | ❌       |
| `vector_fields`              | Comma-separated names of columns holding the vectors.               | `ArrayType(FloatType)`        | ❌       |
| `vector_names`               | Comma-separated names of vectors in the collection.                 | -                             | ❌       |
| `sparse_vector_index_fields` | Comma-separated names of columns holding the sparse vector indices. | `ArrayType(IntegerType)`      | ❌       |
| `sparse_vector_value_fields` | Comma-separated names of columns holding the sparse vector values.  | `ArrayType(FloatType)`        | ❌       |
| `sparse_vector_names`        | Comma-separated names of the sparse vectors in the collection.      | -                             | ❌       |

## LICENSE 📜

Apache 2.0 © [2023](https://github.com/qdrant/qdrant-spark/blob/master/LICENSE)
