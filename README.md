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

```xml
<dependency>
    <groupId>io.qdrant</groupId>
    <artifactId>spark</artifactId>
    <version>2.0.1</version>
</dependency>
```

## Usage 📝

### Creating a Spark session (Single-node) with Qdrant support 🌟

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(
        "spark.jars",
        "spark-2.0.1.jar",  # specify the downloaded JAR file
    )
    .master("local[*]")
    .appName("qdrant")
    .getOrCreate()
```

### Loading data 📊

To load data into Qdrant, a collection has to be created beforehand with the appropriate vector dimensions and configurations.

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

- By default, UUIDs are generated for each row. If you need to use custom IDs, you can do so by setting the `id_field` option.
- An API key can be set using the `api_key` option to make authenticated requests.

## Databricks

You can use the `qdrant-spark` connector as a library in Databricks to ingest data into Qdrant.

- Go to the `Libraries` section in your cluster dashboard.
- Select `Install New` to open the library installation modal.
- Search for `io.qdrant:spark:2.0.1` in the Maven packages and click `Install`.

<img width="1064" alt="Screenshot 2024-01-05 at 17 20 01 (1)" src="https://github.com/qdrant/qdrant-spark/assets/46051506/d95773e0-c5c6-4ff2-bf50-8055bb08fd1b">

## Datatype support 📋

Qdrant supports all the Spark data types. The appropriate types are mapped based on the provided `schema`.

## Options and Spark types 🛠️

| Option            | Description                                                               | DataType               | Required |
| :---------------- | :------------------------------------------------------------------------ | :--------------------- | :------- |
| `qdrant_url`      | GRPC URL of the Qdrant instance. Eg: <http://localhost:6334>                                       | `StringType`           | ✅       |
| `collection_name` | Name of the collection to write data into                                 | `StringType`           | ✅       |
| `embedding_field` | Name of the field holding the embeddings                                  | `ArrayType(FloatType)` | ✅       |
| `schema`          | JSON string of the dataframe schema                                       | `StringType`           | ✅       |
| `id_field`        | Name of the field holding the point IDs. Default: Generates a random UUId | `StringType`           | ❌       |
| `batch_size`      | Max size of the upload batch. Default: 100                                | `IntType`              | ❌       |
| `retries`         | Number of upload retries. Default: 3                                      | `IntType`              | ❌       |
| `api_key`         | Qdrant API key to be sent in the header. Default: null                    | `StringType`           | ❌       |
| `vector_name`         | Name of the vector in the collection. Default: null                    | `StringType`           | ❌       |

## LICENSE 📜

Apache 2.0 © [2023](https://github.com/qdrant/qdrant-spark/blob/master/LICENSE)
