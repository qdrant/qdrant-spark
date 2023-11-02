# Qdrant-Spark Connector 💥

[Apache Spark](https://spark.apache.org/) is a distributed computing framework designed for big data processing and analytics. This connector enables [Qdrant](https://qdrant.tech/) to be a storage destination in Spark.

## Installation 🚀

### GitHub Releases 📦

The packaged `jar` file releases can be found [here](https://github.com/qdrant/qdrant-spark/releases).

### Building from source 🛠️

To build the `jar` from source, you need [JDK@17](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html) and [Maven](https://maven.apache.org/) installed.
Once the requirements have been satisfied, run the following command in the project root. 🛠️

```bash
mvn package -Passembly
```
This will build and store the fat JAR in the `target` directory by default.

### Maven Central 📚

The package will be available at the registry soon.

## Usage 📝

### Creating a Spark session (Single-node) with Qdrant support 🌟

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config(
        "spark.jars",
        "spark-1.0-SNAPSHOT-jar-with-dependencies.jar",  # specify the downloaded JAR file
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
    .option("qdrant_url", <QDRANT_URL>)
    .option("collection_name", <QDRANT_COLLECTION_NAME>)
    .option("embedding_field", <EMBEDDING_FIELD_NAME>)  # Expected to be a field of type ArrayType(FloatType)
    .option("schema", <pyspark.sql.DataFrame>.schema.json())
    .mode("append")
    .save()
```

* By default, UUIDs are generated for each row. If you need to use custom IDs, you can do so by setting the `id_field` option.
* An API key can be set using the `api_key` option to make authenticated requests.

## Datatype support 📋

Qdrant supports all the Spark data types, and the appropriate types are mapped based on the provided `schema`.

## Options 🛠️

| Option | Description     | Required      |
| :-------- | :------- | :------------|
| `qdrant_url` | `string` REST URL of the Qdrant instance | ✅ |
| `collection_name` | `string` Name of the collection to write data into | ✅ |
| `embedding_field` | `string` Name of the field holding the embeddings | ✅ |
| `id_field` | `string` Name of the field holding the point IDs | ✅ |
| `schema` | `string` JSON string of the dataframe schema | ✅ |
| `mode` | `string` Write mode of the dataframe | ✅ |
| `batch_size` | `int` Max size of the upload batch. Default: 100 | ❌ |
| `retries` | `string` Number of upload retries. Default: 3 | ❌ |
| `api_key` | `string` API key to be sent in the header. Default: null | ❌ |

## LICENSE 📜

Apache 2.0 © [2023](https://github.com/qdrant/qdrant-spark)
