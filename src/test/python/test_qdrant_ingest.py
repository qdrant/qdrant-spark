import os
from pyspark.sql import SparkSession

from .schema import schema
from .conftest import Qdrant

current_directory = os.path.dirname(__file__)
input_file_path = os.path.join(current_directory, "..", "resources", "users.json")


def test_upsert_unnamed_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "embedding_field": "dense_vector",
        "api_key": qdrant.api_key,
        "schema": df.schema.json(),
    }

    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_named_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "vector_name": "dense",
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }

    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_multiple_named_dense_vectors(
    qdrant: Qdrant, spark_session: SparkSession
):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "vector_fields": "dense_vector,dense_vector",
        "vector_names": "dense,another_dense",
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }

    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_sparse_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "sparse_vector_value_fields": "sparse_values",
        "sparse_vector_index_fields": "sparse_indices",
        "sparse_vector_names": "sparse",
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }

    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_multiple_sparse_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "sparse_vector_value_fields": "sparse_values,sparse_values",
        "sparse_vector_index_fields": "sparse_indices,sparse_indices",
        "sparse_vector_names": "sparse,another_sparse",
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }

    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_sparse_named_dense_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "embedding_field": "dense_vector",
        "vector_name": "dense",
        "sparse_vector_value_fields": "sparse_values",
        "sparse_vector_index_fields": "sparse_indices",
        "sparse_vector_names": "sparse",
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }

    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_sparse_unnamed_dense_vectors(
    qdrant: Qdrant, spark_session: SparkSession
):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "embedding_field": "dense_vector",
        "sparse_vector_value_fields": "sparse_values",
        "sparse_vector_index_fields": "sparse_indices",
        "sparse_vector_names": "sparse",
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }

    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_multiple_sparse_dense_vectors(
    qdrant: Qdrant, spark_session: SparkSession
):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "embedding_field": "dense_vector",
        "vector_name": "dense",
        "sparse_vector_value_fields": "sparse_values,sparse_values",
        "sparse_vector_index_fields": "sparse_indices,sparse_indices",
        "sparse_vector_names": "sparse,another_sparse",
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }

    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


# Test an upsert without vectors. All the dataframe fields will be treated as payload
def test_upsert_without_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }
    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_custom_id_field(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )

    opts = {
        "qdrant_url": qdrant.url,
        "collection_name": qdrant.collection_name,
        "embedding_field": "dense_vector",
        "vector_name": "dense",
        "id_field": "id",
        "schema": df.schema.json(),
        "api_key": qdrant.api_key,
    }
    df.write.format("io.qdrant.spark.Qdrant").options(**opts).mode("append").save()

    assert len(qdrant.client.retrieve(qdrant.collection_name, [1, 2, 3, 15, 18])) == 5
