import os
from pyspark.sql import SparkSession

from .schema import schema
from .conftest import Qdrant

current_directory = os.path.dirname(__file__)
input_file_path = os.path.join(current_directory, '..', 'Resources', 'users.json')


def test_upsert_unnamed_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "embedding_field", "dense_vector"
    ).mode("append").option("schema", df.schema.json()).save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_named_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "embedding_field", "dense_vector"
    ).option("vector_name", "dense").option("schema", df.schema.json()).mode(
        "append"
    ).save()

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
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "vector_fields", "dense_vector,dense_vector"
    ).option("vector_names", "dense,another_dense").option(
        "schema", df.schema.json()
    ).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_sparse_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "sparse_vector_value_fields", "sparse_values"
    ).option("sparse_vector_index_fields", "sparse_indices").option(
        "sparse_vector_names", "sparse"
    ).option("schema", df.schema.json()).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_multiple_sparse_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "sparse_vector_value_fields", "sparse_values,sparse_values"
    ).option("sparse_vector_index_fields", "sparse_indices,sparse_indices").option(
        "sparse_vector_names", "sparse,another_sparse"
    ).option("schema", df.schema.json()).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_upsert_sparse_named_dense_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "vector_fields", "dense_vector"
    ).option("vector_names", "dense").option(
        "sparse_vector_value_fields", "sparse_values"
    ).option("sparse_vector_index_fields", "sparse_indices").option(
        "sparse_vector_names", "sparse"
    ).option("schema", df.schema.json()).mode("append").save()

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
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "embedding_field", "dense_vector"
    ).option("sparse_vector_value_fields", "sparse_values").option(
        "sparse_vector_index_fields", "sparse_indices"
    ).option("sparse_vector_names", "sparse").option("schema", df.schema.json()).mode(
        "append"
    ).save()

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
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "vector_fields", "dense_vector,dense_vector"
    ).option("vector_names", "dense,another_dense").option(
        "sparse_vector_value_fields", "sparse_values,sparse_values"
    ).option("sparse_vector_index_fields", "sparse_indices,sparse_indices").option(
        "sparse_vector_names", "sparse,another_sparse"
    ).option("schema", df.schema.json()).mode("append").save()

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
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "schema", df.schema.json()
    ).mode("append").save()

    assert (
        qdrant.client.count(qdrant.collection_name).count == df.count()
    ), "Uploaded points count is not equal to the dataframe count"


def test_custom_id_field(qdrant: Qdrant, spark_session: SparkSession):
    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )

    df = (
        spark_session.read.schema(schema)
        .option("multiline", "true")
        .json(str(input_file_path))
    )
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "embedding_field", "dense_vector"
    ).option("schema", df.schema.json()).option("vector_name", "dense").option(
        "id_field", "id"
    ).mode("append").save()

    assert len(qdrant.client.retrieve(qdrant.collection_name, [1, 2, 3, 15, 18])) == 5
