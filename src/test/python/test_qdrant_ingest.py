from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.errors import IllegalArgumentException
import pytest
from .conftest import Qdrant

input_file_path = Path(__file__).with_name("users.json")

def test_upsert_unnamed_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = spark_session.read.option("multiline", "true").json(str(input_file_path))
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "embedding_field", "dense_vector"
    ).option("schema", df.schema.json()).mode("append").save()

    qdrant.client.count(qdrant.collection_name) == df.count()


def test_upsert_named_vectors(qdrant: Qdrant, spark_session: SparkSession):
    df = spark_session.read.option("multiline", "true").json(str(input_file_path))
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "embedding_field", "dense_vector"
    ).option("schema", df.schema.json()).option("vector_name", "dense").mode(
        "append"
    ).save()

    qdrant.client.count(qdrant.collection_name) == df.count()


def test_missing_field(qdrant: Qdrant, spark_session: SparkSession):
    df = spark_session.read.option("multiline", "true").json(str(input_file_path))

    with pytest.raises(IllegalArgumentException) as e:
        df.write.format("io.qdrant.spark.Qdrant").option(
            "qdrant_url",
            qdrant.url,
        ).option("collection_name", qdrant.collection_name).option(
            "embedding_field", "missing_field"
        ).option("schema", df.schema.json()).option("vector_name", "dense").mode(
            "append"
        ).save()

        assert "Specified 'embedding_field' is not present in the schema" in str(e)


def test_custom_id_field(qdrant: Qdrant, spark_session: SparkSession):
    df = spark_session.read.option("multiline", "true").json(str(input_file_path))

    df = spark_session.read.option("multiline", "true").json(str(input_file_path))
    df.write.format("io.qdrant.spark.Qdrant").option(
        "qdrant_url",
        qdrant.url,
    ).option("collection_name", qdrant.collection_name).option(
        "embedding_field", "dense_vector"
    ).option("schema", df.schema.json()).option("vector_name", "dense").option(
        "id_field", "id"
    ).mode("append").save()

    assert len(qdrant.client.retrieve(qdrant.collection_name, [1, 2, 3, 15, 18])) == 5
