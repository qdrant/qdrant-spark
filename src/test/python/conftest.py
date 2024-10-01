import pytest
from testcontainers.core.container import DockerContainer
from qdrant_client import QdrantClient, models
import uuid
from pyspark.sql import SparkSession
from typing import NamedTuple
from uuid import uuid4
from testcontainers.core.waiting_utils import wait_for_logs


QDRANT_GRPC_PORT = 6334
QDRANT_EMBEDDING_DIM = 6
QDRANT_DISTANCE = models.Distance.COSINE
QDRANT_API_KEY = uuid4().hex
STRING_SHARD_KEY = "string_shard_key_32_()-"
INTEGER_SHARD_KEY = 876


class Qdrant(NamedTuple):
    url: str
    api_key: str
    collection_name: str
    client: QdrantClient


qdrant_container = (
    (
        DockerContainer(image="qdrant/qdrant:latest")
        .with_env("QDRANT__SERVICE__API_KEY", QDRANT_API_KEY)
        .with_env("QDRANT__CLUSTER__ENABLED", "true")
    )
    .with_command("./qdrant --uri http://qdrant_node_1:6335")
    .with_exposed_ports(QDRANT_GRPC_PORT)
)


# Reference: https://gist.github.com/dizzythinks/f3bb37fd8ab1484bfec79d39ad8a92d3
def get_pom_version():
    from xml.etree import ElementTree as et

    ns = "http://maven.apache.org/POM/4.0.0"
    et.register_namespace("", ns)
    tree = et.ElementTree()
    tree.parse("pom.xml")
    p = tree.getroot().find("{%s}version" % ns)
    return p.text


@pytest.fixture(scope="module", autouse=True)
def setup_container(request):
    qdrant_container.start()
    wait_for_logs(qdrant_container, "Qdrant gRPC listening on 6334")

    def remove_container():
        qdrant_container.stop()

    request.addfinalizer(remove_container)


@pytest.fixture(scope="session")
def spark_session():
    spark_session = (
        SparkSession.builder.config(
            "spark.jars", f"target/spark-{get_pom_version()}.jar"
        )
        .master("local[*]")
        .appName("qdrant")
        .getOrCreate()
    )

    yield spark_session
    return spark_session.stop()


@pytest.fixture()
def qdrant():
    host = qdrant_container.get_container_host_ip()
    grpc_port = qdrant_container.get_exposed_port(QDRANT_GRPC_PORT)

    client = QdrantClient(
        host=host,
        grpc_port=grpc_port,
        prefer_grpc=True,
        api_key=QDRANT_API_KEY,
        https=False,
    )

    collection_name = str(uuid.uuid4())
    client.create_collection(
        collection_name=collection_name,
        vectors_config={
            "dense": models.VectorParams(
                size=QDRANT_EMBEDDING_DIM,
                distance=QDRANT_DISTANCE,
            ),
            "": models.VectorParams(
                size=QDRANT_EMBEDDING_DIM,
                distance=QDRANT_DISTANCE,
            ),
            "another_dense": models.VectorParams(
                size=QDRANT_EMBEDDING_DIM,
                distance=QDRANT_DISTANCE,
            ),
            "multi": models.VectorParams(
                size=QDRANT_EMBEDDING_DIM,
                distance=QDRANT_DISTANCE,
                multivector_config=models.MultiVectorConfig(
                    comparator=models.MultiVectorComparator.MAX_SIM
                ),
            ),
        },
        sparse_vectors_config={
            "sparse": models.SparseVectorParams(),
            "another_sparse": models.SparseVectorParams(),
        },
        sharding_method=models.ShardingMethod.CUSTOM,
    )
    
    client.create_shard_key(collection_name, STRING_SHARD_KEY)
    client.create_shard_key(collection_name, INTEGER_SHARD_KEY)

    yield Qdrant(
        url=f"http://{host}:{grpc_port}",
        client=client,
        collection_name=collection_name,
        api_key=QDRANT_API_KEY,
    )

    return client.close()
