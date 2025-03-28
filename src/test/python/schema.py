from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    FloatType,
    TimestampType
)

hair_schema = StructType(
    [
        StructField("color", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
    ]
)

coordinates_schema = StructType(
    [
        StructField("lat", DoubleType(), nullable=True),
        StructField("lng", DoubleType(), nullable=True),
    ]
)

address_schema = StructType(
    [
        StructField("address", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("coordinates", coordinates_schema, nullable=True),
        StructField("postalCode", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
    ]
)

bank_schema = StructType(
    [
        StructField("cardExpire", StringType(), nullable=True),
        StructField("cardNumber", StringType(), nullable=True),
        StructField("cardType", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("iban", StringType(), nullable=True),
    ]
)

company_address_schema = StructType(
    [
        StructField("address", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("coordinates", coordinates_schema, nullable=True),
        StructField("postalCode", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
    ]
)

company_schema = StructType(
    [
        StructField("address", company_address_schema, nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
    ]
)

crypto_schema = StructType(
    [
        StructField("coin", StringType(), nullable=True),
        StructField("wallet", StringType(), nullable=True),
        StructField("network", StringType(), nullable=True),
    ]
)

schema = StructType(
    [
        StructField("id", IntegerType(), nullable=True),
        StructField("firstName", StringType(), nullable=True),
        StructField("lastName", StringType(), nullable=True),
        StructField("maidenName", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("username", StringType(), nullable=True),
        StructField("password", StringType(), nullable=True),
        StructField("birthDate", StringType(), nullable=True),
        StructField("image", StringType(), nullable=True),
        StructField("bloodGroup", StringType(), nullable=True),
        StructField("height", DoubleType(), nullable=True),
        StructField("weight", DoubleType(), nullable=True),
        StructField("eyeColor", StringType(), nullable=True),
        StructField("hair", hair_schema, nullable=True),
        StructField("domain", StringType(), nullable=True),
        StructField("ip", StringType(), nullable=True),
        StructField("address", address_schema, nullable=True),
        StructField("macAddress", StringType(), nullable=True),
        StructField("university", StringType(), nullable=True),
        StructField("bank", bank_schema, nullable=True),
        StructField("company", company_schema, nullable=True),
        StructField("ein", StringType(), nullable=True),
        StructField("ssn", StringType(), nullable=True),
        StructField("userAgent", StringType(), nullable=True),
        StructField("crypto", crypto_schema, nullable=True),
        StructField("dense_vector", ArrayType(FloatType()), nullable=False),
        StructField("sparse_indices", ArrayType(IntegerType()), nullable=False),
        StructField("sparse_values", ArrayType(FloatType()), nullable=False),
        StructField("multi", ArrayType(ArrayType(FloatType())), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
    ]
)
