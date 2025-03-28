package io.qdrant.spark;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TestSchema {

  public static StructType schema() {
    StructType hairSchema =
        new StructType()
            .add(new StructField("color", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("type", DataTypes.StringType, true, Metadata.empty()));

    StructType coordinatesSchema =
        new StructType()
            .add(new StructField("lat", DataTypes.DoubleType, true, Metadata.empty()))
            .add(new StructField("lng", DataTypes.DoubleType, true, Metadata.empty()));

    StructType addressSchema =
        new StructType()
            .add(new StructField("address", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("city", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("coordinates", coordinatesSchema, true, Metadata.empty()))
            .add(new StructField("postalCode", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("state", DataTypes.StringType, true, Metadata.empty()));

    StructType bankSchema =
        new StructType()
            .add(new StructField("cardExpire", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("cardNumber", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("cardType", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("currency", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("iban", DataTypes.StringType, true, Metadata.empty()));

    StructType companyAddressSchema =
        new StructType()
            .add(new StructField("address", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("city", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("coordinates", coordinatesSchema, true, Metadata.empty()))
            .add(new StructField("postalCode", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("state", DataTypes.StringType, true, Metadata.empty()));

    StructType companySchema =
        new StructType()
            .add(new StructField("address", companyAddressSchema, true, Metadata.empty()))
            .add(new StructField("department", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("name", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("title", DataTypes.StringType, true, Metadata.empty()));

    StructType cryptoSchema =
        new StructType()
            .add(new StructField("coin", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("wallet", DataTypes.StringType, true, Metadata.empty()))
            .add(new StructField("network", DataTypes.StringType, true, Metadata.empty()));

    DataType denseVectorType = DataTypes.createArrayType(DataTypes.FloatType);
    DataType sparseIndicesType = DataTypes.createArrayType(DataTypes.IntegerType);
    DataType sparseValuesType = denseVectorType;

    return new StructType()
        .add(new StructField("id", DataTypes.IntegerType, true, Metadata.empty()))
        .add(new StructField("firstName", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("lastName", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("maidenName", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("age", DataTypes.IntegerType, true, Metadata.empty()))
        .add(new StructField("gender", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("email", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("phone", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("username", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("password", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("birthDate", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("image", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("bloodGroup", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("height", DataTypes.DoubleType, true, Metadata.empty()))
        .add(new StructField("weight", DataTypes.DoubleType, true, Metadata.empty()))
        .add(new StructField("eyeColor", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("hair", hairSchema, true, Metadata.empty()))
        .add(new StructField("domain", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("ip", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("address", addressSchema, true, Metadata.empty()))
        .add(new StructField("macAddress", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("university", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("bank", bankSchema, true, Metadata.empty()))
        .add(new StructField("company", companySchema, true, Metadata.empty()))
        .add(new StructField("ein", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("ssn", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("userAgent", DataTypes.StringType, true, Metadata.empty()))
        .add(new StructField("crypto", cryptoSchema, true, Metadata.empty()))
        .add(new StructField("dense_vector", denseVectorType, false, Metadata.empty()))
        .add(new StructField("sparse_indices", sparseIndicesType, false, Metadata.empty()))
        .add(new StructField("sparse_values", sparseValuesType, false, Metadata.empty()))
        .add(
            new StructField(
                "multi", DataTypes.createArrayType(denseVectorType), false, Metadata.empty()))
        .add(new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()));
  }
}
