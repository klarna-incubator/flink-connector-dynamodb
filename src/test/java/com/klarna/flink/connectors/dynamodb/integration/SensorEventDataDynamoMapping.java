package com.klarna.flink.connectors.dynamodb.integration;

import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class SensorEventDataDynamoMapping {

  public static SensorEventData loadFromDynamo(String sensorId, Long timestamp, DynamoDbClient dynamodbClient, String tableName) {

    var response = dynamodbClient.getItem(
        GetItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                "PK", AttributeValue.builder().s(sensorId).build(),
                "SK", AttributeValue.builder().n(timestamp.toString()).build()
            ))
            .build()
    );

    if (!response.hasItem()) {
      throw new RuntimeException(
          "could not retrieve SensorEventData for PK=" + sensorId + ", SK=" + timestamp + " from table " + tableName);
    } else {
      return new SensorEventData(
          response.item().get("PK").s(),
          Long.parseLong(response.item().get("SK").n()),
          Long.parseLong(response.item().get("Field1").n()),
          Long.parseLong(response.item().get("Field2").n()),
          Long.parseLong(response.item().get("Field3").n())
      );
    }
  }

  public static PutRequest asDynamoPutItemRequest(SensorEventData testData) {
    return PutRequest.builder().item(
        Map.of(
            "PK", AttributeValue.builder().s(testData.sensorId).build(),
            "SK", AttributeValue.builder().n(testData.timestamp.toString()).build(),
            "Field1", AttributeValue.builder().n(testData.field1.toString()).build(),
            "Field2", AttributeValue.builder().n(testData.field2.toString()).build(),
            "Field3", AttributeValue.builder().n(testData.field3.toString()).build()
        ))
        .build();
  }

  /**
   * Provides a String representation of the DynamoKey
   */
  public static String dynamoKeyStr(WriteRequest writeRequest) {
    return writeRequest.putRequest().item().get("PK").s()
        + writeRequest.putRequest().item().get("SK").n();
  }


}
