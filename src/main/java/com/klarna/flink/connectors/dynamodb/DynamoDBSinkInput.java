package com.klarna.flink.connectors.dynamodb;

public class DynamoDBSinkInput<T> {
    String tableName;
    T value;

}
