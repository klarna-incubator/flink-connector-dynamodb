package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class DynamoDBWriteRequest {

    private String tableName;
    private WriteRequest writeRequest;

    public DynamoDBWriteRequest(String tableName, WriteRequest writeRequest) {
        this.tableName = tableName;
        this.writeRequest = writeRequest;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public WriteRequest getWriteRequest() {
        return writeRequest;
    }

    public void setWriteRequest(WriteRequest writeRequest) {
        this.writeRequest = writeRequest;
    }
}
