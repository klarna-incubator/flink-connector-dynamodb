package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import java.io.Serializable;

public interface AmazonDynamoDBBuilder extends Serializable {
    AmazonDynamoDB build();
}
