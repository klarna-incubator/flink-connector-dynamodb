package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.io.Serializable;

public abstract class DynamoDBWriterBuilder implements Serializable {

    public DynamoDBWriter getAmazonDynamoDB() {
        return new DynamoDBWriter(build(AmazonDynamoDBClientBuilder.standard()));
    }

    protected abstract AmazonDynamoDB build(AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder);
}
