package com.klarna.flink.connectors.dynamodb;

import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.Serializable;

/**
 * Mapping an object to a DynamoDB WriteRequest
 * @param <IN> the object to map
 */
@FunctionalInterface
public interface DynamoDBSinkWriteRequestMapper<IN> extends Serializable {

    WriteRequest map(IN in);

}
