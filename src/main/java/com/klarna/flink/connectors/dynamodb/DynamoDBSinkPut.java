package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.util.Preconditions;

public class DynamoDBSinkPut<IN> extends DynamoDBSinkBase<IN, PutItemResult> {

    private final DynamoDBItemRequestMapper<IN, PutItemRequest> mapper;

    public DynamoDBSinkPut(final DynamoDBBuilder builder,
                           final DynamoDBSinkBaseConfig config,
                           final DynamoDBFailureHandler dynamoDBFailureHandler,
                           final DynamoDBItemRequestMapper<IN, PutItemRequest> dynamoDBItemRequestMapper) {
        super(builder, config, dynamoDBFailureHandler);
        Preconditions.checkNotNull(dynamoDBItemRequestMapper, "dynamoDBItemRequestMapper must not be null");
        this.mapper = dynamoDBItemRequestMapper;
    }

    @Override
    protected ListenableFuture<PutItemResult> send(IN value) {
        return client.put(mapper.map(value));
    }
}
