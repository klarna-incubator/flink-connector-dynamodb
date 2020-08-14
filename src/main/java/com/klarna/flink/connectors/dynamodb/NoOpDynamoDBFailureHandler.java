package com.klarna.flink.connectors.dynamodb;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * Default implementation for failure handler
 */
@Internal
public class NoOpDynamoDBFailureHandler implements DynamoDBFailureHandler {
    @Override
    public void onFailure(Throwable t) throws IOException {
        throw new IOException("Error sending value", t);
    }
}
