package com.klarna.flink.connectors.dynamodb;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.io.Serializable;

@PublicEvolving
public interface DynamoDBFailureHandler extends Serializable {
    void onFailure(Throwable t) throws IOException;
}
