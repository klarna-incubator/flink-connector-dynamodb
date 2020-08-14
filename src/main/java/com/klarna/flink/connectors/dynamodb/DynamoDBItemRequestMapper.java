package com.klarna.flink.connectors.dynamodb;

import java.io.Serializable;

public interface DynamoDBItemRequestMapper<IN, OUT> extends Serializable {
    OUT map(IN out);
}
