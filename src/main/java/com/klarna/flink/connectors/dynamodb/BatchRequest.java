package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import java.util.List;
import java.util.Map;

public class BatchRequest {

    private final Map<String, List<WriteRequest>> batch;

    public BatchRequest(Map<String, List<WriteRequest>> batch) {
        this.batch = batch;
    }

    public Map<String, List<WriteRequest>> getBatch() {
        return batch;
    }
}
