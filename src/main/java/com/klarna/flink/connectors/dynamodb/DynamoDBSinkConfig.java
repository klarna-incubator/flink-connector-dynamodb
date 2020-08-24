package com.klarna.flink.connectors.dynamodb;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

public class DynamoDBSinkConfig implements Serializable {

    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 20;
    private static final int DEFAULT_BATCH_SIZE = 25;

    private final int maxConcurrentRequests;
    private final int batchSize;

    public DynamoDBSinkConfig(int maxConcurrentRequests,
                              int batchSize) {
        Preconditions.checkArgument(maxConcurrentRequests > 0,
                "Max concurrent requests is expected to be positive");
        Preconditions.checkArgument(batchSize > 0 && batchSize <= 25,
                "Batch size is expected to be greater than 1 and less than equals to 25");
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.batchSize = batchSize;
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public int getBatchSize() {
        return batchSize;
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
        private int batchSize = DEFAULT_BATCH_SIZE;

        public Builder maxConcurrentRequests(final int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            return this;
        }

        public Builder batchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public DynamoDBSinkConfig build() {
            return new DynamoDBSinkConfig(maxConcurrentRequests, batchSize);
        }

    }
}
