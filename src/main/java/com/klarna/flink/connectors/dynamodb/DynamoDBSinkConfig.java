package com.klarna.flink.connectors.dynamodb;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;

public class DynamoDBSinkConfig implements Serializable {

    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 20;
    private static final Duration DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT = Duration.ofMillis(1000L);
    private static final int DEFAULT_BATCH_SIZE = 25;

    private final int maxConcurrentRequests;

    private final int batchSize;
    private final Duration maxConcurrentRequestsTimeout;

    public DynamoDBSinkConfig(int maxConcurrentRequests,
                              int batchSize,
                              Duration maxConcurrentRequestsTimeout) {
        Preconditions.checkArgument(maxConcurrentRequests > 0,
                "Max concurrent requests is expected to be positive");
        Preconditions.checkArgument(batchSize > 0 && batchSize <= 25,
                "Batch size is expected to be greater than 1 and less than equals to 25");
        Preconditions.checkNotNull(maxConcurrentRequestsTimeout,
                "Max concurrent requests timeout cannot be null");
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.batchSize = batchSize;
        this.maxConcurrentRequestsTimeout = maxConcurrentRequestsTimeout;
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Duration getMaxConcurrentRequestsTimeout() {
        return maxConcurrentRequestsTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private Duration maxConcurrentRequestsTimeout = DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT;

        public Builder maxConcurrentRequests(final int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            return this;
        }

        public Builder batchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder maxConcurrentRequestsTimeout(final Duration maxConcurrentRequestsTimeout) {
            this.maxConcurrentRequestsTimeout = maxConcurrentRequestsTimeout;
            return this;
        }

        public DynamoDBSinkConfig build() {
            return new DynamoDBSinkConfig(maxConcurrentRequests, batchSize, maxConcurrentRequestsTimeout);
        }

    }
}
