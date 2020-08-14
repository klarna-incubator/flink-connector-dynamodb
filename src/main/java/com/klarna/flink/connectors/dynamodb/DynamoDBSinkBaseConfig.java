package com.klarna.flink.connectors.dynamodb;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;

public class DynamoDBSinkBaseConfig implements Serializable {

    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 1000;
    private static final Duration DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT = Duration.ofMillis(1000L);

    private final int maxConcurrentRequests;
    private final Duration maxConcurrentRequestsTimeout;

    public DynamoDBSinkBaseConfig(int maxConcurrentRequests,
                                  Duration maxConcurrentRequestsTimeout) {
        Preconditions.checkArgument(maxConcurrentRequests > 0,
                "Max concurrent requests is expected to be positive");
        Preconditions.checkNotNull(maxConcurrentRequestsTimeout,
                "Max concurrent requests timeout cannot be null");
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.maxConcurrentRequestsTimeout = maxConcurrentRequestsTimeout;
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public Duration getMaxConcurrentRequestsTimeout() {
        return maxConcurrentRequestsTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
        private Duration maxConcurrentRequestsTimeout = DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT;

        public Builder maxConcurrentRequests(final int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            return this;
        }

        public Builder maxConcurrentRequestsTimeout(final Duration maxConcurrentRequestsTimeout) {
            this.maxConcurrentRequestsTimeout = maxConcurrentRequestsTimeout;
            return this;
        }

        public DynamoDBSinkBaseConfig build() {
            return new DynamoDBSinkBaseConfig(maxConcurrentRequests, maxConcurrentRequestsTimeout);
        }

    }
}
