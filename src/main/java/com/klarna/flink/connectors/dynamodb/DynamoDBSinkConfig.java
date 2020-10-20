/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.flink.connectors.dynamodb;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Configuration for {@link FlinkDynamoDBSink}.
 */
public class DynamoDBSinkConfig implements Serializable {

    /**
     * The default maximum number of concurrent requests. By default, 25.
     */
    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 20;

    /**
     * The default batch size. By default, 25.
     */
    private static final int DEFAULT_BATCH_SIZE = 25;

    /** Maximum number of concurrent requests allowed. */
    private final int maxConcurrentRequests;

    /** Batch size, max batch size is 25 */
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

    /**
     * Builder for the {@link DynamoDBSinkConfig}.
     */
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
