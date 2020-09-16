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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class FlinkDynamoDBSink extends RichSinkFunction<DynamoDBWriteRequest> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDynamoDBSink.class);

    /** Batch processor to buffer and send requests to DynamoDB */
    private transient DynamoDBBatchProcessor dynamoDBBatchProcessor;

    /**
     * This is set from inside the {@link DynamoDBBatchProcessor.Listener} if a {@link Throwable} was thrown in callbacks and
     * the user considered it should fail the sink via the
     * {@link DynamoDBFailureHandler#onFailure(Throwable)} method.
     *
     * Errors will be checked and rethrown before processing each input element, and when the sink is closed.
     */
    private final AtomicReference<Throwable> throwable = new AtomicReference<>();

    /** User-provided handler for failed batch request */
    private final DynamoDBFailureHandler failureHandler;

    /** DynamoDB sink configuration */
    private final DynamoDBSinkConfig dynamoDBSinkConfig;

    /** Builder for Amazon Dynamo DB*/
    private final AmazonDynamoDBBuilder amazonDynamoDBBuilder;

    /** number of pending records */
    private final AtomicLong numPendingRecords = new AtomicLong(0);

    /**
     * Constructor of FlinkDynamoDBSink
     * @param amazonDynamoDBBuilder builder for dynamo db client
     * @param dynamoDBSinkConfig configuration for dynamo db sink
     * @param failureHandler failure handler
     */
    public FlinkDynamoDBSink(final AmazonDynamoDBBuilder amazonDynamoDBBuilder,
                             final DynamoDBSinkConfig dynamoDBSinkConfig,
                             final DynamoDBFailureHandler failureHandler) {
        Preconditions.checkNotNull(amazonDynamoDBBuilder, "amazonDynamoDBBuilder must not be null");
        Preconditions.checkNotNull(dynamoDBSinkConfig, "DynamoDBSinkConfig must not be null");
        Preconditions.checkNotNull(failureHandler, "FailureHandler must not be null");
        this.failureHandler = failureHandler;
        this.dynamoDBSinkConfig = dynamoDBSinkConfig;
        this.amazonDynamoDBBuilder = amazonDynamoDBBuilder;
    }

    @Override
    public void invoke(DynamoDBWriteRequest value, Context context) throws Exception {
        if (dynamoDBBatchProcessor == null) {
            throw new NullPointerException("DynamoDB batch processor is closed");
        }
        checkAsyncErrors();
        numPendingRecords.incrementAndGet();
        dynamoDBBatchProcessor.add(value);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.dynamoDBBatchProcessor = buildDynamoDBBatchProcessor(new DynamoDBBatchProcessorListener());
        dynamoDBBatchProcessor.open();
    }

    @Override
    public void close() throws Exception {
        try {
            checkAsyncErrors();
            flush();
            checkAsyncErrors();
        } finally {
            try {
                if (dynamoDBBatchProcessor != null) {
                    dynamoDBBatchProcessor.close();
                    dynamoDBBatchProcessor = null;
                }
            } catch (Exception e) {
                LOG.warn("Error while closing DynamoDBBatchProcessor", e);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        checkAsyncErrors();
        flush();
        checkAsyncErrors();
    }

    /**
     * When numPendingRecords equals 0, it means all batch inserts completed successfully
     * @throws Exception propagated exception from {@link FlinkDynamoDBSink#checkAsyncErrors()}
     */
    private void flush() throws Exception {
        while (numPendingRecords.get() > 0) {
            dynamoDBBatchProcessor.flush();
            checkAsyncErrors();
        }
    }

    /**
     *
     * @throws Exception propagated exception from failureHandler
     */
    private void checkAsyncErrors() throws Exception {
        final Throwable currentError = throwable.getAndSet(null);
        if (currentError != null) {
            failureHandler.onFailure(currentError);
        }
    }

    @VisibleForTesting
    long getNumPendingRecords() {
        return numPendingRecords.get();
    }

    /**
     * Build the {@link DynamoDBBatchProcessor}.
     * this is exposed for testing purposes.
     */
    @VisibleForTesting
    protected DynamoDBBatchProcessor buildDynamoDBBatchProcessor(DynamoDBBatchProcessor.Listener listener) {
        return new DynamoDBBatchProcessor(amazonDynamoDBBuilder,
                dynamoDBSinkConfig.getMaxConcurrentRequests(),
                dynamoDBSinkConfig.getBatchSize(),
                listener);
    }

    private class DynamoDBBatchProcessorListener implements DynamoDBBatchProcessor.Listener {

        @Override
        public void onSuccess(BatchResponse batchResponse) {
            if (batchResponse != null) {
                if (batchResponse.getT() != null) {
                    throwable.compareAndSet(null, batchResponse.getT());
                } else {
                    numPendingRecords.addAndGet(-batchResponse.getBatchSize());
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            throwable.compareAndSet(null, t);
        }
    }

}
