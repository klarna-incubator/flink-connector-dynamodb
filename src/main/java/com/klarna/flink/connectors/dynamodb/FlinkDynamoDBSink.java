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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.klarna.flink.connectors.dynamodb.utils.TimeoutLatch;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkDynamoDBSink extends RichSinkFunction<DynamoDBWriteRequest> implements CheckpointedFunction {

    public static final String DYNAMO_DB_SINK_METRIC_GROUP = "dynamoDBSink";

    public static final String METRIC_BACKPRESSURE_CYCLES = "backpressureCycles";

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDynamoDBSink.class);

    /** Batch processor to buffer and send requests to DynamoDB */
    private transient DynamoDBProducer producer;

    /** User-provided handler for failed batch request */
    private final DynamoDBFailureHandler failureHandler;

    /** DynamoDB sink configuration */
    private final DynamoDBSinkConfig dynamoDBSinkConfig;

    /** Builder for Amazon Dynamo DB*/
    private final FlinkDynamoDBClientBuilder flinkDynamoDBClientBuilder;

    /** Counts how often we have to wait for KPL because we are above the queue limit */
    private transient Counter backpressureCycles;

    /** Backpressuring waits for this latch, triggered by record callback */
    private transient volatile TimeoutLatch backpressureLatch;

    /** Callback handling failures */
    private transient FutureCallback<BatchResponse> callback;

    /** holds the first thrown exception in the sink */
    private Throwable thrownException = null;

    /** limit for the outgoing batches */
    private long queueLimit;

    /** key selector passed to the producer to deduplicate by keys */
    private KeySelector<DynamoDBWriteRequest, String> keySelector;

    /**
     * Constructor of FlinkDynamoDBSink
     * @param flinkDynamoDBClientBuilder builder for dynamo db client
     * @param dynamoDBSinkConfig configuration for dynamo db sink
     * @param keySelector key used to deduplicate records
     * @param failureHandler failure handler
     */
    public FlinkDynamoDBSink(final FlinkDynamoDBClientBuilder flinkDynamoDBClientBuilder,
                             final DynamoDBSinkConfig dynamoDBSinkConfig,
                             final KeySelector<DynamoDBWriteRequest, String> keySelector,
                             final DynamoDBFailureHandler failureHandler) {
        Preconditions.checkNotNull(flinkDynamoDBClientBuilder, "amazonDynamoDBBuilder must not be null");
        Preconditions.checkNotNull(dynamoDBSinkConfig, "DynamoDBSinkConfig must not be null");
        Preconditions.checkNotNull(failureHandler, "FailureHandler must not be null");
        this.failureHandler = failureHandler;
        this.dynamoDBSinkConfig = dynamoDBSinkConfig;
        this.flinkDynamoDBClientBuilder = flinkDynamoDBClientBuilder;
        this.queueLimit = dynamoDBSinkConfig.getQueueLimit();
        this.keySelector = keySelector;
    }

    @Override
    public void invoke(DynamoDBWriteRequest value, Context context) throws Exception {
        if (producer == null) {
            throw new NullPointerException("DynamoDB batch processor is closed");
        }
        checkAsyncErrors();
        boolean didWaitForFlush = enforceQueueLimit();
        if (didWaitForFlush) {
            checkAsyncErrors();
        }
        ListenableFuture<BatchResponse> add = producer.add(value);
        Futures.addCallback(add, callback, MoreExecutors.directExecutor());
    }

    @Override
    public void open(Configuration parameters) {
        backpressureLatch = new TimeoutLatch();
        final MetricGroup dynamoDBSinkMectricGroup =
                getRuntimeContext().getMetricGroup().addGroup(DYNAMO_DB_SINK_METRIC_GROUP);
        this.backpressureCycles = dynamoDBSinkMectricGroup.counter(METRIC_BACKPRESSURE_CYCLES);
        callback =
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(BatchResponse result) {
                        backpressureLatch.trigger();
                        if (!result.isSuccessful()) {
                            if (thrownException == null) {
                                thrownException = new RuntimeException("Error", result.getThrowable());
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        backpressureLatch.trigger();
                        thrownException = t;
                    }
                };
        this.producer = getDynamoDBProducer();
    }

    @Override
    public void close() throws Exception {
        try {
            LOG.info("Closing sink");
            super.close();
            flushSync();
            checkAsyncErrors();
        } finally {
            try {
                if (producer != null) {
                    producer.destroy();
                    producer = null;
                }
            } catch (Exception e) {
                LOG.warn("Error while closing DynamoDBProducer", e);
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
        flushSync();
        checkAsyncErrors();
    }

    /**
     *
     * @throws Exception propagated exception from failureHandler
     */
    private void checkAsyncErrors() throws Exception {
        if (thrownException != null) {
            failureHandler.onFailure(thrownException);
        }
    }

    /**
     * If the internal queue of the {@link DynamoDBProducer} gets too long, flush some of the records
     * until we are below the limit again. We don't want to flush _all_ records at this point since
     * that would break record aggregation.
     *
     * @return boolean whether flushing occurred or not
     */
    private boolean enforceQueueLimit() {
        int attempt = 0;
        while (producer.getOutstandingRecordsCount() >= queueLimit) {
            backpressureCycles.inc();
            if (attempt >= 10) {
                LOG.warn(
                        "Waiting for the queue length to drop below the limit takes unusually long, still not done after {} attempts.",
                        attempt);
            }
            attempt++;
            try {
                backpressureLatch.await(100);
            } catch (InterruptedException e) {
                LOG.warn("Flushing was interrupted.");
                break;
            }
        }
        return attempt > 0;
    }

    /**
     * releases the block on flushing if an interruption occurred.
     */
    private void flushSync() throws Exception {
        while (producer.getOutstandingRecordsCount() > 0) {
            producer.flush();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOG.warn("Flushing was interrupted.");
                break;
            }
        }
    }

    /**
     * Creates a {@link DynamoDBProducer}. Exposed so that tests can inject mock producers easily.
     */
    @VisibleForTesting
    protected DynamoDBProducer getDynamoDBProducer() {
        return new DynamoDBProducer(flinkDynamoDBClientBuilder, keySelector,
                dynamoDBSinkConfig.getBatchSize());
    }

}
