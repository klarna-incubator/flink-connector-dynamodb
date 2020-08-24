package com.klarna.flink.connectors.dynamodb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public class DynamoDBSink extends RichSinkFunction<DynamoDBWriteRequest> implements CheckpointedFunction {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected transient DynamoDBBatchProcessor dynamoDBBatchProcessor;

    private AtomicReference<Throwable> throwable;

    private final DynamoDBFailureHandler failureHandler;

    private final DynamoDBSinkConfig dynamoDBSinkConfig;

    private final AmazonDynamoDBBuilder amazonDynamoDBBuilder;

    public DynamoDBSink(final AmazonDynamoDBBuilder amazonDynamoDBBuilder,
                        final DynamoDBSinkConfig dynamoDBSinkConfig,
                        final DynamoDBFailureHandler failureHandler) {
        Preconditions.checkNotNull(amazonDynamoDBBuilder, "amazonDynamoDBBuilder must not be null");
        Preconditions.checkNotNull(dynamoDBSinkConfig, "DynamoDBSinkConfig must not be null");
        Preconditions.checkNotNull(failureHandler, "FailureHandler must not be null");
        this.failureHandler = failureHandler;
        this.dynamoDBSinkConfig = dynamoDBSinkConfig;
        this.amazonDynamoDBBuilder = amazonDynamoDBBuilder;
    }

    protected DynamoDBBatchProcessor buildDynamoDBBatchProcessor(AmazonDynamoDBBuilder amazonDynamoDBBuilder,
                                                                 int maxConcurrentRequests,
                                                                 int batchSize,
                                                                 DynamoDBBatchProcessor.Listener listener) {
        return new DynamoDBBatchProcessor(amazonDynamoDBBuilder,
                maxConcurrentRequests,
                batchSize,
                listener);
    }

    @Override
    public void invoke(DynamoDBWriteRequest value, Context context) throws Exception {
        if (dynamoDBBatchProcessor == null) {
            throw new NullPointerException("DynamoDB writer is closed");
        }
        checkAsyncErrors();
        dynamoDBBatchProcessor.add(value);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.dynamoDBBatchProcessor = buildDynamoDBBatchProcessor(amazonDynamoDBBuilder,
                dynamoDBSinkConfig.getMaxConcurrentRequests(),
                dynamoDBSinkConfig.getBatchSize(),
                new DynamoDBBatchProcessorListener());
        dynamoDBBatchProcessor.open();
        throwable = new AtomicReference<>();
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
                logger.error("Error while closing com.klarna.flink.connectors.dynamodb client.", e);
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

    private void flush() throws Exception {
        while (dynamoDBBatchProcessor.getOutBatches() > 0) {
            dynamoDBBatchProcessor.flush();
            checkAsyncErrors();
        }
    }

    private void checkAsyncErrors() throws Exception {
        final Throwable currentError = throwable.getAndSet(null);
        if (currentError != null) {
            failureHandler.onFailure(currentError);
        }
    }

    private class DynamoDBBatchProcessorListener implements DynamoDBBatchProcessor.Listener {
        @Override
        public void onSuccess(BatchResponse batchResponse) {
            if (batchResponse != null && batchResponse.getT() != null) {
                throwable.compareAndSet(null, batchResponse.getT());
            }
        }

        @Override
        public void onFailure(Throwable t) {
            throwable.compareAndSet(null, t);
        }
    }

}
