package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class DynamoDBSink extends RichSinkFunction<DynamoDBWriteRequest> implements CheckpointedFunction {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected transient DynamoDBWriter dynamoDBWriter;

    private Map<String, List<WriteRequest>> batchUnderProcess = new HashMap<>();
    private int currentBatchSize = 0;

    private FutureCallback<BatchResponse> callback;
    private AtomicReference<Throwable> throwable;
    private Semaphore semaphore;

    private DynamoDBSinkConfig config;
    private final DynamoDBWriterBuilder builder;
    private final DynamoDBFailureHandler failureHandler;

    public DynamoDBSink(final DynamoDBWriterBuilder builder,
                        final DynamoDBSinkConfig config,
                        final DynamoDBFailureHandler failureHandler) {
        this.builder = builder;
        this.config = config;
        this.failureHandler = failureHandler;
    }

    @Override
    public void invoke(DynamoDBWriteRequest value, Context context) throws Exception {
        if (dynamoDBWriter == null) {
            throw new RuntimeException("DynamoDB writer is closed");
        }
        checkAsyncErrors();
        String tableName = value.getTableName();
        final List<WriteRequest> writeRequests = batchUnderProcess.computeIfAbsent(tableName,
                k -> new ArrayList<>());
        writeRequests.add(value.getWriteRequest());
        currentBatchSize++;
        if (currentBatchSize >= config.getBatchSize()) {
            process();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        callback = new FutureCallback<BatchResponse>() {
            @Override
            public void onSuccess(@Nullable BatchResponse out) {
                if (out != null) {
                    if (!out.isSuccess()) {
                        throwable.compareAndSet(null, out.getT());
                    }
                }
                semaphore.release();
            }

            @Override
            public void onFailure(Throwable t) {
                throwable.compareAndSet(null, t);
                logger.error("Error while sending value.", t);
                semaphore.release();
            }
        };
        dynamoDBWriter = builder.getAmazonDynamoDB();
        semaphore = new Semaphore(config.getMaxConcurrentRequests());
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
                if (dynamoDBWriter != null) {
                    dynamoDBWriter.close();
                    dynamoDBWriter = null;
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

    private void process() {
        if (currentBatchSize > 0) {
            semaphore.tryAcquire(1);
            final BatchRequest batchRequest = new BatchRequest(batchUnderProcess);
            batchUnderProcess = new HashMap<>();
            currentBatchSize = 0;
            Futures.addCallback(dynamoDBWriter.batchWrite(batchRequest), callback);
        }
    }

    private void flush() throws Exception {
        while (semaphore.availablePermits() != config.getMaxConcurrentRequests()) {
            process();
            checkAsyncErrors();
        }
    }

    private void checkAsyncErrors() throws Exception {
        final Throwable currentError = throwable.getAndSet(null);
        if (currentError != null) {
            failureHandler.onFailure(currentError);
        }
    }

    @VisibleForTesting
    int getAvailablePermits() {
        return semaphore.availablePermits();
    }

    @VisibleForTesting
    int getAcquiredPermits() {
        return config.getMaxConcurrentRequests() - semaphore.availablePermits();
    }

}
