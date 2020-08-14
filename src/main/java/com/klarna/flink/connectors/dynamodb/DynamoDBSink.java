package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public abstract class DynamoDBSink<T> extends RichSinkFunction<DynamoDBSinkInput<T>> implements CheckpointedFunction {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected transient DynamoDBAsyncClient client;

    private Map<String, List<WriteRequest>> batchUnderProcess = new HashMap<>();

    private FutureCallback<BatchResponse> callback;
    private AtomicReference<Throwable> throwable;
    private Semaphore semaphore;

    private DynamoDBSinkBaseConfig config;
    private final DynamoDBBuilder builder;
    private final DynamoDBFailureHandler failureHandler;

    protected DynamoDBSink(final DynamoDBBuilder builder,
                           final DynamoDBSinkBaseConfig config,
                           final DynamoDBFailureHandler failureHandler) {
        this.builder = builder;
        this.config = config;
        this.failureHandler = failureHandler;
    }

    @Override
    public void invoke(DynamoDBSinkInput<T> value, Context context) throws Exception {
        checkAsyncErrors();
        semaphore.tryAcquire(1);
        String tableName = value.tableName;
        // initialize list correctly here
        batchUnderProcess.get(tableName).add(new WriteRequest());
        if (batchUnderProcess.size() >= 25) {
            //init new list to use
            batchUnderProcess.clear();
            Futures.addCallback(client.batchWrite(batchUnderProcess), callback);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        callback = new FutureCallback<BatchResponse>() {
            @Override
            public void onSuccess(@Nullable BatchResponse out) {
                // check status and set exception
                semaphore.release();
            }

            @Override
            public void onFailure(Throwable t) {
                throwable.compareAndSet(null, t);
                logger.error("Error while sending value.", t);
                semaphore.release();
            }
        };
        client = builder.getAmazonDynamoDB();
        semaphore = new Semaphore(config.getMaxConcurrentRequests());
        throwable = new AtomicReference<>();
    }

    @Override
    public void close() throws Exception {
        try {
            checkAsyncErrors();
        } finally {
            try {
                if (client != null) {
                    client.close();
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
