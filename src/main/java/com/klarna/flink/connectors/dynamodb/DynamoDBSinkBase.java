package com.klarna.flink.connectors.dynamodb;

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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public abstract class DynamoDBSinkBase<IN, OUT> extends RichSinkFunction<IN> implements CheckpointedFunction {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected transient DynamoDBAsyncClient client;

    private FutureCallback<OUT> callback;
    private AtomicReference<Throwable> throwable;
    private Semaphore semaphore;

    private DynamoDBSinkBaseConfig config;
    private final DynamoDBBuilder builder;
    private final DynamoDBFailureHandler failureHandler;

    protected DynamoDBSinkBase(final DynamoDBBuilder builder,
                               final DynamoDBSinkBaseConfig config,
                               final DynamoDBFailureHandler failureHandler) {
        this.builder = builder;
        this.config = config;
        this.failureHandler = failureHandler;
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        checkAsyncErrors();
        tryAcquire(1);
        final ListenableFuture<OUT> result;
        try {
            result = send(value);
        } catch (Throwable e) {
            semaphore.release();
            throw e;
        }
        Futures.addCallback(result, callback);
    }

    protected abstract ListenableFuture<OUT> send(IN value);

    @Override
    public void open(Configuration parameters) throws Exception {
        callback = new FutureCallback<OUT>() {
            @Override
            public void onSuccess(@Nullable OUT out) {
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
            flush();
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
        flush();
        checkAsyncErrors();
    }

    private void checkAsyncErrors() throws Exception {
        final Throwable currentError = throwable.getAndSet(null);
        if (currentError != null) {
            failureHandler.onFailure(currentError);
        }
    }

    private void flush() throws InterruptedException, TimeoutException {
        tryAcquire(config.getMaxConcurrentRequests());
        semaphore.release(config.getMaxConcurrentRequests());
    }

    private void tryAcquire(int permits) throws InterruptedException, TimeoutException {
        if (!semaphore.tryAcquire(permits, config.getMaxConcurrentRequestsTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException(
                    String.format(
                            "Failed to acquire %d out of %d permits to send value in %s.",
                            permits,
                            config.getMaxConcurrentRequests(),
                            config.getMaxConcurrentRequestsTimeout()
                    )
            );
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
