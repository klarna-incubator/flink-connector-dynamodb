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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * This class is used to send batch requests to DynamoDB
 * records are added to the batch processor by calling the add method. The processor accumulates and promotes a batch by inserting it into a queue.
 * A background thread is polling the queue and try to acquire a permit to execute the batch request.
 *
 * This class is not synchronized. It is recommended to create separate format instances for each thread.
 * Concurrent access to this class from multiple threads must be synchronized externally.
 */
@Internal
public class DynamoDBBatchProcessor {

    /**
     * Listener for batch insert operation. allows fot additional logic to be executed on batch request success/failure
     */

    public interface Listener {

        /**
         * invoke on successful batch insert
         * @param batchResponse the response from the batch insert
         */
        void onSuccess(BatchResponse batchResponse);

        /**
         * invoked on failed batch insert
         * @param t the exception thrown by the batch insert
         */
        void onFailure(Throwable t);
    }

    private DynamoDbClient dynamoDbClient;

    private static final int corePoolSize = Runtime.getRuntime().availableProcessors() * (1 + 40/2);

    private final ListeningExecutorService listeningExecutorService;

    /** executor service invoking the batch requests to DynamoDB */
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    /** a semaphore to permit only allowed number of concurrent requests to be executed */
    private Semaphore semaphore;

    /** is the processor closed */
    private transient volatile boolean closed = false;

    /** a listener to invoke in the future callback */
    private Listener listener;

    /** a callback to invoke on a completed future */
    private FutureCallback<BatchResponse> callback;

    private final FlinkDynamoDBClientBuilder flinkDynamoDBClientBuilder;

    /** max number of concurrent requests that are permitted */
    private final int maxConcurrentRequests;

    /** the batch size for the batch request */
    private final int batchSize;

    /** accumulated records for the next batch */
    private Map<String, List<WriteRequest>> batchUnderProcess;

    /** the size of the next batch */
    private int numberOfRecords = 0;

    /** A queue for the requests. This queue is blocking. */
    private final BlockingQueue<BatchRequest> queue = new LinkedBlockingQueue<>();

    public DynamoDBBatchProcessor(final FlinkDynamoDBClientBuilder flinkDynamoDBClientBuilder,
                                  final int maxConcurrentRequests,
                                  final int batchSize,
                                  final DynamoDBBatchProcessor.Listener listener) {
        Preconditions.checkNotNull(flinkDynamoDBClientBuilder, "amazonDynamoDBWBuilder must not be null");
        this.flinkDynamoDBClientBuilder = flinkDynamoDBClientBuilder;
        final ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(corePoolSize, corePoolSize, 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        listeningExecutorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
        this.listener = listener;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.batchSize = batchSize;
        batchUnderProcess = new HashMap<>(batchSize);
    }

    public void open() {
        if (closed) {
            throw new RuntimeException("Writer is closed");
        }
        this.dynamoDbClient = flinkDynamoDBClientBuilder.build();
        this.semaphore = new Semaphore(maxConcurrentRequests);
        callback = new FutureCallback<BatchResponse>() {
            @Override
            public void onSuccess(@Nullable BatchResponse out) {
                semaphore.release();
                listener.onSuccess(out);
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
                listener.onFailure(t);
            }
        };
        executorService.execute(() -> {
            while (!closed) {
                try {
                    BatchRequest batchRequest = queue.take();
                    try {
                        semaphore.acquire();
                    } catch (InterruptedException e) {
                        semaphore.release();
                        Thread.currentThread().interrupt();
                    }
                    Futures.addCallback(batchWrite(batchRequest), callback, MoreExecutors.directExecutor());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /**
     * add a DynamoDB request. accumulate the requests until batchSize, then promote to the queue
     * @param dynamoDBWriteRequest a single write request to DynamoDB
     */
    public void add(final DynamoDBWriteRequest dynamoDBWriteRequest) {
        numberOfRecords++;
        String tableName = dynamoDBWriteRequest.getTableName();
        final List<WriteRequest> writeRequests = batchUnderProcess.computeIfAbsent(tableName,
                k -> new ArrayList<>());
        writeRequests.add(dynamoDBWriteRequest.getWriteRequest());
        if (numberOfRecords >= batchSize) {
            promoteBatch();
        }
    }

    private void promoteBatch() {
        queue.offer(new BatchRequest(batchUnderProcess, numberOfRecords));
        batchUnderProcess = new HashMap<>(batchSize);
        numberOfRecords = 0;
    }

    /**
     * If there are records that are not added, promote them
     */
    public void flush() {
        if (numberOfRecords > 0) {
            promoteBatch();
        }
    }

    // currently protected to allow overriding for testing. should be extracted from this class.
    protected ListenableFuture<BatchResponse> batchWrite(final BatchRequest batchRequest) {
        return listeningExecutorService.submit(() -> {
            final BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    .requestItems(batchRequest.getBatch())
                    .build();
            boolean retry = false;
            int retries = 0;
            Throwable t = null;
            while (!retry && retries < 3) {
                t = null;
                try {
                    final BatchWriteItemResponse batchWriteItemResponse = dynamoDbClient.batchWriteItem(batchWriteItemRequest);
                    if (batchWriteItemResponse.hasUnprocessedItems()) {
                        retry = true;
                        batchWriteItemRequest.toBuilder().requestItems(batchWriteItemResponse.unprocessedItems());
                    } else {
                        retry = false;
                    }
                } catch (ResourceNotFoundException e) {
                    throw new IOException("Resource not found while inserting to dynamodb. do not retry", e);
                } catch (Exception e) {
                    t = e;
                    retry = true;
                }
                if (retry) {
                    try {
                        // exponential backoff using jitter
                        // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
                        long jitter = ThreadLocalRandom.current()
                                .nextLong(0, (long) Math.pow(2, retries) * 100);
                        Thread.sleep(jitter);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", e);
                    }
                }
                retries++;
            }
            if (retry && t != null) {
                throw new IOException("Error in batch insert after retries");
            }
            return BatchResponse.success(batchRequest.getBatchSize());
        });

    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
        executorService.shutdown();
    }

    @VisibleForTesting
    int getAvailablePermits() {
        return semaphore.availablePermits();
    }

    @VisibleForTesting
    BlockingQueue<BatchRequest> getQueue() {
        return queue;
    }

}
