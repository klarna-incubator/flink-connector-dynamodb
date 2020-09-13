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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DynamoDBBatchProcessor {

    /**
     * Listener for batch insert operation
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

    private AmazonDynamoDB amazonDynamoDB;

    private static final int corePoolSize = Runtime.getRuntime().availableProcessors() * (1 + 40/2);

    private final ListeningExecutorService listeningExecutorService;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private Semaphore semaphore;

    private transient volatile boolean closed = false;

    private Listener listener;

    private FutureCallback<BatchResponse> callback;

    private final AmazonDynamoDBBuilder amazonDynamoDBBuilder;

    private final int maxConcurrentRequests;

    private final int batchSize;

    private Map<String, List<WriteRequest>> batchUnderProcess;

    private int numberOfRecords = 0;

    private final LinkedBlockingQueue<BatchRequest> queue = new LinkedBlockingQueue<>();

    public DynamoDBBatchProcessor(final AmazonDynamoDBBuilder amazonDynamoDBBuilder,
                                  final int maxConcurrentRequests,
                                  final int batchSize,
                                  final DynamoDBBatchProcessor.Listener listener) {
        Preconditions.checkNotNull(amazonDynamoDBBuilder, "amazonDynamoDBWBuilder must not be null");
        this.amazonDynamoDBBuilder = amazonDynamoDBBuilder;
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
        this.amazonDynamoDB = amazonDynamoDBBuilder.build();
        this.semaphore = new Semaphore(maxConcurrentRequests);
        callback = new FutureCallback<BatchResponse>() {
            @Override
            public void onSuccess(@Nullable BatchResponse out) {
                listener.onSuccess(out);
                semaphore.release();
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
                semaphore.release();
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
                    Futures.addCallback(batchWrite(batchRequest), callback);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    public void add(final DynamoDBWriteRequest dynamoDBWriteRequest) {
        numberOfRecords++;
        String tableName = dynamoDBWriteRequest.getTableName();
        final List<WriteRequest> writeRequests = batchUnderProcess.computeIfAbsent(tableName,
                k -> new ArrayList<>());
        writeRequests.add(dynamoDBWriteRequest.getWriteRequest());
        if (numberOfRecords >= batchSize) {
            promote();
        }
    }

    private void promote() {
        queue.offer(new BatchRequest(batchUnderProcess, numberOfRecords));
        batchUnderProcess = new HashMap<>(batchSize);
        numberOfRecords = 0;
    }

    public void flush() {
        if (numberOfRecords > 0) {
            promote();
        }
    }

    protected ListenableFuture<BatchResponse> batchWrite(final BatchRequest batchRequest) {
        return listeningExecutorService.submit(() -> {
            final BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
            batchWriteItemRequest.withRequestItems(batchRequest.getBatch());
            boolean retry = false;
            int retries = 0;
            Throwable t = null;
            while (!retry && retries < 3) {
                t = null;
                try {
                    final BatchWriteItemResult batchWriteItemResult = amazonDynamoDB.batchWriteItem(batchWriteItemRequest);
                    if (!batchWriteItemResult.getUnprocessedItems().isEmpty()) {
                        retry = true;
                        batchWriteItemRequest.withRequestItems(batchWriteItemResult.getUnprocessedItems());
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
        closed = true;
        if (amazonDynamoDB != null) {
            amazonDynamoDB.shutdown();
        }
        executorService.shutdown();
    }

    @VisibleForTesting
    int getAvailablePermits() {
        return semaphore.availablePermits();
    }

}
