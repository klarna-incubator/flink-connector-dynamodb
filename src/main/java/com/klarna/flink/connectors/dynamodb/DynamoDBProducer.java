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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is used to send batch requests to DynamoDB
 * records are added to the batch processor by calling the add method. The processor accumulates and promotes a batch by inserting it into a queue.
 * A background thread is polling the queue and try to acquire a permit to execute the batch request.
 *
 * This class is not synchronized. It is recommended to create separate format instances for each thread.
 * Concurrent access to this class from multiple threads must be synchronized externally.
 */
@Internal
public class DynamoDBProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBProducer.class);

    private DynamoDbClient dynamoDbClient;

    private static final int corePoolSize = Runtime.getRuntime().availableProcessors() * (1 + 40/2);

    private final ExecutorService taskExecutor;

    private final CompletionService<BatchResponse> completionService;

    /** executor service invoking the batch requests to DynamoDB */
    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("dynamodb-sink-%d")
            .build());

    /** is the processor closed */
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    /** the batch size for the batch request */
    private final int batchSize;

    /** accumulated records for the next batch */
    private Map<String, List<WriteRequest>> batchUnderProcess;

    /** the size of the next batch */
    private int numberOfRecords = 0;

    /** batchId of the current batch */
    private long batchId = 1;

    /** used to deduplicate messages by key - set of keys in the current batch */
    private Set<String> seenKeys = new HashSet<>();

    /** used to deduplicate messages by key - get a string key from the current {@link DynamoDBWriteRequest} */
    private KeySelector<WriteRequest, String> keySelector;

    /** map key is batchId and the value is the outgoing batch */
    private final Map<Long, SettableFuture<BatchResponse>> futures = new ConcurrentHashMap<>();

    /** A queue for the requests. This queue is blocking. */
    private final BlockingQueue<BatchRequest> queue = new LinkedBlockingQueue<>();

    public DynamoDBProducer(final FlinkDynamoDBClientBuilder flinkDynamoDBClientBuilder,
                            final KeySelector<WriteRequest, String> keySelector,
                            final int batchSize) {
        Preconditions.checkNotNull(flinkDynamoDBClientBuilder, "flinkDynamoDBClientBuilder must not be null");
        taskExecutor = new ThreadPoolExecutor(corePoolSize, corePoolSize, 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        this.batchSize = batchSize;
        batchUnderProcess = new HashMap<>(batchSize);
        completionService = new ExecutorCompletionService<>(taskExecutor);
        this.keySelector = keySelector;
        this.dynamoDbClient = flinkDynamoDBClientBuilder.build();
        executor.execute(() -> {
            while (!shutdown.get()) {
                try {
                    BatchRequest batchRequest = queue.take();
                    completionService.submit(batchWrite(batchRequest));
                } catch (InterruptedException e) {
                    fatalError("Failed to take batch from the queue", e);
                }
            }
        });
        executor.execute(() -> {
            while(!shutdown.get()) {
                try {
                    Future<BatchResponse> future = completionService.take();
                    BatchResponse batchResponse = future.get();
                    long id = batchResponse.getBatchId();
                    SettableFuture<BatchResponse> f = futures.remove(id);
                    if (f == null) {
                        LOG.error("Future was not found for batch id {}", id);
                        throw new RuntimeException("Future for batch id " + id + " not found");
                    }
                    if (batchResponse.isSuccessful()) {
                        f.set(batchResponse);
                    } else {
                        f.setException(new RuntimeException("Failed to execute batch", batchResponse.getThrowable()));
                    }
                } catch (InterruptedException | ExecutionException e) {
                    fatalError("Failed to retrieve future from completion service", e);
                }
            }
        });
    }

    private synchronized void fatalError(String message, Throwable t) {
        if (!shutdown.getAndSet(true)) {
            try {
                this.taskExecutor.awaitTermination(1, TimeUnit.SECONDS);
                this.executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
                LOG.error("Shutdown request could not finish gracefully. in process batches might be lost");
            }
            this.taskExecutor.shutdownNow();
            this.executor.shutdownNow();

            RuntimeException futureException;
            if (t == null) {
                futureException = new RuntimeException(message);
            } else {
                futureException = new RuntimeException(message, t);
            }

            for (Map.Entry<Long, SettableFuture<BatchResponse>> entry : futures.entrySet()) {
                entry.getValue().setException(futureException);
            }

            futures.clear();
            if (dynamoDbClient != null) {
                dynamoDbClient.close();
            }
        }
    }

    /**
     * add a DynamoDB request. accumulate the requests until batchSize, then promote to the queue
     * @param dynamoDBWriteRequest a single write request to DynamoDB
     */
    public ListenableFuture<BatchResponse> add(final DynamoDBWriteRequest dynamoDBWriteRequest) {
        if (keySelector != null) {
            try {
                String key = keySelector.getKey(dynamoDBWriteRequest.getWriteRequest());
                if (seenKeys.contains(key)) {
                    promoteBatch();
                } else {
                    seenKeys.add(key);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        numberOfRecords++;
        String tableName = dynamoDBWriteRequest.getTableName();
        final List<WriteRequest> writeRequests = batchUnderProcess.computeIfAbsent(tableName,
                k -> new ArrayList<>());
        writeRequests.add(dynamoDBWriteRequest.getWriteRequest());
        long currentBatchNumber = batchId;
        if (numberOfRecords >= batchSize) {
            promoteBatch();
        }
        return futures.computeIfAbsent(currentBatchNumber, k -> SettableFuture.create());
    }

    private void promoteBatch() {
        try {
            queue.put(new BatchRequest(batchId, batchUnderProcess, numberOfRecords));
        } catch (InterruptedException e) {
            fatalError("Failed to promote batch", e);
        }
        batchUnderProcess = new HashMap<>(batchSize);
        numberOfRecords = 0;
        batchId++;
        seenKeys.clear();
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
    protected Callable<BatchResponse> batchWrite(final BatchRequest batchRequest) {
        return () -> {
            boolean interrupted = false;
            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
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
                        batchWriteItemRequest = batchWriteItemRequest.toBuilder()
                                .requestItems(batchWriteItemResponse.unprocessedItems())
                                .build();
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
                        interrupted = true;
                        return new BatchResponse(batchRequest.getbatchId(), batchRequest.getBatchSize(), false, e);
                    } finally {
                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
                retries++;
            }
            if (retry) {
                if (t != null) {
                    return new BatchResponse(batchRequest.getbatchId(), batchRequest.getBatchSize(), false, t);
                }
                return new BatchResponse(batchRequest.getbatchId(), batchRequest.getBatchSize(), false, new RuntimeException("Max retries reached"));
            }
            return new BatchResponse(batchRequest.getbatchId(), batchRequest.getBatchSize(), true, null);
        };

    }

    public int getOutstandingRecordsCount() {
        return futures.size();
    }

    public void destroy() {
        fatalError("Destory was called", null);
    }

    @VisibleForTesting
    BlockingQueue<BatchRequest> getQueue() {
        return queue;
    }

    @VisibleForTesting
    Map<String, List<WriteRequest>> getUnderConstruction() {
        return batchUnderProcess;
    }

}
