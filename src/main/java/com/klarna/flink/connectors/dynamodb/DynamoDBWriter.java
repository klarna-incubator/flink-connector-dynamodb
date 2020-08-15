package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DynamoDBWriter {

    private AmazonDynamoDB amazonDynamoDB;
    private static final int corePoolSize = Runtime.getRuntime().availableProcessors() * (1 + 40/2);
    private final ListeningExecutorService listeningExecutorService;

    public DynamoDBWriter(final AmazonDynamoDB amazonDynamoDB) {
        Preconditions.checkNotNull(amazonDynamoDB, "amazonDynamoDB must not be null");
        final ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(corePoolSize, corePoolSize, 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        listeningExecutorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public ListenableFuture<BatchResponse> batchWrite(final BatchRequest batchRequest) {
        return listeningExecutorService.submit(new Callable<BatchResponse>() {
            @Override
            public BatchResponse call() throws Exception {
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
                    return new BatchResponse(false, t);
                }
                return new BatchResponse(true, null);
            }
        });

    }

    public void close() {
        if (amazonDynamoDB != null) {
            amazonDynamoDB.shutdown();
        }
    }

}
