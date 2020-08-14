package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DynamoDBAsyncClient {

    private AmazonDynamoDB amazonDynamoDB;
    private static final int corePoolSize = Runtime.getRuntime().availableProcessors() * (1 + 40/2);
    private final ListeningExecutorService listeningExecutorService;

    public DynamoDBAsyncClient(final AmazonDynamoDB amazonDynamoDB) {
        Preconditions.checkNotNull(amazonDynamoDB, "amazonDynamoDB must not be null");
        final ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(corePoolSize, corePoolSize, 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        listeningExecutorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public ListenableFuture<PutItemResult> put(final PutItemRequest putItemRequest) {
        return listeningExecutorService.submit(new Callable<PutItemResult>() {
           @Override
           public PutItemResult call() throws Exception {
               return amazonDynamoDB.putItem(putItemRequest);
           }
       });
    }

    public void close() {
        if (amazonDynamoDB != null) {
            amazonDynamoDB.shutdown();
        }
    }

}
