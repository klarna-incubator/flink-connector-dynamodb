package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class DynamoDBSinkBatchWrite<IN> extends DynamoDBSinkBase<IN, BatchWriteItemResult> {

    Map<String, LinkedBlockingQueue<WriteRequest>> list;
    Map<String, List<WriteRequest>> batchesUnderProcess;

    private Semaphore semaphore = new Semaphore(10);

    protected DynamoDBSinkBatchWrite(DynamoDBBuilder builder, DynamoDBSinkBaseConfig config, DynamoDBFailureHandler failureHandler) {
        super(builder, config, failureHandler);
    }

    @Override
    protected ListenableFuture<BatchWriteItemResult> send(IN value) {
        WriteRequest writeRequest = new WriteRequest();
        semaphore.tryAcquire(1);
        writeRequest.withPutRequest(new PutRequest());
        list.get("tableName").add(writeRequest);
        if (list.get("tableName").size() >= 25) {
            client.batchWrite(batchesUnderProcess);
        }
        return null;
    }
}
