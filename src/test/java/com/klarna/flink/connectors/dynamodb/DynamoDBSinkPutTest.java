package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(5)
public class DynamoDBSinkPutTest {

    @Test
    void testMapperMustNotBeNull() {
        DynamoDBBuilder dynamoDBBuilder = Mockito.mock(DynamoDBBuilder.class);

        assertThrows(NullPointerException.class, () -> new DynamoDBSinkPut<>(dynamoDBBuilder,
                DynamoDBSinkBaseConfig.builder().build(), new NoOpDynamoDBFailureHandler(), null), "dynamoDBItemRequestMapper must not be null");
    }

    @Test
    void testSuccessfulSend() throws Exception {
        DynamoDBAsyncClient dynamoDBAsyncClient = Mockito.mock(DynamoDBAsyncClient.class);
        DynamoDBBuilder dynamoDBBuilder = Mockito.mock(DynamoDBBuilder.class);
        Mockito.when(dynamoDBBuilder.getAmazonDynamoDB()).thenReturn(dynamoDBAsyncClient);
        DynamoDBItemRequestMapper<String, PutItemRequest> dynamoDBItemRequestMapper = Mockito.mock(DynamoDBItemRequestMapper.class);
        DynamoDBSinkPut<String> dynamoDBSinkPut = new DynamoDBSinkPut<>(dynamoDBBuilder,
                DynamoDBSinkBaseConfig.builder().build(), new NoOpDynamoDBFailureHandler(), dynamoDBItemRequestMapper);
        dynamoDBSinkPut.open(new Configuration());
        PutItemRequest putItemRequest = Mockito.mock(PutItemRequest.class);
        Mockito.when(dynamoDBItemRequestMapper.map(Mockito.anyString())).thenReturn(putItemRequest);
        PutItemResult putItemResult = Mockito.mock(PutItemResult.class);
        Mockito.when(dynamoDBAsyncClient.put(putItemRequest)).thenReturn(Futures.immediateFuture(putItemResult));
        ListenableFuture<PutItemResult> send = dynamoDBSinkPut.send("N/A");
        assertEquals(putItemResult, send.get());
    }

    @Test
    void testFailedSend() throws Exception {
        DynamoDBAsyncClient dynamoDBAsyncClient = Mockito.mock(DynamoDBAsyncClient.class);
        DynamoDBBuilder dynamoDBBuilder = Mockito.mock(DynamoDBBuilder.class);
        Mockito.when(dynamoDBBuilder.getAmazonDynamoDB()).thenReturn(dynamoDBAsyncClient);
        DynamoDBItemRequestMapper<String, PutItemRequest> dynamoDBItemRequestMapper = Mockito.mock(DynamoDBItemRequestMapper.class);
        DynamoDBSinkPut<String> dynamoDBSinkPut = new DynamoDBSinkPut<>(dynamoDBBuilder,
                DynamoDBSinkBaseConfig.builder().build(), new NoOpDynamoDBFailureHandler(), dynamoDBItemRequestMapper);
        dynamoDBSinkPut.open(new Configuration());
        PutItemRequest putItemRequest = Mockito.mock(PutItemRequest.class);
        Mockito.when(dynamoDBItemRequestMapper.map(Mockito.anyString())).thenReturn(putItemRequest);
        RuntimeException cause = new RuntimeException();
        Mockito.when(dynamoDBAsyncClient.put(putItemRequest)).thenReturn(Futures.immediateFailedFuture(cause));
        ListenableFuture<PutItemResult> send = dynamoDBSinkPut.send("N/A");
        Exception e = assertThrows(Exception.class, () -> send.get());
        assertEquals(cause, e.getCause());
    }

}
