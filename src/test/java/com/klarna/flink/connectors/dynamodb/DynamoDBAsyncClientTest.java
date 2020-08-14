package com.klarna.flink.connectors.dynamodb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(5)
public class DynamoDBAsyncClientTest {

    private final static String TABLE_NAME = "TestTable";

    private static DynamoDBProxyServer server;
    private static AmazonDynamoDB amazonDynamoDB;

    @BeforeAll
    public static void setupClass() throws Exception {
        System.setProperty("sqlite4java.library.path", "native-libs");
        String port = "8000";
        server = ServerRunner.createServerFromCommandLineArgs(
                new String[]{"-inMemory", "-port", port});
        server.start();
        amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accesskey", "secretkey")))
                .build();
    }

    @BeforeEach
    void beforeEach() {
        amazonDynamoDB.createTable(new CreateTableRequest()
                .withTableName(TABLE_NAME)
                .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
                .withKeySchema(new KeySchemaElement("item_id", KeyType.HASH))
                .withAttributeDefinitions(Collections.singletonList(new AttributeDefinition("item_id", ScalarAttributeType.S))));
    }

    @AfterEach
    void afterEach() {
        amazonDynamoDB.deleteTable(TABLE_NAME);
    }

    @AfterAll
    public static void teardownClass() throws Exception {
        server.stop();
    }

    @Test
    void testSuccessfulPut() {
        DynamoDBAsyncClient dynamoDBAsyncClient = new DynamoDBAsyncClient(amazonDynamoDB);
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("item_id", new AttributeValue("1"));
        item.put("name", new AttributeValue("name1"));
        final PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(TABLE_NAME)
                .withItem(item);
        ListenableFuture<PutItemResult> put = dynamoDBAsyncClient.put(putItemRequest);
        assertDoesNotThrow(() -> put.get());
        GetItemResult getItemResult = amazonDynamoDB.getItem(new GetItemRequest()
                .withTableName(TABLE_NAME)
                .addKeyEntry("item_id", new AttributeValue("1")));
        assertNotNull(getItemResult.getItem());
        assertEquals("name1", getItemResult.getItem().get("name").getS());
    }

    @Test
    void testFailedPutWhenTableNameIsWrong() {
        DynamoDBAsyncClient dynamoDBAsyncClient = new DynamoDBAsyncClient(amazonDynamoDB);
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("item_id", new AttributeValue("1"));
        item.put("name", new AttributeValue("name1"));
        final PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(TABLE_NAME + 1)
                .withItem(item);
        ListenableFuture<PutItemResult> put = dynamoDBAsyncClient.put(putItemRequest);
        Exception e = assertThrows(Exception.class, () -> put.get());
        assertEquals(ResourceNotFoundException.class, e.getCause().getClass());
    }

    @Test
    void testErrorWhenDynamoDBIsNull() {
        assertThrows(NullPointerException.class, () -> new DynamoDBAsyncClient(null), "amazonDynamoDB must not be null");
    }

}
