package com.klarna.flink.connectors.dynamodb.integration;

import com.klarna.flink.connectors.dynamodb.DynamoDBSinkConfig;
import com.klarna.flink.connectors.dynamodb.FlinkDynamoDBSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.*;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.List;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

public class IntegrationTest {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(IntegrationTest.class);

    private static final String testDynamoTable = "test-table";

    @ClassRule
    public static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.12.9.1"))
            .withServices(DYNAMODB);

    private static Integer localDynamodbPort() {
        return localstack.getMappedPort(4566);
    }

    @Before
    public void createTestTable() {

        var dynamoClient = new LocalStackDynamoDBClientBuilder(localDynamodbPort()).build();

        LOG.info("creating DynamoDB table: {}", testDynamoTable);
        dynamoClient.createTable(
                CreateTableRequest.builder()
                        .tableName(testDynamoTable)
                        .attributeDefinitions(
                                List.of(
                                        AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
                                        AttributeDefinition.builder().attributeName("SK").attributeType(ScalarAttributeType.N).build()
                                )
                        )
                        .keySchema(
                                KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build()
                        )
                        .provisionedThroughput(
                                ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build()
                        )
                        .build());
    }

    @After
    public void cleanup() {
        LOG.info("deleting DynamoDB table: {}", testDynamoTable);
        var dynamoClient = new LocalStackDynamoDBClientBuilder(localDynamodbPort()).build();
        dynamoClient.deleteTable(DeleteTableRequest.builder().tableName(testDynamoTable).build());
    }

    @Test
    public void testProduceSomeSimpleItemsInOneBatch() throws Exception {

        // 5 records as part of one single batch of 25 item
        var inputTestData = List.of(
                new SensorEventData("sensor01", 1500000000L, 11L, 12L, 13L),
                new SensorEventData("sensor02", 1500000000L, 21L, 22L, 23L),
                new SensorEventData("sensor03", 1500000000L, 31L, 32L, 33L),
                new SensorEventData("sensor04", 1500000000L, 41L, 42L, 43L),
                new SensorEventData("sensor05", 1500000000L, 51L, 52L, 53L)
        );

        // test Flink logic: just passing sensor events straight to the DynamoDB sink
        var streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        var testInputStream = streamingEnv.fromCollection(inputTestData);
        var dynamoDbSink = new FlinkDynamoDBSink<SensorEventData>(
                new LocalStackDynamoDBClientBuilder(localDynamodbPort()),
                testDynamoTable,
                DynamoDBSinkConfig.builder()
                        .batchSize(25)
                        .queueLimit(10)
                        .build(),
                testData -> WriteRequest.builder()
                        .putRequest(SensorEventDataDynamoMapping.asDynamoPutItemRequest(testData)).build(),
                SensorEventDataDynamoMapping::dynamoKeyStr
        );
        testInputStream.addSink(dynamoDbSink);

        streamingEnv.execute();

        LOG.info("Checking stored data in DynamoDB");
        var dynamoClient = new LocalStackDynamoDBClientBuilder(localDynamodbPort()).build();
        inputTestData.forEach(
                sensorEventData ->
                        Assert.assertEquals(
                                sensorEventData,
                                SensorEventDataDynamoMapping
                                        .loadFromDynamo(sensorEventData.sensorId, sensorEventData.timestamp, dynamoClient, testDynamoTable)
                        ));
    }


    @Test
    public void testProduceSomeSimpleItemsInMultipleBatches() throws Exception {

        // 10 records as part of two batches of size 5 each
        var inputTestData = List.of(
                new SensorEventData("sensor01", 1500000000L, 11L, 12L, 13L),
                new SensorEventData("sensor02", 1500000000L, 21L, 22L, 23L),
                new SensorEventData("sensor03", 1500000000L, 31L, 32L, 33L),
                new SensorEventData("sensor04", 1500000000L, 41L, 42L, 43L),
                new SensorEventData("sensor05", 1500000000L, 51L, 52L, 53L),
                new SensorEventData("sensor06", 1500000000L, 61L, 62L, 63L),
                new SensorEventData("sensor07", 1500000000L, 71L, 72L, 73L),
                new SensorEventData("sensor08", 1500000000L, 81L, 82L, 83L),
                new SensorEventData("sensor09", 1500000000L, 91L, 92L, 93L),
                new SensorEventData("sensor10", 1500000000L, 101L, 102L, 103L)
        );

        // test Flink logic: just passing sensor events straight to the DynamoDB sink
        var streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        var testInputStream = streamingEnv.fromCollection(inputTestData);
        var dynamoDbSink = new FlinkDynamoDBSink<SensorEventData>(
                new LocalStackDynamoDBClientBuilder(localDynamodbPort()),
                testDynamoTable,
                DynamoDBSinkConfig.builder()
                        .batchSize(5)
                        .queueLimit(2)
                        .build(),
                testData -> WriteRequest.builder()
                        .putRequest(SensorEventDataDynamoMapping.asDynamoPutItemRequest(testData)).build(),
                SensorEventDataDynamoMapping::dynamoKeyStr
        );
        testInputStream.addSink(dynamoDbSink);

        streamingEnv.execute();

        LOG.info("Checking stored data in DynamoDB");
        var dynamoClient = new LocalStackDynamoDBClientBuilder(localDynamodbPort()).build();
        inputTestData.forEach(
                sensorEventData ->
                        Assert.assertEquals(
                                sensorEventData,
                                SensorEventDataDynamoMapping
                                        .loadFromDynamo(sensorEventData.sensorId, sensorEventData.timestamp, dynamoClient, testDynamoTable)
                        ));
    }

}
