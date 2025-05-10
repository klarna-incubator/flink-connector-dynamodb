package com.klarna.flink.connectors.dynamodb.integration;

import com.klarna.flink.connectors.dynamodb.FlinkDynamoDBClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;

/**
 * Builder of an DynamoDB instance pointing to an overridden endpoint
 */
public class LocalStackDynamoDBClientBuilder implements FlinkDynamoDBClientBuilder {

    private final static String TEST_LOCAL_AWS_ACCESS_KEY_ID = "testAccessKeyId";
    private final static String TEST_LOCAL_AWS_SECRET_ACCESS_KEY = "testSecretAccessKey";
    public final int dynamodbPort;

    public LocalStackDynamoDBClientBuilder(int dynamodbPort) {
        this.dynamodbPort = dynamodbPort;
    }

    @Override
    public DynamoDbClient build() {
        return DynamoDbClient
                .builder()
                .endpointOverride(URI.create("http://localhost:" + dynamodbPort))
                .region(Region.EU_WEST_1)
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(
                                        TEST_LOCAL_AWS_ACCESS_KEY_ID,
                                        TEST_LOCAL_AWS_SECRET_ACCESS_KEY
                                )
                        )
                )
                .build();
    }
}
