package com.klarna.flink.connectors.dynamodb.integration;

import com.klarna.flink.connectors.dynamodb.FlinkDynamoDBClientBuilder;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class LocalStackDynamoDBClientBuilder implements FlinkDynamoDBClientBuilder {

  public final int localPort;

  public static String TEST_LOCAL_AWS_ACCESS_KEY_ID = "testAccessKeyId";
  public static String TEST_LOCAL_AWS_SECRET_ACCESS_KEY = "testSecretAccessKey";

  public LocalStackDynamoDBClientBuilder(int localPort) {
    this.localPort = localPort;
  }

  @Override
  public DynamoDbClient build() {
    return DynamoDbClient
        .builder()
        .endpointOverride(URI.create("http://localhost:" + localPort))
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
