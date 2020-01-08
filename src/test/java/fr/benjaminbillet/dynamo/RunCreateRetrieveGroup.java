package fr.benjaminbillet.dynamo;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import fr.benjaminbillet.dynamo.schema.DynamoSchema;

public class RunCreateRetrieveGroup {
  public static void main(String[] args) {
    AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(new ProfileCredentialsProvider("uat"))
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://dynamodb.ap-southeast-1.amazonaws.com", "ap-southeast-1"))
      .build();
    amazonDynamoDB.listTables().getTableNames().stream().forEach(System.out::println);

    DynamoSchema schema = RunSchemaBuilder.getSchema();

    GroupRepository groupRepository = new GroupRepository(amazonDynamoDB, schema);

    Group group = new Group();
    group.setGroupId("groupId");
    group.setInviteLink("inviteLink");
    group.setStreamId("streamId");

    groupRepository.save(group);

    Group group1 = groupRepository.findById("groupId").get();
    Group group2 = groupRepository.findByStreamId("streamId").get();

    // IntStream.range(0, 10).forEach(x -> groupRepository.save(makeGroup()));

    amazonDynamoDB.shutdown();
  }
}
