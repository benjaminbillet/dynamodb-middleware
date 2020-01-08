package fr.benjaminbillet.dynamo;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import fr.benjaminbillet.dynamo.schema.DynamoSchema;

import java.util.UUID;

public class RunCreateGroupAndMembersTransaction {
  public static void main(String[] args) {
    AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(new ProfileCredentialsProvider("uat"))
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://dynamodb.ap-southeast-1.amazonaws.com", "ap-southeast-1"))
      .build();
    amazonDynamoDB.listTables().getTableNames().stream().forEach(System.out::println);

    DynamoSchema schema = RunSchemaBuilder.getSchema();

    GroupRepository groupRepository = new GroupRepository(amazonDynamoDB, schema);

    Group group = makeGroup();
    groupRepository.createGroupAndMembers(group, makeMember(group), makeMember(group));

    amazonDynamoDB.shutdown();
  }

  public static Group makeGroup() {
    String id = UUID.randomUUID().toString();
    Group group = new Group();
    group.setGroupId("groupId:" + id);
    group.setInviteLink("inviteLink:" + id);
    group.setStreamId("streamId:" + id);
    return group;
  }

  public static GroupMember makeMember(Group group) {
    GroupMember member = new GroupMember();
    member.setGroupId(group.getGroupId());
    member.setUserId("userId:" + UUID.randomUUID().toString());
    member.setJoinTime(null);
    return member;
  }
}
