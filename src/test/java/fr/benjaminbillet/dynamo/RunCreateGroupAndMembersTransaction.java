package fr.benjaminbillet.dynamo;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import fr.benjaminbillet.dynamo.schema.DynamoKeyDefinition;
import fr.benjaminbillet.dynamo.schema.DynamoSchema;
import fr.benjaminbillet.dynamo.schema.GlobalSecondaryKeyDefinition;
import fr.benjaminbillet.dynamo.schema.PrimaryKeyDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static fr.benjaminbillet.dynamo.DynamoConstants.HASH_KEY;
import static fr.benjaminbillet.dynamo.DynamoConstants.RANGE_KEY;

public class RunCreateGroupAndMembersTransaction {
  public static void main(String[] args) {
    AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(new ProfileCredentialsProvider("uat"))
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://dynamodb.ap-southeast-1.amazonaws.com", "ap-southeast-1"))
      .build();
    amazonDynamoDB.listTables().getTableNames().stream().forEach(System.out::println);

    Map<String, DynamoKeyDefinition> secondaryKeys = new HashMap<>();
    secondaryKeys.put("streamIdIndex", new GlobalSecondaryKeyDefinition("streamId"));
    DynamoSchema schema = DynamoSchema.builder()
      .tableName("TestSingleTable")
      .primaryKey(new PrimaryKeyDefinition(HASH_KEY, RANGE_KEY))
      .secondaryKeys(secondaryKeys)
      .build();

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
    member.setUserId(new Random().nextLong());
    member.setJoinTime(null);
    return member;
  }
}
