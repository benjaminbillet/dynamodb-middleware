package fr.benjaminbillet.dynamo;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import fr.benjaminbillet.dynamo.pagination.DocumentPage;
import fr.benjaminbillet.dynamo.pagination.Pageable;
import fr.benjaminbillet.dynamo.pagination.Traversal;
import fr.benjaminbillet.dynamo.schema.DynamoSchema;

import java.util.UUID;

public class RunPaginatedRetrieve {
  public static void main(String[] args) {
    AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(new ProfileCredentialsProvider("uat"))
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://dynamodb.ap-southeast-1.amazonaws.com", "ap-southeast-1"))
      .build();
    amazonDynamoDB.listTables().getTableNames().stream().forEach(System.out::println);

    DynamoSchema schema = RunSchemaBuilder.getSchema();

    GroupMemberRepository groupMemberRepository = new GroupMemberRepository(amazonDynamoDB, schema);

    String userId = "1000";
    // IntStream.range(0, 8).forEach(x -> groupMemberRepository.save(makeMember(userId)));

    Pageable nextPage = Pageable.first(5, Traversal.ASC);
    Pageable previousPage = null;
    DocumentPage<GroupMember> members = null;

    // iterate pages forward
    members = groupMemberRepository.findByUserId(userId, nextPage);
    System.out.println(members.size());
    System.out.println(members.asList());
    System.out.println(members.hasNext());

    nextPage = members.nextPage();
    members = groupMemberRepository.findByUserId(userId, nextPage);
    System.out.println(members.size());
    System.out.println(members.asList());
    System.out.println(members.hasNext());

    // iterate pages backward
    previousPage = members.previousPage();
    members = groupMemberRepository.findByUserId(userId, previousPage);
    System.out.println(members.size());
    System.out.println(members.asList());
    System.out.println(members.hasPrevious());

    previousPage = members.previousPage();
    members = groupMemberRepository.findByUserId(userId, previousPage);
    System.out.println(members.size());
    System.out.println(members.asList());
    System.out.println(members.hasPrevious());

    // reverse the dataset traversal
    nextPage = Pageable.first(5, Traversal.DESC);

    // iterate pages forward
    members = groupMemberRepository.findByUserId(userId, nextPage);
    System.out.println(members.size());
    System.out.println(members.asList());
    System.out.println(members.hasNext());

    nextPage = members.nextPage();
    members = groupMemberRepository.findByUserId(userId, nextPage);
    System.out.println(members.size());
    System.out.println(members.asList());
    System.out.println(members.hasNext());

    // iterate pages backward
    previousPage = members.previousPage();
    members = groupMemberRepository.findByUserId(userId, previousPage);
    System.out.println(members.size());
    System.out.println(members.asList());
    System.out.println(members.hasPrevious());

    previousPage = members.previousPage();
    members = groupMemberRepository.findByUserId(userId, previousPage);
    System.out.println(members.size());
    System.out.println(members.asList());
    System.out.println(members.hasPrevious());

    amazonDynamoDB.shutdown();
  }

  public static GroupMember makeMember(String userId) {
    GroupMember member = new GroupMember();
    member.setGroupId("groupId:" + UUID.randomUUID().toString());
    member.setUserId(userId);
    member.setJoinTime(null);
    return member;
  }
}
