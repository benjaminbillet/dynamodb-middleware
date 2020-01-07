package fr.benjaminbillet.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import fr.benjaminbillet.dynamo.schema.DynamoSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static fr.benjaminbillet.dynamo.DynamoConstants.HASH_KEY;
import static fr.benjaminbillet.dynamo.DynamoConstants.RANGE_KEY;

public class GroupRepository extends AbstractDynamoRepository {
  public GroupRepository(AmazonDynamoDB amazonDynamoDB, DynamoSchema schema) {
    super(amazonDynamoDB, schema);
  }

  public Optional<Group> findById(String groupId) {
    return super.findByHashKey(Group.deriveHashKey(groupId), Group::new).stream().findFirst();
  }

  public Optional<Group> findByStreamId(String streamId) {
    return super.findBySecondaryHashKey("streamIdIndex", streamId, Group::new)
      .stream()
      .findFirst()
      .map(g -> findByPrimaryKey(g.getHashKey(), g.getRangeKey(), Group::new).get());
  }

  public void save(Group group) {
    super.save(group);
  }

  public void createGroupAndMembers(Group group, GroupMember member1, GroupMember member2) {
    Put putGroup = new Put()
      .withTableName(schema.getTableName())
      .withItem(group.toAttributeMap());
    Put putMember1 = new Put()
      .withTableName(schema.getTableName())
      .withItem(member1.toAttributeMap());
    Put putMember2 = new Put()
      .withTableName(schema.getTableName())
      .withItem(member2.toAttributeMap());

    amazonDynamoDB.transactWriteItems(new TransactWriteItemsRequest()
      .withTransactItems(Arrays.asList(
        new TransactWriteItem().withPut(putGroup),
        new TransactWriteItem().withPut(putMember1),
        new TransactWriteItem().withPut(putMember2))));
  }

  public void deleteGroupAndMembers(Group group) {
    Map<String, AttributeValue> groupKeyAttributes = new HashMap<>();
    groupKeyAttributes.put(HASH_KEY, new AttributeValue().withS(group.getHashKey()));
    groupKeyAttributes.put(RANGE_KEY, new AttributeValue().withS(group.getRangeKey()));

    Delete deleteGroup = new Delete()
      .withTableName(schema.getTableName())
      .withKey(groupKeyAttributes);
    Delete deleteMembers = new Delete()
      .withTableName(schema.getTableName())
      .withConditionExpression("#pk = :pkValue")
      .withExpressionAttributeNames(Collections.singletonMap("#pk", HASH_KEY))
      .withExpressionAttributeValues(Collections.singletonMap(":pkValue", new AttributeValue(GroupMember.deriveHashKey(group.getGroupId()))));

    amazonDynamoDB.transactWriteItems(new TransactWriteItemsRequest()
      .withTransactItems(Arrays.asList(
        new TransactWriteItem().withDelete(deleteGroup),
        new TransactWriteItem().withDelete(deleteMembers))));
  }

  public void deleteGroupAndMembers(String groupId) {
    Delete deleteGroup = new Delete()
      .withTableName(schema.getTableName())
      .withConditionExpression("#pk = :pkValue")
      .withExpressionAttributeNames(Collections.singletonMap("#pk", HASH_KEY))
      .withExpressionAttributeValues(Collections.singletonMap(":pkValue", new AttributeValue(Group.deriveHashKey(groupId))));

    Delete deleteMembers = new Delete()
      .withTableName(schema.getTableName())
      .withConditionExpression("#pk = :pkValue")
      .withExpressionAttributeNames(Collections.singletonMap("#pk", HASH_KEY))
      .withExpressionAttributeValues(Collections.singletonMap(":pkValue", new AttributeValue(GroupMember.deriveHashKey(groupId))));

    amazonDynamoDB.transactWriteItems(new TransactWriteItemsRequest()
      .withTransactItems(Arrays.asList(
        new TransactWriteItem().withDelete(deleteGroup),
        new TransactWriteItem().withDelete(deleteMembers))));
  }
}
