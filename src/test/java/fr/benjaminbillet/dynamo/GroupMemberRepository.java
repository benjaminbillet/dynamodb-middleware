package fr.benjaminbillet.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import fr.benjaminbillet.dynamo.pagination.DocumentPage;
import fr.benjaminbillet.dynamo.pagination.Pageable;
import fr.benjaminbillet.dynamo.schema.DynamoSchema;

import java.util.List;
import java.util.Optional;

public class GroupMemberRepository extends AbstractDynamoRepository {
  public GroupMemberRepository(AmazonDynamoDB amazonDynamoDB, DynamoSchema schema) {
    super(amazonDynamoDB, schema);
  }

  public Optional<GroupMember> findById(String groupId, Long userId) {
    return super.findByPrimaryKey(
      GroupMember.deriveHashKey(groupId),
      GroupMember.deriveRangeKey(userId),
      GroupMember::new);
  }

  public List<GroupMember> findByGroupId(String groupId) {
    return super.findByHashKey(groupId, GroupMember::new);
  }

  public DocumentPage<GroupMember> findByGroupId(String groupId, Pageable pageable) {
    return super.findByHashKey(groupId, pageable, GroupMember::new);
  }
}
