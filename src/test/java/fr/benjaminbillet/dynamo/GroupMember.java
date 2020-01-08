package fr.benjaminbillet.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;

import static fr.benjaminbillet.dynamo.util.AttributeUtils.dateToString;

@Data
@NoArgsConstructor
public class GroupMember extends DynamoDocument {
  private String groupId;
  private String userId;
  private ZonedDateTime joinTime;

  public GroupMember(AttributeMap map) {
    super(map);
    this.groupId = map.getString("groupId");
    this.userId = map.getString("userId");
    this.joinTime = map.getDate("joinTime");
  }

  public static String deriveHashKey(String groupId) {
    return "GROUP_MEMBER#" + groupId;
  }

  public static String deriveRangeKey(String userId) {
    return "GROUP_MEMBER#userId:" + userId;
  }

  public AttributeMap toAttributeMap() {
    AttributeMap map = super.toAttributeMap();
    map.put("groupId", new AttributeValue(getGroupId()));
    map.put("userId", new AttributeValue(getUserId()));
    if (getJoinTime() != null) {
      map.put("joinTime", new AttributeValue(dateToString(getJoinTime())));
    }
    return map;
  }

  public void setGroupId(String groupId) {
    setHashKey(deriveHashKey(groupId));
    this.groupId = groupId;
  }

  public void setUserId(String userId) {
    setRangeKey(deriveRangeKey(userId));
    this.userId = userId;
  }
}
