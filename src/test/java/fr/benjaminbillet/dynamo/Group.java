package fr.benjaminbillet.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import lombok.Data;

@Data
public class Group extends DynamoDocument {
  private String groupId;
  private String inviteLink;
  private String streamId;

  public Group() {
    super();
  }

  public Group(AttributeMap map) {
    super(map);
    this.groupId = map.getString("groupId");
    this.inviteLink = map.getString("inviteLink");
    this.streamId = map.getString("streamId");
  }

  public static String deriveHashKey(String groupId) {
    return "GROUP#" + groupId;
  }

  public static String deriveRangeKey(String streamId) {
    return "GROUP#streamId:" + streamId;
  }

  @Override
  public AttributeMap toAttributeMap() {
    AttributeMap map = super.toAttributeMap();
    map.put("groupId", new AttributeValue(getGroupId()));
    map.put("inviteLink", new AttributeValue(getInviteLink()));
    map.put("streamId", new AttributeValue(getStreamId()));
    return map;
  }

  public void setGroupId(String groupId) {
    setHashKey(deriveHashKey(groupId));
    this.groupId = groupId;
  }

  public void setStreamId(String streamId) {
    setRangeKey(deriveRangeKey(streamId));
    this.streamId = streamId;
  }
}
