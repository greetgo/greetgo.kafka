package kz.greetgo.kafka.core.model;

public class GroupIdInstanceSizeOffset {
  public GroupIdInstance groupIdInstance;
  public SizeOffset sizeOffset;

  public GroupIdInstanceSizeOffset() {
  }

  public GroupIdInstanceSizeOffset(GroupIdInstance groupIdInstance, SizeOffset sizeOffset) {
    this.groupIdInstance = groupIdInstance;
    this.sizeOffset = sizeOffset;
  }
}
