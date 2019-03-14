package kz.greetgo.kafka_old.core.model;

public class GroupIdInstanceSizeOffset {
  public GroupIdInstance groupIdInstance;
  public SizeOffset sizeOffset;

  public GroupIdInstanceSizeOffset() {
  }

  public GroupIdInstanceSizeOffset(GroupIdInstance groupIdInstance, SizeOffset sizeOffset) {
    this.groupIdInstance = groupIdInstance;
    this.sizeOffset = sizeOffset;
  }

  @Override
  public String toString() {
    return "GroupIdInstanceSizeOffset{" +
        "groupIdInstance=" + groupIdInstance +
        ", sizeOffset=" + sizeOffset +
        '}';
  }
}
