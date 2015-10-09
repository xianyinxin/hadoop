package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.proto.RMNotificationProtocol.NotificationProto;
import org.apache.hadoop.yarn.proto.RMNotificationProtocol.NotificationProtoOrBuilder;
import org.apache.hadoop.yarn.proto.RMNotificationProtocol.NotificationTypeProto;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationType;

public class NotificationRequestPBImpl extends NotificationRequest {
  NotificationProto proto = NotificationProto.getDefaultInstance();
  NotificationProto.Builder builder = null;
  boolean viaProto = false;

  private NotificationType type = null;

  public NotificationRequestPBImpl() {
    builder = NotificationProto.newBuilder();
  }

  public NotificationRequestPBImpl(NotificationProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public NotificationProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.type != null) {
      builder.setNotificationType(convertToProtoFormat(type));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NotificationProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public NotificationType getNotificationType() {
    NotificationProtoOrBuilder p = viaProto ? proto : builder;
    if (this.type != null) {
      return this.type;
    }
    if (!p.hasNotificationType()) {
      return null;
    }
    this.type = convertFromProtoFormat(p.getNotificationType());
    return this.type;
  }

  @Override
  public void setNotificationType(NotificationType type) {
    maybeInitBuilder();
    if (type == null) {
      builder.clearNotificationType();
    }
    this.type = type;
  }

  private NotificationType convertFromProtoFormat(NotificationTypeProto type) {
    return NotificationType.valueOf(type.name());
  }

  private NotificationTypeProto convertToProtoFormat(NotificationType type) {
    return NotificationTypeProto.valueOf(type.name());
  }
}
