/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterNotificationAddressRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterNotificationAddressRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterNotificationAddressRequestProtoOrBuilder;

public class RegisterNotificationAddressRequestPBImpl extends
    RegisterNotificationAddressRequest{
  RegisterNotificationAddressRequestProto proto =
      RegisterNotificationAddressRequestProto.getDefaultInstance();
  RegisterNotificationAddressRequestProto.Builder builder = null;
  boolean viaProto = false;

  public RegisterNotificationAddressRequestPBImpl() {
    builder = RegisterNotificationAddressRequestProto.newBuilder();
  }

  public RegisterNotificationAddressRequestPBImpl(RegisterNotificationAddressRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public RegisterNotificationAddressRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterNotificationAddressRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getHost() {
    RegisterNotificationAddressRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getHost());
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    builder.setHost(host);
  }

  @Override
  public int getNotificationPort() {
    RegisterNotificationAddressRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNotificationPort());
  }

  @Override
  public void setNotificationPort(int port) {
    maybeInitBuilder();
    builder.setNotificationPort(port);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
