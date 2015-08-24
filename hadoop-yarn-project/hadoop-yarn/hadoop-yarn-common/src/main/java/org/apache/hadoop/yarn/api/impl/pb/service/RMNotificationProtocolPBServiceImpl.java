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

package org.apache.hadoop.yarn.api.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.RMNotificationProtocol.NotificationProto;
import org.apache.hadoop.yarn.proto.RMNotificationProtocol.NotificationResponseProto;
import org.apache.hadoop.yarn.api.RMNotificationProtocol;
import org.apache.hadoop.yarn.api.RMNotificationProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.NotificationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.NotificationResponsePBImpl;

import java.io.IOException;

public class RMNotificationProtocolPBServiceImpl implements RMNotificationProtocolPB{

  private RMNotificationProtocol real;

  public RMNotificationProtocolPBServiceImpl(RMNotificationProtocol impl) {
    real = impl;
  }
  @Override
  public NotificationResponseProto handleRMNotification(RpcController arg0, NotificationProto proto)
      throws ServiceException {
    NotificationRequestPBImpl request = new NotificationRequestPBImpl(proto);
    try {
      NotificationResponse response = real.handleRMNotification(request);
      return ((NotificationResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
