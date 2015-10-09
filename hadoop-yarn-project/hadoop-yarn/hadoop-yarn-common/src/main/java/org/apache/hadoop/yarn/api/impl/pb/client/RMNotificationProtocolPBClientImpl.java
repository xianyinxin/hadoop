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

package org.apache.hadoop.yarn.api.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.RMNotificationProtocol.NotificationProto;
import org.apache.hadoop.yarn.api.RMNotificationProtocol;
import org.apache.hadoop.yarn.api.RMNotificationProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NotificationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.NotificationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.NotificationResponsePBImpl;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public class RMNotificationProtocolPBClientImpl implements RMNotificationProtocol, Closeable{

  private RMNotificationProtocolPB proxy;

  public RMNotificationProtocolPBClientImpl(long clientVersion, InetSocketAddress addr,
  Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, RMNotificationProtocolPB.class, ProtobufRpcEngine.class);
    proxy =
        (RMNotificationProtocolPB) RPC.getProxy(RMNotificationProtocolPB.class, clientVersion,
            addr, conf);
  }

  @Override
  public void close() throws IOException {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public NotificationResponse handleRMNotification(NotificationRequest notification)
      throws YarnException, IOException {
    NotificationProto requestProto = ((NotificationRequestPBImpl) notification).getProto();
    try {
      return new NotificationResponsePBImpl(proxy.handleRMNotification(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
