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

package org.apache.hadoop.yarn.server.resourcemanager.notificationsmanager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.RMNotificationProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RMClientProxy<T> {
  private static final Log LOG = LogFactory.getLog(RMClientProxy.class);
  private static final long CLIENT_CONNECT_MAX_WAIT_MS = 1000;
  private static final long CLIENT_CONNECT_RETRY_INTERVAL_MS = 1000;

  private interface ClientProtocols extends RMNotificationProtocol {}

  private static void checkAllowedProtocols(Class<?> protocol) {
    Preconditions.checkArgument(
        protocol.isAssignableFrom(ClientProtocols.class),
        "This protocol is not supported.");
  }

  public static <T> T createClientProxy(final Configuration configuration,
      final Class<T> protocol, InetSocketAddress clientAddress) throws IOException {
    YarnConfiguration conf = (configuration instanceof YarnConfiguration)
        ? (YarnConfiguration) configuration
        : new YarnConfiguration(configuration);
    checkAllowedProtocols(protocol);
    RetryPolicy retryPolicy = createRetryPolicy(conf);
    LOG.info("Connecting to Client at " + clientAddress);
    T proxy = RMClientProxy.<T>getProxy(conf, protocol, clientAddress);
    return (T) RetryProxy.create(protocol, proxy, retryPolicy);
  }

  static <T> T getProxy(final Configuration conf,
                        final Class<T> protocol, final InetSocketAddress clientAddress)
      throws IOException {
    return UserGroupInformation.getCurrentUser().doAs(
        new PrivilegedAction<T>() {
          @Override
          public T run() {
            return (T) YarnRPC.create(conf).getProxy(protocol, clientAddress, conf);
          }
        });
  }

  /**
   * Create retry policy
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  public static RetryPolicy createRetryPolicy(Configuration conf) {
    long rmConnectWaitMS = CLIENT_CONNECT_MAX_WAIT_MS;
    long rmConnectionRetryIntervalMS = CLIENT_CONNECT_RETRY_INTERVAL_MS;

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumTimeWithFixedSleep(rmConnectWaitMS,
            rmConnectionRetryIntervalMS, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<Class<? extends Exception>, RetryPolicy>();

    exceptionToPolicyMap.put(EOFException.class, retryPolicy);
    exceptionToPolicyMap.put(ConnectException.class, retryPolicy);
    exceptionToPolicyMap.put(NoRouteToHostException.class, retryPolicy);
    exceptionToPolicyMap.put(UnknownHostException.class, retryPolicy);
    exceptionToPolicyMap.put(ConnectTimeoutException.class, retryPolicy);
    exceptionToPolicyMap.put(RetriableException.class, retryPolicy);
    exceptionToPolicyMap.put(SocketException.class, retryPolicy);

    return RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
  }
}
