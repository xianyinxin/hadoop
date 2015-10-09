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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * The request sent by the {@code ApplicationMaster} to {@code ResourceManager}
 * when register the {@code ApplicationMaster}'s notification address.
 * <p>
 * The registration includes details such as:
 * <ul>
 *   <li>Hostname on which the AM is running.</li>
 *   <li>Notification Port</li>
 * </ul>
 *
 * @see ApplicationMasterProtocol#registerNotificationAddress(RegisterNotificationAddressRequest)
 */
public abstract class RegisterNotificationAddressRequest {

  /**
   * Create a new instance of <code>RegisterNotificationAddressRequest</code>.
   *
   * @param host where the <code>ApplicationMaster</code> is running
   * @param port on which the <code>ApplicationMaster</code> is receiving
   *             notifications
   * @return the new instance of <code>RegisterNotificationAddressRequest</code>
   */
  @Public
  @Stable
  public static RegisterNotificationAddressRequest newInstance(String host,
                                                             int port) {
    RegisterNotificationAddressRequest request =
        Records.newRecord(RegisterNotificationAddressRequest.class);
    request.setHost(host);
    request.setNotificationPort(port);
    return request;
  }

  /**
   * Get the <em>host</em> on which the <code>ApplicationMaster</code> is
   * running.
   * @return <em>host</em> on which the <code>ApplicationMaster</code> is running
   */
  @Public
  @Stable
  public abstract String getHost();

  /**
   * Set the <em>host</em> on which the <code>ApplicationMaster</code> is
   * running.
   * @param host <em>host</em> on which the <code>ApplicationMaster</code>
   *             is running
   */
  @Public
  @Stable
  public abstract void setHost(String host);

  /**
   * Get the <em>notification port</em> on which the {@code ApplicationMaster} is
   * receiving notifications.
   * @return the <em>notification port</em> on which the {@code ApplicationMaster}
   *         is receiving notifications
   */
  @Public
  @Stable
  public abstract int getNotificationPort();

  /**
   * Set the <em>notification port</em> on which the {@code ApplicationMaster} is
   * receiving notifications.
   * @param port <em>notification port</em> on which the {@code ApplicationMaster}
   *             is receiving notifications
   */
  @Public
  @Stable
  public abstract void setNotificationPort(int port);
}
