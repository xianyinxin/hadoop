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
 * The response sent by the {@code ResourceManager} to {@code ApplicationMaster}
 * on registration.
 * <p>
 * The registration includes details such as:
 * <ul>
 *   <li>Hostname on which the AM is running.</li>
 *   <li>Notification Port</li>
 * </ul>
 *
 * @see ApplicationMasterProtocol#registerNotificationAddress(RegisterNotificationAddressRequest)
 */
public abstract  class RegisterNotificationAddressResponse {

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
  public static RegisterNotificationAddressResponse newInstance(
      boolean success, String diagnostics) {
    RegisterNotificationAddressResponse request =
        Records.newRecord(RegisterNotificationAddressResponse.class);
    request.setIfSuccess(success);
    request.setDiagnostics(diagnostics);
    return request;
  }

  /**
   * Set if <em>registration</em> succeed.
   */
  @Public
  @Stable
  public abstract void setIfSuccess(boolean success);

  /**
   * Get if <em>registration</em> succeed.
   */
  @Public
  @Stable
  public abstract boolean getIfSuccess();

  /**
   * Set the <em>diagnostics</em> of the <em>registration</em>.
   */
  @Public
  @Stable
  public abstract void setDiagnostics(String diagnostics);

  /**
   * Get the <em>diagnostics</em> of the <em>registration</em>.
   */
  @Public
  @Stable
  public abstract String getDiagnostics();
}
