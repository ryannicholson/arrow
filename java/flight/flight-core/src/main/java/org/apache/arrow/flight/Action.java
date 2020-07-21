/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import org.apache.arrow.flight.impl.Flight;

import com.google.protobuf.Any;

/**
 * An opaque action for the service to perform.
 *
 * <p>This is a POJO wrapper around the message of the same name in Flight.proto.
 */
public class Action {
  private static final Any NO_VALUE = Any.getDefaultInstance();

  private final String type;
  private final Any body;

  public Action(String type) {
    this(type, NO_VALUE);
  }

  public Action(String type, Any body) {
    this.type = type;
    this.body = body;
  }

  Action(Flight.Action action) {
    this(action.getType(), action.getBody());
  }

  public String getType() {
    return type;
  }

  public Any getBody() {
    return body;
  }

  Flight.Action toProtocol() {
    return Flight.Action.newBuilder()
        .setType(getType())
        .setBody(getBody())
        .build();
  }
}
