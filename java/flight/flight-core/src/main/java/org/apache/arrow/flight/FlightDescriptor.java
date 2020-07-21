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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.util.Preconditions;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;

/**
 * An identifier for a particular set of data.  This can either be an opaque command that generates
 * the data or a static "path" to the data.  This is a POJO wrapper around the protobuf message with
 * the same name.
 */
public class FlightDescriptor {

  private static final Any NO_VALUE = Any.getDefaultInstance();

  private boolean isCmd;
  private List<String> path;
  private Any cmd;

  private FlightDescriptor(boolean isCmd, List<String> path, Any cmd) {
    super();
    this.isCmd = isCmd;
    this.path = path;
    this.cmd = cmd;
  }

  public static FlightDescriptor command(Any cmd) {
    return new FlightDescriptor(true, null, (cmd != null ? cmd : NO_VALUE));
  }

  public static FlightDescriptor path(Iterable<String> path) {
    return new FlightDescriptor(false, ImmutableList.copyOf(path), NO_VALUE);
  }

  public static FlightDescriptor path(String...path) {
    return new FlightDescriptor(false, ImmutableList.copyOf(path), NO_VALUE);
  }

  FlightDescriptor(Flight.FlightDescriptor descriptor) {
    if (descriptor.getType() == DescriptorType.CMD) {
      isCmd = true;
      cmd = descriptor.getCmd();
    } else if (descriptor.getType() == DescriptorType.PATH) {
      isCmd = false;
      path = descriptor.getPathList();
      cmd = NO_VALUE;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public boolean isCommand() {
    return isCmd;
  }

  public List<String> getPath() {
    Preconditions.checkArgument(!isCmd);
    return path;
  }

  public Any getCommand() {
    Preconditions.checkArgument(isCmd);
    return cmd;
  }

  Flight.FlightDescriptor toProtocol() {
    Flight.FlightDescriptor.Builder b = Flight.FlightDescriptor.newBuilder();

    if (isCmd) {
      return b.setType(DescriptorType.CMD).setCmd(cmd).build();
    }
    return b.setType(DescriptorType.PATH).addAllPath(path).setCmd(NO_VALUE).build();
  }

  /**
   * Get the serialized form of this protocol message.
   *
   * <p>Intended to help interoperability by allowing non-Flight services to still return Flight types.
   */
  public ByteBuffer serialize() {
    return ByteBuffer.wrap(toProtocol().toByteArray());
  }

  /**
   * Parse the serialized form of this protocol message.
   *
   * <p>Intended to help interoperability by allowing Flight clients to obtain stream info from non-Flight services.
   *
   * @param serialized The serialized form of the FlightDescriptor, as returned by {@link #serialize()}.
   * @return The deserialized FlightDescriptor.
   * @throws IOException if the serialized form is invalid.
   */
  public static FlightDescriptor deserialize(ByteBuffer serialized) throws IOException {
    return new FlightDescriptor(Flight.FlightDescriptor.parseFrom(serialized));
  }

  @Override
  public String toString() {
    if (isCmd) {
      return cmd.toString();
    } else {
      return Joiner.on('.').join(path);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + cmd.hashCode();
    result = prime * result + (isCmd ? 1231 : 1237);
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FlightDescriptor other = (FlightDescriptor) obj;
    if (!cmd.equals(other.cmd)) {
      return false;
    }
    if (isCmd != other.isCmd) {
      return false;
    }
    if (path == null) {
      if (other.path != null) {
        return false;
      }
    } else if (!path.equals(other.path)) {
      return false;
    }
    return true;
  }


}
