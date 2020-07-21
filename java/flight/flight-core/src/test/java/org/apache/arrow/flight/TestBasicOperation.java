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

import static org.apache.arrow.flight.FlightTestUtil.buildBytesWrapper;
import static org.apache.arrow.flight.FlightTestUtil.buildStringWrapper;
import static org.apache.arrow.flight.FlightTestUtil.unpackOrAssert;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.flight.wrappers.impl.Wrappers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Test the operations of a basic flight service.
 */
public class TestBasicOperation {

  /**
   * ARROW-6017: we should be able to construct locations for unknown schemes.
   */
  @Test
  public void unknownScheme() throws URISyntaxException {
    final Location location = new Location("s3://unknown");
    Assert.assertEquals("s3", location.getUri().getScheme());
  }

  @Test
  public void unknownSchemeRemote() throws Exception {
    test(c -> {
      try {
        final FlightInfo info = c.getInfo(FlightDescriptor.path("test"));
        Assert.assertEquals(new URI("https://example.com"), info.getEndpoints().get(0).getLocations().get(0).getUri());
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void roundTripTicket() throws Exception {
    final Ticket ticket = new Ticket(new byte[]{0, 1, 2, 3, 4, 5});
    Assert.assertEquals(ticket, Ticket.deserialize(ticket.serialize()));
  }

  @Test
  public void roundTripInfo() throws Exception {
    final Map<String, String> metadata = new HashMap<>();
    metadata.put("foo", "bar");
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("a", new ArrowType.Int(32, true)),
        Field.nullable("b", new ArrowType.FixedSizeBinary(32))
    ), metadata);
    final FlightInfo info1 = new FlightInfo(schema, FlightDescriptor.path(), Collections.emptyList(), -1, -1);
    final FlightInfo info2 = new FlightInfo(schema, FlightDescriptor.command(buildBytesWrapper(new byte[2])),
        Collections.singletonList(new FlightEndpoint(
            new Ticket(new byte[10]), Location.forGrpcDomainSocket("/tmp/test.sock"))), 200, 500);
    final FlightInfo info3 = new FlightInfo(schema, FlightDescriptor.path("a", "b"),
        Arrays.asList(new FlightEndpoint(
                new Ticket(new byte[10]), Location.forGrpcDomainSocket("/tmp/test.sock")),
            new FlightEndpoint(
                new Ticket(new byte[10]), Location.forGrpcDomainSocket("/tmp/test.sock"),
                Location.forGrpcInsecure("localhost", 50051))
        ), 200, 500);

    Assert.assertEquals(info1, FlightInfo.deserialize(info1.serialize()));
    Assert.assertEquals(info2, FlightInfo.deserialize(info2.serialize()));
    Assert.assertEquals(info3, FlightInfo.deserialize(info3.serialize()));
  }

  @Test
  public void roundTripDescriptor() throws Exception {
    final FlightDescriptor cmd = FlightDescriptor.command(buildStringWrapper("test command"));
    Assert.assertEquals(cmd, FlightDescriptor.deserialize(cmd.serialize()));
    final FlightDescriptor path = FlightDescriptor.path("foo", "bar", "test.arrow");
    Assert.assertEquals(path, FlightDescriptor.deserialize(path.serialize()));
  }

  @Test
  public void getDescriptors() throws Exception {
    test(c -> {
      int count = 0;
      for (FlightInfo i : c.listFlights(Criteria.ALL)) {
        count += 1;
      }
      Assert.assertEquals(1, count);
    });
  }

  @Test
  public void getDescriptorsWithCriteria() throws Exception {
    test(c -> {
      int count = 0;
      for (FlightInfo i : c.listFlights(new Criteria(new byte[]{1}))) {
        count += 1;
      }
      Assert.assertEquals(0, count);
    });
  }

  @Test
  public void getDescriptor() throws Exception {
    test(c -> {
      System.out.println(c.getInfo(FlightDescriptor.path("hello")).getDescriptor());
    });
  }

  @Test
  public void getSchema() throws Exception {
    test(c -> {
      System.out.println(c.getSchema(FlightDescriptor.path("hello")).getSchema());
    });
  }


  @Test
  public void listActions() throws Exception {
    test(c -> {
      for (ActionType at : c.listActions()) {
        System.out.println(at.getType());
      }
    });
  }

  @Test
  public void doAction() throws Exception {
    test(c -> {
      Iterator<Result> stream = c.doAction(new Action("hello"));

      Assert.assertTrue(stream.hasNext());
      Result r = stream.next();
      try {
        Assert.assertEquals("world", r.getBody().unpack(Wrappers.stringWrapper.class).getMessage());
      } catch (InvalidProtocolBufferException e) {
        Assert.fail(e.getMessage());
      }
    });
    test(c -> {
      Iterator<Result> stream = c.doAction(new Action("hellooo"));

      Assert.assertTrue(stream.hasNext());
      Result r = stream.next();
      Assert.assertEquals("world", unpackOrAssert(r.getBody(), Wrappers.stringWrapper.class).getMessage());
      Assert.assertTrue(stream.hasNext());
      r = stream.next();
      Assert.assertEquals("!", unpackOrAssert(r.getBody(), Wrappers.stringWrapper.class).getMessage());
      Assert.assertFalse(stream.hasNext());
    });
  }

  @Test
  public void putStream() throws Exception {
    test((c, a) -> {
      final int size = 10;

      IntVector iv = new IntVector("c1", a);

      try (VectorSchemaRoot root = VectorSchemaRoot.of(iv)) {
        ClientStreamListener listener = c
            .startPut(FlightDescriptor.path("hello"), root, new AsyncPutListener());

        //batch 1
        root.allocateNew();
        for (int i = 0; i < size; i++) {
          iv.set(i, i);
        }
        iv.setValueCount(size);
        root.setRowCount(size);
        listener.putNext();

        // batch 2

        root.allocateNew();
        for (int i = 0; i < size; i++) {
          iv.set(i, i + size);
        }
        iv.setValueCount(size);
        root.setRowCount(size);
        listener.putNext();
        root.clear();
        listener.completed();

        // wait for ack to avoid memory leaks.
        listener.getResult();
      }
    });
  }

  @Test
  public void propagateErrors() throws Exception {
    test(client -> {
      FlightTestUtil.assertCode(FlightStatusCode.UNIMPLEMENTED, () -> {
        client.doAction(new Action("invalid-action")).forEachRemaining(action -> Assert.fail());
      });
    });
  }

  @Test
  public void getStream() throws Exception {
    test(c -> {
      try (final FlightStream stream = c.getStream(new Ticket(new byte[0]))) {
        VectorSchemaRoot root = stream.getRoot();
        IntVector iv = (IntVector) root.getVector("c1");
        int value = 0;
        while (stream.next()) {
          for (int i = 0; i < root.getRowCount(); i++) {
            Assert.assertEquals(value, iv.get(i));
            value++;
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Ensure the client is configured to accept large messages. */
  @Test
  public void getStreamLargeBatch() throws Exception {
    test(c -> {
      try (final FlightStream stream = c.getStream(new Ticket(Producer.TICKET_LARGE_BATCH))) {
        Assert.assertEquals(128, stream.getRoot().getFieldVectors().size());
        Assert.assertTrue(stream.next());
        Assert.assertEquals(65536, stream.getRoot().getRowCount());
        Assert.assertTrue(stream.next());
        Assert.assertEquals(65536, stream.getRoot().getRowCount());
        Assert.assertFalse(stream.next());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Ensure the server is configured to accept large messages. */
  @Test
  public void startPutLargeBatch() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      final List<FieldVector> vectors = new ArrayList<>();
      for (int col = 0; col < 128; col++) {
        final BigIntVector vector = new BigIntVector("f" + col, allocator);
        for (int row = 0; row < 65536; row++) {
          vector.setSafe(row, row);
        }
        vectors.add(vector);
      }
      test(c -> {
        try (final VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {
          root.setRowCount(65536);
          final ClientStreamListener stream = c.startPut(FlightDescriptor.path(""), root, new SyncPutListener());
          stream.putNext();
          stream.putNext();
          stream.completed();
          stream.getResult();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private void test(Consumer<FlightClient> consumer) throws Exception {
    test((c, a) -> {
      consumer.accept(c);
    });
  }

  private void test(BiConsumer<FlightClient, BufferAllocator> consumer) throws Exception {
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s =
            FlightTestUtil.getStartedServer(
                (location) -> FlightServer.builder(a, location, producer).build()
            )) {

      try (
          FlightClient c = FlightClient.builder(a, s.getLocation()).build()
      ) {
        try (BufferAllocator testAllocator = a.newChildAllocator("testcase", 0, Long.MAX_VALUE)) {
          consumer.accept(c, testAllocator);
        }
      }
    }
  }

  /**
   * An example FlightProducer for test purposes.
   */
  public static class Producer implements FlightProducer, AutoCloseable {
    static final byte[] TICKET_LARGE_BATCH = "large-batch".getBytes(StandardCharsets.UTF_8);

    private final BufferAllocator allocator;

    public Producer(BufferAllocator allocator) {
      super();
      this.allocator = allocator;
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria,
        StreamListener<FlightInfo> listener) {
      if (criteria.getExpression().length > 0) {
        // Don't send anything if criteria are set
        listener.onCompleted();
      }

      Flight.FlightInfo getInfo = Flight.FlightInfo.newBuilder()
          .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
              .setType(DescriptorType.CMD)
              .setCmd(buildStringWrapper("cool thing")))
          .build();
      try {
        listener.onNext(new FlightInfo(getInfo));
      } catch (URISyntaxException e) {
        listener.onError(e);
        return;
      }
      listener.onCompleted();
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        while (flightStream.next()) {
          // Drain the stream
        }
      };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      if (Arrays.equals(TICKET_LARGE_BATCH, ticket.getBytes())) {
        getLargeBatch(listener);
        return;
      }
      final int size = 10;

      IntVector iv = new IntVector("c1", allocator);
      VectorSchemaRoot root = VectorSchemaRoot.of(iv);
      listener.start(root);

      //batch 1
      root.allocateNew();
      for (int i = 0; i < size; i++) {
        iv.set(i, i);
      }
      iv.setValueCount(size);
      root.setRowCount(size);
      listener.putNext();

      // batch 2

      root.allocateNew();
      for (int i = 0; i < size; i++) {
        iv.set(i, i + size);
      }
      iv.setValueCount(size);
      root.setRowCount(size);
      listener.putNext();
      root.clear();
      listener.completed();
    }

    private void getLargeBatch(ServerStreamListener listener) {
      final List<FieldVector> vectors = new ArrayList<>();
      for (int col = 0; col < 128; col++) {
        final BigIntVector vector = new BigIntVector("f" + col, allocator);
        for (int row = 0; row < 65536; row++) {
          vector.setSafe(row, row);
        }
        vectors.add(vector);
      }
      try (final VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {
        root.setRowCount(65536);
        listener.start(root);
        listener.putNext();
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public void close() throws Exception {
      allocator.close();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context,
        FlightDescriptor descriptor) {
      try {
        Flight.FlightInfo getInfo = Flight.FlightInfo.newBuilder()
            .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
                .setType(DescriptorType.CMD)
                .setCmd(buildStringWrapper("cool thing")))
            .addEndpoint(
                Flight.FlightEndpoint.newBuilder().addLocation(new Location("https://example.com").toProtocol()))
            .build();
        return new FlightInfo(getInfo);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void doAction(CallContext context, Action action,
        StreamListener<Result> listener) {
      switch (action.getType()) {
        case "hello": {
          listener.onNext(new Result(buildStringWrapper("world")));
          listener.onCompleted();
          break;
        }
        case "hellooo": {
          listener.onNext(new Result(buildStringWrapper("world")));
          listener.onNext(new Result(buildStringWrapper("!")));
          listener.onCompleted();
          break;
        }
        default:
          listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Action not implemented: " + action.getType())
              .toRuntimeException());
      }
    }

    @Override
    public void listActions(CallContext context,
        StreamListener<ActionType> listener) {
      listener.onNext(new ActionType("get", ""));
      listener.onNext(new ActionType("put", ""));
      listener.onNext(new ActionType("hello", ""));
      listener.onCompleted();
    }

  }


}
