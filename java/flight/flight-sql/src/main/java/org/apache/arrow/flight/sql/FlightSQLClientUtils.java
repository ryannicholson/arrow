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

package org.apache.arrow.flight.sql;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSQL;
import org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetPreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetTablesRequest;
import org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetTablesResult;
import org.apache.arrow.flight.sql.impl.FlightSQL.CommandPreparedStatementQuery;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

import io.grpc.Status;

/**
 * Client side utilities to work with Flight SQL semantics.
 */
public final class FlightSQLClientUtils {

  /**
   * Helper method to request a list of tables from a Flight SQL enabled endpoint.
   *
   * @param client              The Flight Client.
   * @param catalog             The catalog.
   * @param schemaFilterPattern The schema filter pattern.
   * @param tableFilterPattern  The table filter pattern.
   * @param tableTypes          The table types to include.
   * @param includeSchema       True to include the schema upon return, false to not include the schema.
   * @return A list of tables matching the criteria.
   */
  public static List<ActionGetTablesResult> getTables(FlightClient client, String catalog, String schemaFilterPattern,
          String tableFilterPattern, List<String> tableTypes, boolean includeSchema) {

    final ActionGetTablesRequest.Builder requestBuilder = ActionGetTablesRequest
            .newBuilder()
            .setIncludeSchema(includeSchema);

    if (catalog != null) {
      requestBuilder.setCatalog(catalog);
    }

    if (schemaFilterPattern != null) {
      requestBuilder.setSchemaFilterPattern(schemaFilterPattern);
    }

    if (tableFilterPattern != null) {
      requestBuilder.setTableNameFilterPattern(tableFilterPattern);
    }

    if (tableTypes != null) {
      requestBuilder.addAllTableTypes(tableTypes);
    }

    final Iterator<Result> results = client.doAction(new Action(
            "GetTables", Any.pack(requestBuilder.build()).toByteArray()));

    final List<ActionGetTablesResult> getTablesResults = new ArrayList<>();
    results.forEachRemaining(result -> {
      ActionGetTablesResult actual = FlightSQLUtils.unpackAndParseOrThrow(result.getBody(),
              ActionGetTablesResult.class);
      getTablesResults.add(actual);
    });

    return getTablesResults;
  }

  /**
   * Helper method to create a prepared statement on the server.
   *
   * @param client The Flight Client.
   * @param query  The query to prepare.
   * @return Metadata and handles to the prepared statement which exists on the server.
   */
  public static FlightSQLPreparedStatement getPreparedStatement(FlightClient client, String query) {
    return new FlightSQLPreparedStatement(client, query);
  }

  /**
   * Helper class to encapsulate Flight SQL prepared statement logic.
   */
  public static class FlightSQLPreparedStatement implements Closeable {
    private final FlightClient client;
    private final ActionGetPreparedStatementResult preparedStatementResult;
    private long invocationCount;
    private boolean isClosed;
    private Schema resultSetSchema = null;
    private Schema parameterSchema = null;

    /**
     * Constructor.
     *
     * @param client The client. FlightSQLPreparedStatement does not maintain this resource.
     * @param sql    The query.
     */
    public FlightSQLPreparedStatement(FlightClient client, String sql) {
      this.client = client;

      final Iterator<Result> preparedStatementResults = client.doAction(new Action("GetPreparedStatement",
              Any.pack(FlightSQL.ActionGetPreparedStatementRequest
                      .newBuilder()
                      .setQuery(sql)
                      .build())
                      .toByteArray()));

      preparedStatementResult = FlightSQLUtils.unpackAndParseOrThrow(
              preparedStatementResults.next().getBody(),
              ActionGetPreparedStatementResult.class);

      invocationCount = 0;
      isClosed = false;
    }

    /**
     * Returns the Schema of the resultset.
     *
     * @return the Schema of the resultset.
     */
    public Schema getResultSetSchema() {
      if (resultSetSchema == null && preparedStatementResult.getDatasetSchema() != null) {
        resultSetSchema = Schema.deserialize(preparedStatementResult.getDatasetSchema().asReadOnlyByteBuffer());
      }
      return resultSetSchema;
    }

    /**
     * Returns the Schema of the parameters.
     *
     * @return the Schema of the parameters.
     */
    public Schema getParameterSchema() {
      if (parameterSchema == null && preparedStatementResult.getParameterSchema() != null) {
        parameterSchema = Schema.deserialize(preparedStatementResult.getParameterSchema().asReadOnlyByteBuffer());
      }
      return parameterSchema;
    }

    /**
     * Executes the prepared statement query on the server.
     *
     * @return a FlightInfo object representing the stream(s) to fetch.
     * @throws IOException if the PreparedStatement is closed.
     */
    public FlightInfo executeQuery() throws IOException {
      if (isClosed) {
        throw new IOException("Prepared statement has already been closed on the server.");
      }

      final FlightDescriptor descriptor = FlightDescriptor
              .command(Any.pack(CommandPreparedStatementQuery.newBuilder()
                      .setClientExecutionHandle(
                              ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(invocationCount++)))
                      .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                      .build())
                      .toByteArray());

      return client.getInfo(descriptor);
    }

    /**
     * Executes the prepared statement update on the server.
     *
     * @return the number of rows updated.
     */
    public long executeUpdate() {
      throw Status.UNIMPLEMENTED.asRuntimeException();
    }

    @Override
    public void close() {
      isClosed = true;
      final Iterator<Result> closePreparedStatementResults = client.doAction(new Action("ClosePreparedStatement",
              Any.pack(FlightSQL.ActionClosePreparedStatementRequest
                      .newBuilder()
                      .setPreparedStatementHandleBytes(preparedStatementResult.getPreparedStatementHandle())
                      .build())
                      .toByteArray()));
      closePreparedStatementResults.forEachRemaining(result -> {
      });
    }

    /**
     * Returns if the prepared statement is already closed.
     *
     * @return true if the prepared statement is already closed.
     */
    public boolean isClosed() {
      return isClosed;
    }
  }
}
