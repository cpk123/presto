/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.rtview;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.net.URI;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestRtviewSplit
{
    private final RtviewSplit split = new RtviewSplit("connectorId", "schemaName", "tableName", URI.create("http://127.0.0.1/test.file"));

    @Test
    public void testAddresses()
    {
        // http split with default port
        RtviewSplit httpSplit = new RtviewSplit("connectorId", "schemaName", "tableName", URI.create("http://rtview.com/rtview"));
        assertEquals(httpSplit.getAddresses(), ImmutableList.of(HostAddress.fromString("rtview.com")));
        assertEquals(httpSplit.isRemotelyAccessible(), true);

        // http split with custom port
        httpSplit = new RtviewSplit("connectorId", "schemaName", "tableName", URI.create("http://rtview.com:8080/rtview"));
        assertEquals(httpSplit.getAddresses(), ImmutableList.of(HostAddress.fromParts("rtview.com", 8080)));
        assertEquals(httpSplit.isRemotelyAccessible(), true);

        // http split with default port
        RtviewSplit httpsSplit = new RtviewSplit("connectorId", "schemaName", "tableName", URI.create("https://rtview.com/rtview"));
        assertEquals(httpsSplit.getAddresses(), ImmutableList.of(HostAddress.fromString("rtview.com")));
        assertEquals(httpsSplit.isRemotelyAccessible(), true);

        // http split with custom port
        httpsSplit = new RtviewSplit("connectorId", "schemaName", "tableName", URI.create("https://rtview.com:8443/rtview"));
        assertEquals(httpsSplit.getAddresses(), ImmutableList.of(HostAddress.fromParts("rtview.com", 8443)));
        assertEquals(httpsSplit.isRemotelyAccessible(), true);
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<RtviewSplit> codec = jsonCodec(RtviewSplit.class);
        String json = codec.toJson(split);
        RtviewSplit copy = codec.fromJson(json);
        assertEquals(copy.getConnectorId(), split.getConnectorId());
        assertEquals(copy.getSchemaName(), split.getSchemaName());
        assertEquals(copy.getTableName(), split.getTableName());
        assertEquals(copy.getUri(), split.getUri());

        assertEquals(copy.getAddresses(), ImmutableList.of(HostAddress.fromString("127.0.0.1")));
        assertEquals(copy.isRemotelyAccessible(), true);
    }
}
