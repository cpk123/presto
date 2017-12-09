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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class RtviewRecordSet
        implements RecordSet
{
    private final List<RtviewColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ByteSource byteSource;

    public static URL appendQuery(URL url, String query)
    {
        URL newURL = url;

        if (url.getProtocol().equals("http")) {
            try {
                newURL = new URL(url.toString() + "&" + query);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return newURL;
    }

    public RtviewRecordSet(RtviewSplit split, List<RtviewColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        String columns = "cols=";
        for (RtviewColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
            columns += column.getColumnName() + ";";
        }
        this.columnTypes = types.build();

        try {
            URL splitURL = appendQuery(split.getUri().toURL(), columns);
            byteSource = Resources.asByteSource(splitURL);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new RtviewRecordCursor(columnHandles, byteSource);
    }
}
