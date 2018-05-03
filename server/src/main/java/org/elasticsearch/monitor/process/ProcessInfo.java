/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.process;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ProcessInfo implements Writeable, ToXContentFragment {

    private static final ObjectParser<ProcessInfoBuilder, Void> PARSER = new ObjectParser<>("process", true, ProcessInfoBuilder::new);

    static {
        PARSER.declareLong((b, v) -> b.refreshInterval = v, Fields.REFRESH_INTERVAL_IN_MILLIS);
        PARSER.declareLong((b, v) -> b.id = v, Fields.ID);
        PARSER.declareBoolean((b, v) -> b.mlockall = v, Fields.MLOCKALL);
    }

    private final long refreshInterval;
    private final long id;
    private final boolean mlockall;

    public ProcessInfo(long id, boolean mlockall, long refreshInterval) {
        this.id = id;
        this.mlockall = mlockall;
        this.refreshInterval = refreshInterval;
    }

    public ProcessInfo(StreamInput in) throws IOException {
        refreshInterval = in.readLong();
        id = in.readLong();
        mlockall = in.readBoolean();
    }

    private static class ProcessInfoBuilder {
        private long refreshInterval;
        private long id;
        private boolean mlockall;

        ProcessInfoBuilder() { }

        ProcessInfo build() {
            return new ProcessInfo(id, mlockall, refreshInterval);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(refreshInterval);
        out.writeLong(id);
        out.writeBoolean(mlockall);
    }

    public long refreshInterval() {
        return this.refreshInterval;
    }

    public long getRefreshInterval() {
        return this.refreshInterval;
    }

    /**
     * The process id.
     */
    public long getId() {
        return id;
    }

    public boolean isMlockall() {
        return mlockall;
    }

    static final class Fields {
        static final ParseField PROCESS = new ParseField("process");
        static final ParseField REFRESH_INTERVAL = new ParseField("refresh_interval");
        static final ParseField REFRESH_INTERVAL_IN_MILLIS = new ParseField("refresh_interval_in_millis");
        static final ParseField ID = new ParseField("id");
        static final ParseField MLOCKALL = new ParseField("mlockall");
    }

    public static ProcessInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null).build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.humanReadableField(Fields.REFRESH_INTERVAL_IN_MILLIS.getPreferredName(),
            Fields.REFRESH_INTERVAL.getPreferredName(), new TimeValue(refreshInterval));
        builder.field(Fields.ID.getPreferredName(), id);
        builder.field(Fields.MLOCKALL.getPreferredName(), mlockall);
        return builder;
    }
}
