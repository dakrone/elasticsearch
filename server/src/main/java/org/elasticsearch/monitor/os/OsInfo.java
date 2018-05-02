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

package org.elasticsearch.monitor.os;

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

public class OsInfo implements Writeable, ToXContentFragment {

    private static final ObjectParser<OsInfoBuilder, Void> PARSER = new ObjectParser<>("os", true, OsInfoBuilder::new);

    static {
        PARSER.declareField((b, v) -> b.refreshInterval = v, (p, c) -> p.longValue(),
            Fields.REFRESH_INTERVAL_IN_MILLIS, ObjectParser.ValueType.LONG);
        PARSER.declareField((b, v) -> b.availableProcessors = v, (p, c) -> p.intValue(),
            Fields.AVAILABLE_PROCESSORS, ObjectParser.ValueType.INT);
        PARSER.declareField((b, v) -> b.allocatedProcessors = v, (p, c) -> p.intValue(),
            Fields.ALLOCATED_PROCESSORS, ObjectParser.ValueType.INT);
        PARSER.declareString((b, v) -> b.name = v, Fields.NAME);
        PARSER.declareString((b, v) -> b.arch = v, Fields.ARCH);
        PARSER.declareString((b, v) -> b.version = v, Fields.VERSION);
    }

    private final long refreshInterval;
    private final int availableProcessors;
    private final int allocatedProcessors;
    private final String name;
    private final String arch;
    private final String version;

    public OsInfo(long refreshInterval, int availableProcessors, int allocatedProcessors, String name, String arch, String version) {
        this.refreshInterval = refreshInterval;
        this.availableProcessors = availableProcessors;
        this.allocatedProcessors = allocatedProcessors;
        this.name = name;
        this.arch = arch;
        this.version = version;
    }

    public OsInfo(StreamInput in) throws IOException {
        this.refreshInterval = in.readLong();
        this.availableProcessors = in.readInt();
        this.allocatedProcessors = in.readInt();
        this.name = in.readOptionalString();
        this.arch = in.readOptionalString();
        this.version = in.readOptionalString();
    }

    private static class OsInfoBuilder {
        private long refreshInterval;
        private int availableProcessors;
        private int allocatedProcessors;
        private String name;
        private String arch;
        private String version;

        OsInfoBuilder() { }

        OsInfo build() {
            return new OsInfo(refreshInterval, availableProcessors, allocatedProcessors, name, arch, version);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(refreshInterval);
        out.writeInt(availableProcessors);
        out.writeInt(allocatedProcessors);
        out.writeOptionalString(name);
        out.writeOptionalString(arch);
        out.writeOptionalString(version);
    }

    public long getRefreshInterval() {
        return this.refreshInterval;
    }

    public int getAvailableProcessors() {
        return this.availableProcessors;
    }

    public int getAllocatedProcessors() {
        return this.allocatedProcessors;
    }

    public String getName() {
        return name;
    }

    public String getArch() {
        return arch;
    }

    public String getVersion() {
        return version;
    }

    static final class Fields {
        static final ParseField NAME = new ParseField("name");
        static final ParseField ARCH = new ParseField("arch");
        static final ParseField VERSION = new ParseField("version");
        static final ParseField REFRESH_INTERVAL = new ParseField("refresh_interval");
        static final ParseField REFRESH_INTERVAL_IN_MILLIS = new ParseField("refresh_interval_in_millis");
        static final ParseField AVAILABLE_PROCESSORS = new ParseField("available_processors");
        static final ParseField ALLOCATED_PROCESSORS = new ParseField("allocated_processors");
    }

    public static OsInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null).build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.humanReadableField(Fields.REFRESH_INTERVAL_IN_MILLIS.getPreferredName(),
            Fields.REFRESH_INTERVAL.getPreferredName(), new TimeValue(refreshInterval));
        if (name != null) {
            builder.field(Fields.NAME.getPreferredName(), name);
        }
        if (arch != null) {
            builder.field(Fields.ARCH.getPreferredName(), arch);
        }
        if (version != null) {
            builder.field(Fields.VERSION.getPreferredName(), version);
        }
        builder.field(Fields.AVAILABLE_PROCESSORS.getPreferredName(), availableProcessors);
        builder.field(Fields.ALLOCATED_PROCESSORS.getPreferredName(), allocatedProcessors);
        return builder;
    }
}
