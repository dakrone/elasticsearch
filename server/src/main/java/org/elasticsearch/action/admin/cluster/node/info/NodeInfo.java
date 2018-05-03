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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.NodeInformation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Node information (static, does not change over time).
 */
public class NodeInfo extends BaseNodeResponse implements ToXContentFragment {

    // Does not parse inner id
    private static final ObjectParser<NodeInfoBuilder, String> PARSER = new ObjectParser<>("node_info", true, NodeInfoBuilder::new);

    static {
        PARSER.declareString((n, v) -> n.name = v, Fields.NAME);
        PARSER.declareString((n, v) -> n.transportAddress = v, Fields.TRANSPORT_ADDRESS);
        PARSER.declareString((n, v) -> n.host = v, Fields.HOST);
        PARSER.declareString((n, v) -> n.ipAddress = v, Fields.IP);
        PARSER.declareString((n, v) -> n.version = v, Fields.VERSION);
        PARSER.declareStringArray((n, v) -> n.roles = v, Fields.ROLES);
        PARSER.declareObject((n, v) -> n.attributes = v, (p, c) -> p.mapStrings(), Fields.ATTRIBUTES);
        PARSER.declareField((n, v) -> n.totalIndexingBuffer = v, (p, c) -> p.longValue(), Fields.TOTAL_INDEXING_BUFFER_BYTES,
            ObjectParser.ValueType.LONG);
        PARSER.declareField((n, v) -> n.settings = v, Settings::fromXContent, Fields.SETTINGS, ObjectParser.ValueType.OBJECT);
        PARSER.declareString((n, v) -> n.buildFlavor = v, Fields.BUILD_FLAVOR);
        PARSER.declareString((n, v) -> n.buildType = v, Fields.BUILD_TYPE);
        PARSER.declareString((n, v) -> n.buildShortHash = v, Fields.BUILD_HASH);
        PARSER.declareString((n, v) -> n.buildDate = v, Fields.BUILD_DATE);
        PARSER.declareBoolean((n, v) -> n.buildSnapshot = v, Fields.BUILD_SNAPSHOT);

        // Parsers for the sub objects
        PARSER.declareField((n, v) -> n.os = v, OsInfo::fromXContent, Fields.OS, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((n, v) -> n.process = v, ProcessInfo::fromXContent, Fields.PROCESS, ObjectParser.ValueType.OBJECT);

    }

    static final class Fields {
        static final ParseField NAME = new ParseField("name");
        static final ParseField TRANSPORT_ADDRESS = new ParseField("transport_address");
        static final ParseField HOST = new ParseField("host");
        static final ParseField IP = new ParseField("ip");
        static final ParseField VERSION = new ParseField("version");
        static final ParseField TOTAL_INDEXING_BUFFER = new ParseField("total_indexing_buffer");
        static final ParseField TOTAL_INDEXING_BUFFER_BYTES = new ParseField("total_indexing_buffer_in_bytes");
        static final ParseField ROLES = new ParseField("roles");
        static final ParseField ATTRIBUTES = new ParseField("attributes");
        static final ParseField SETTINGS = new ParseField("settings");
        static final ParseField BUILD_FLAVOR = new ParseField("build_flavor");
        static final ParseField BUILD_TYPE = new ParseField("build_type");
        static final ParseField BUILD_HASH = new ParseField("build_hash");
        static final ParseField BUILD_DATE = new ParseField("build_date");
        static final ParseField BUILD_SNAPSHOT = new ParseField("build_snapshot");

        static final ParseField OS = new ParseField("os");
        static final ParseField PROCESS = new ParseField("process");
        static final ParseField JVM = new ParseField("jvm");
        static final ParseField THREADPOOL = new ParseField("threadpool");
        static final ParseField TRANSPORT = new ParseField("transport");
        static final ParseField HTTP = new ParseField("http");
        static final ParseField PLUGINS = new ParseField("plugins");
        static final ParseField INGEST = new ParseField("ingest");
    }


    private Build build;

    @Nullable
    private Settings settings;

    @Nullable
    private OsInfo os;

    @Nullable
    private ProcessInfo process;

    @Nullable
    private JvmInfo jvm;

    @Nullable
    private ThreadPoolInfo threadPool;

    @Nullable
    private TransportInfo transport;

    @Nullable
    private HttpInfo http;

    @Nullable
    private PluginsAndModules plugins;

    @Nullable
    private IngestInfo ingest;

    @Nullable
    private ByteSizeValue totalIndexingBuffer;

    public NodeInfo() {
    }

    public NodeInfo(Build build, DiscoveryNode node, @Nullable Settings settings, @Nullable OsInfo os, @Nullable ProcessInfo process,
                    @Nullable JvmInfo jvm, @Nullable ThreadPoolInfo threadPool, @Nullable TransportInfo transport, @Nullable HttpInfo http,
                    @Nullable PluginsAndModules plugins, @Nullable IngestInfo ingest, @Nullable ByteSizeValue totalIndexingBuffer) {
        super(node);
        this.build = build;
        this.settings = settings;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.transport = transport;
        this.http = http;
        this.plugins = plugins;
        this.ingest = ingest;
        this.totalIndexingBuffer = totalIndexingBuffer;
    }

    private NodeInfo(Build build, NodeInformation node, @Nullable Settings settings, @Nullable OsInfo os, @Nullable ProcessInfo process,
                     @Nullable JvmInfo jvm, @Nullable ThreadPoolInfo threadPool, @Nullable TransportInfo transport, @Nullable HttpInfo http,
                     @Nullable PluginsAndModules plugins, @Nullable IngestInfo ingest, @Nullable ByteSizeValue totalIndexingBuffer) {
        this(build, node.toDiscoveryNode(), settings, os, process, jvm, threadPool, transport, http, plugins, ingest, totalIndexingBuffer);
    }

    private static class NodeInfoBuilder {
        private String id;
        private String name;
        private String transportAddress;
        private String host;
        private String ipAddress;
        private String version;
        private List<String> roles = new ArrayList<>();
        private Map<String, String> attributes = new HashMap<>();
        private long totalIndexingBuffer;
        private String buildFlavor;
        private String buildType;
        private String buildShortHash;
        private String buildDate;
        private boolean buildSnapshot;
        private Settings settings;
        private OsInfo os;
        private ProcessInfo process;
        private JvmInfo jvm;
        private ThreadPoolInfo threadPool;
        private TransportInfo transport;
        private HttpInfo http;
        private PluginsAndModules plugins;
        private IngestInfo ingest;

        NodeInfoBuilder() { }

        public NodeInfo build() {
            Build build = new Build(Build.Flavor.fromDisplayName(buildFlavor), Build.Type.fromDisplayName(buildType),
                buildShortHash, buildDate, buildSnapshot);
            NodeInformation ni = new NodeInformation(id, name, host, ipAddress, transportAddress,
                new HashSet<>(roles), attributes, Version.fromString(version));
            return new NodeInfo(build, ni, settings, os, process, jvm, threadPool, transport,
                http, plugins, ingest, new ByteSizeValue(totalIndexingBuffer));
        }
    }

    /**
     * System's hostname. <code>null</code> in case of UnknownHostException
     */
    @Nullable
    public String getHostname() {
        return getNode().getHostName();
    }

    /**
     * The current ES version
     */
    public Version getVersion() {
        return getNode().getVersion();
    }

    /**
     * The build version of the node.
     */
    public Build getBuild() {
        return this.build;
    }

    /**
     * The settings of the node.
     */
    @Nullable
    public Settings getSettings() {
        return this.settings;
    }

    /**
     * Operating System level information.
     */
    @Nullable
    public OsInfo getOs() {
        return this.os;
    }

    /**
     * Process level information.
     */
    @Nullable
    public ProcessInfo getProcess() {
        return process;
    }

    /**
     * JVM level information.
     */
    @Nullable
    public JvmInfo getJvm() {
        return jvm;
    }

    @Nullable
    public ThreadPoolInfo getThreadPool() {
        return this.threadPool;
    }

    @Nullable
    public TransportInfo getTransport() {
        return transport;
    }

    @Nullable
    public HttpInfo getHttp() {
        return http;
    }

    @Nullable
    public PluginsAndModules getPlugins() {
        return this.plugins;
    }

    @Nullable
    public IngestInfo getIngest() {
        return ingest;
    }

    @Nullable
    public ByteSizeValue getTotalIndexingBuffer() {
        return totalIndexingBuffer;
    }

    public static NodeInfo readNodeInfo(StreamInput in) throws IOException {
        NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().before(Version.V_7_0_0_alpha1)) {
            Version.readVersion(in);
        }
        build = Build.readBuild(in);
        if (in.readBoolean()) {
            totalIndexingBuffer = new ByteSizeValue(in.readLong());
        } else {
            totalIndexingBuffer = null;
        }
        if (in.readBoolean()) {
            settings = Settings.readSettingsFromStream(in);
        }
        os = in.readOptionalWriteable(OsInfo::new);
        process = in.readOptionalWriteable(ProcessInfo::new);
        jvm = in.readOptionalWriteable(JvmInfo::new);
        threadPool = in.readOptionalWriteable(ThreadPoolInfo::new);
        transport = in.readOptionalWriteable(TransportInfo::new);
        http = in.readOptionalWriteable(HttpInfo::new);
        plugins = in.readOptionalWriteable(PluginsAndModules::new);
        ingest = in.readOptionalWriteable(IngestInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_7_0_0_alpha1)) {
            out.writeVInt(getVersion().id);
        }
        Build.writeBuild(build, out);
        if (totalIndexingBuffer == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(totalIndexingBuffer.getBytes());
        }
        if (settings == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Settings.writeSettingsToStream(settings, out);
        }
        out.writeOptionalWriteable(os);
        out.writeOptionalWriteable(process);
        out.writeOptionalWriteable(jvm);
        out.writeOptionalWriteable(threadPool);
        out.writeOptionalWriteable(transport);
        out.writeOptionalWriteable(http);
        out.writeOptionalWriteable(plugins);
        out.writeOptionalWriteable(ingest);
    }

    public static NodeInfo fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        parser.nextToken(); // Unwrap
        String id = parser.currentName(); // get Node ID
        parser.nextToken(); // advance to the actual node info object
        NodeInfoBuilder builder = PARSER.apply(parser, null);
        builder.id = id;
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getNode().getId());

        builder.field(Fields.NAME.getPreferredName(), getNode().getName());
        builder.field(Fields.TRANSPORT_ADDRESS.getPreferredName(), getNode().getAddress().toString());
        builder.field(Fields.HOST.getPreferredName(), getNode().getHostName());
        builder.field(Fields.IP.getPreferredName(), getNode().getHostAddress());

        builder.field(Fields.VERSION.getPreferredName(), getVersion());
        builder.field(Fields.BUILD_FLAVOR.getPreferredName(), getBuild().flavor().displayName());
        builder.field(Fields.BUILD_TYPE.getPreferredName(), getBuild().type().displayName());
        builder.field(Fields.BUILD_HASH.getPreferredName(), getBuild().shortHash());
        builder.field(Fields.BUILD_DATE.getPreferredName(), getBuild().date());
        builder.field(Fields.BUILD_SNAPSHOT.getPreferredName(), getBuild().isSnapshot());
        if (getTotalIndexingBuffer() != null) {
            builder.humanReadableField(Fields.TOTAL_INDEXING_BUFFER_BYTES.getPreferredName(),
                Fields.TOTAL_INDEXING_BUFFER.getPreferredName(), getTotalIndexingBuffer());
        }

        builder.startArray(Fields.ROLES.getPreferredName());
        for (DiscoveryNode.Role role : getNode().getRoles()) {
            builder.value(role.getRoleName());
        }
        builder.endArray();

        if (!getNode().getAttributes().isEmpty()) {
            builder.startObject(Fields.ATTRIBUTES.getPreferredName());
            for (Map.Entry<String, String> entry : getNode().getAttributes().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (getSettings() != null) {
            builder.startObject(Fields.SETTINGS.getPreferredName());
            Settings settings = getSettings();
            settings.toXContent(builder, params);
            builder.endObject();
        }

        if (getOs() != null) {
            builder.startObject(Fields.OS.getPreferredName());
            getOs().toXContent(builder, params);
            builder.endObject();
        }
        if (getProcess() != null) {
            builder.startObject(Fields.PROCESS.getPreferredName());
            getProcess().toXContent(builder, params);
            builder.endObject();
        }
        if (getJvm() != null) {
            getJvm().toXContent(builder, params);
        }
        if (getThreadPool() != null) {
            getThreadPool().toXContent(builder, params);
        }
        if (getTransport() != null) {
            getTransport().toXContent(builder, params);
        }
        if (getHttp() != null) {
            getHttp().toXContent(builder, params);
        }
        if (getPlugins() != null) {
            getPlugins().toXContent(builder, params);
        }
        if (getIngest() != null) {
            getIngest().toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }
}
