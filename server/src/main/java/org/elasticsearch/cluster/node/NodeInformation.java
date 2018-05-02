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

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeInformation {
    private final String id;
    private final String name;
    private final String hostname;
    private final String ip;
    private final String transportAddress;
    private final Set<String> roles;
    private final Map<String, String> attributes;
    private final Version version;

    public NodeInformation(String id, String name, String hostname, String ip, String transportAddress,
                           Set<String> roles, Map<String, String> attributes, Version version) {
        this.id = id;
        this.name = name;
        this.hostname = hostname;
        this.ip = ip;
        this.transportAddress = transportAddress;
        this.roles = roles;
        this.attributes = attributes;
        this.version = version;
    }

    public DiscoveryNode toDiscoveryNode() {
        int lastColon = transportAddress.lastIndexOf(":");
        assert lastColon != -1 : "expected address to have at least one :";
        String transportIp = transportAddress.substring(0, lastColon);
        int port = Integer.valueOf(transportAddress.substring(lastColon + 1, transportAddress.length()));
        System.out.println("got hostname: " + hostname + " and ip " + transportIp + " port: " + port);
        //            TransportAddress addr = new TransportAddress(InetAddress.getByAddress(hostname, transportIp.getBytes(StandardCharsets.UTF_8)), port);
        TransportAddress addr = new TransportAddress(TransportAddress.META_ADDRESS, port);
        return new DiscoveryNode(name, id, addr, attributes,
            roles.stream().map(DiscoveryNode.Role::valueOf).collect(Collectors.toSet()), version);
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getHostName() {
        return hostname;
    }

    public String getIp() {
        return ip;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public String getTransportAddress() {
        return transportAddress;
    }

    public String getAddress() {
        return getTransportAddress();
    }

    public String getHostAddress() {
        return getIp();
    }

    public Version getVersion() {
        return version;
    }
}
