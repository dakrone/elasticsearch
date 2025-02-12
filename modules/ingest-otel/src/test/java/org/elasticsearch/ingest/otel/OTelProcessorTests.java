/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class OTelProcessorTests extends ESTestCase {
    public void testDoc() throws Exception {
        IngestDocument doc = new IngestDocument("index", "id", 1, "routing", VersionType.INTERNAL, Map.of(
            "@timestamp", "2025-01-01",
            "message", "this is a log message",
            "log", Map.of("level", "INFO"),
            "host.name", "machine2",
            "tag", Map.of("env.name", "prod", "type", Map.of("type_name", "eggplant"))
//            "rack.name", Map.of("value", "rack1")
        ));
        OTelProcessor processor = new OTelProcessor("tag", "desc");
        processor.execute(doc);
    }
}
