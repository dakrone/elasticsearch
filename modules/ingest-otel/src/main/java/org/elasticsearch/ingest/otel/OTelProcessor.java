/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OTelProcessor extends AbstractProcessor {
    private static final Logger logger = LogManager.getLogger(OTelProcessor.class);

    public static final String TYPE = "otel";

    public static final String MESSAGE = "message";
    public static final String TIMESTAMP = "@timestamp";
    public static final String RESOURCE_ATTRIBUTES = "resource.attributes";
    public static final String ATTRIBUTES = "attributes";
    public static final String BODY_TEXT = "body.text";
    public static final String HOST_NAME = "host.name";
    public static final String LOG_LEVEL = "log.level";
    public static final String SEVERITY_TEXT = "severity_text";

    protected OTelProcessor(String tag, String description) {
        super(tag, description);
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        try {
            logger.info("--> document: {}", document.getSource());
            // Set<String> allFields = reduceFields(document.getSource().keySet());
            Set<String> allFields = reduceFields(IngestDocument.getAllFields(document.getSource()));
            logger.info("--> all fields: {}", allFields);
            for (String field : allFields) {
                logger.info(
                    "--> field: {} contained? {} value: {}",
                    field,
                    document.hasField(field),
                    document.getFieldValue(field, Object.class, true)
                );
                for (String subField : allSubFields(field)) {
                    logger.info(
                        "==> field: {} contained? {} value: {}",
                        subField,
                        document.hasField(subField),
                        document.getFieldValue(subField, Object.class, true)
                    );
                }
                // Ignore all metadata fields
                if (field.startsWith("_")) {
                    continue;
                }

                switch (field) {
                    case MESSAGE:
                        document.setFieldValue(BODY_TEXT, document.getFieldValue(MESSAGE, Object.class, true), true);
                        document.removeField(MESSAGE);
                        break;
                    case TIMESTAMP:
                        // Leave the timestamp alone
                        continue;
                    case RESOURCE_ATTRIBUTES:
                        // Leave resources alone
                        continue;
                    case ATTRIBUTES:
                        // Leave attributes alone
                        continue;
                    case HOST_NAME:
                        moveField(document, field, RESOURCE_ATTRIBUTES + "." + field);
                        break;
                    case LOG_LEVEL:
                        moveField(document, field, SEVERITY_TEXT);
                        break;
                    default:
                        moveField(document, field, ATTRIBUTES + "." + field);
                        break;
                }
                logger.info("--> new doc: {}", document.getSource());
            }
        } catch (Exception e) {
            logger.error("failed otel processing", e);
            throw e;
        }
        return document;
    }

    private Set<String> reduceFields(Set<String> allFields) {
        Set<String> reduced = new HashSet<>(allFields);
        for (String field : allFields) {
            if (field.contains(".")) {
                allSubFields(field).forEach(reduced::remove);
            }
        }
        return reduced;
    }

    /**
     * For a field name like "foo.bar.baz", return all the "sub-fields" for that field, such as "foo" and "foo.bar"
     */
    private List<String> allSubFields(String name) {
        if (name.contains(".") == false) {
            return List.of();
        }
        List<String> pieces = new ArrayList<>();
        int lastDot = name.lastIndexOf('.');
        while (lastDot > 0) {
            String subName = name.substring(0, lastDot);
            pieces.add(subName);
            lastDot = subName.lastIndexOf('.');
        }
        return pieces;
    }

    private void moveField(IngestDocument document, String field, String path) {
        logger.info("--> moving {} ==> {}", field, path);
        if (document.hasField(field)) {
            logger.info("--> field resolved and is present, setting…");
            document.setFieldValue(path, document.getFieldValue(field, Object.class, true), true);
            document.removeField(field);
        } else {
            Map<String, Object> source = document.getSource();

            boolean found = false;
            for (String subField : allSubFields(field)) {
                logger.info("--> trying subfield {} for {}", subField, field);
                if (document.hasField(subField)) {
                    logger.info("--> found in doc!");
                    logger.info(
                        "--> field {} resolved from {}, setting to {}…",
                        field,
                        subField,
                        document.getFieldValue(subField, Object.class, true)
                    );
                    document.setFieldValue(path, document.getFieldValue(subField, Object.class, true), true);
                    document.removeField(subField);
                    found = true;
                    break;
                }

                if (source.containsKey(subField)) {
                    logger.info("--> found in map!");
                    var value = source.get(subField);
                    logger.info("--> field {} resolved from {}, setting to {}…", field, subField, value);
                    document.setFieldValue(path, value, true);
                    document.removeFieldRaw(subField);
                    found = true;
                    break;
                }
            }
            logger.info("--> field found? {}", found);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        public Factory() {}

        @Override
        public OTelProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            return new OTelProcessor(processorTag, description);
        }
    }
}
