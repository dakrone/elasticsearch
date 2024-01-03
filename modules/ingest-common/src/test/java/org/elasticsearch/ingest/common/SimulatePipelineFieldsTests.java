/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.action.ingest.SimulateDocumentResult;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;

public class SimulatePipelineFieldsTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(IngestCommonPlugin.class);
    }

    public void testFieldReading() throws Exception {
        // Vanilla field access
        simulateReadFrom("foo", """
            {"foo": "val"}""", "val");

        // Dotted access
        simulateReadFrom("foo.bar", """
            {"foo": {"bar": "val"}}""", "val");

        // Dotted object-then-flattened access
        // Fails because dots in field names is not yet supported
        // simulateReadFrom("foo.bar.baz", """
        // {"foo": {"bar.baz": "val"}}""", "val");

        // Dotted flattened access
        // Fails because field access in the foo.bar format does not work
        // simulateReadFrom("foo.bar", """
        // {"foo.bar": "val"}""", "val");

        // Array offset access
        simulateReadFrom("foo.0", """
            {"foo": ["val", "other"]}""", "val");

        // Array offset+field access
        simulateReadFrom("foo.0.baz", """
            {"foo": [{"baz": "val"}, {"other": "thing"}]}""", "val");

        // Array offset+object access
        simulateReadFrom("foo.0.baz.eggplant", """
            {"foo": [{"baz": {"eggplant": "val"}, "potato": "skin"}, {"other": "thing"}]}""", "val");

        // Array offset+array access
        simulateReadFrom("foo.0.baz.1", """
            {"foo": [{"baz": ["eggplant", "val"], "potato": "skin"}, {"other": "thing"}]}""", "val");

        // Array offset+dotted access
        // Fails because field access in dotted format does not work
        // simulateReadFrom("foo.0.baz.eggplant", """
        // {"foo": [{"baz.eggplant": "val"}, {"other": "thing"}]}""", "val");
    }

    public void testFieldWriting() throws Exception {
        // Vanilla field writing
        simulateWriteTo("foo", "foo");

        // Dotted field writing
        simulateWriteTo("foo.bar", "foo.bar");

        // Dotted field writing with overwrite
        simulateWriteTo("foo.bar", "foo.bar", """
            {"foo": {"bar": "other"}}""");
    }

    private void simulateReadFrom(String fieldAccess, String doc, String expected) throws ExecutionException, InterruptedException {
        SimulatePipelineRequest request = new SimulatePipelineRequest(new BytesArray("""
            {
              "pipeline" :
              {
                "description": "_description",
                "processors": [
                  {
                    "set" : {
                      "field" : "value",
                      "copy_from" : """ + "\"" + fieldAccess + "\"" + """
                  }
                }
              ]
            },
            "docs": [
              {
                "_index": "index",
                "_id": "id",
                "_source": """ + doc + """
                }
              ]
            }
            """), XContentType.JSON);
        // logger.info("--> req: {}", request.getSource().utf8ToString());
        SimulatePipelineResponse response = client().execute(SimulatePipelineAction.INSTANCE, request).get();
        SimulateDocumentResult docResult = response.getResults().get(0);
        // logger.info("--> res: {}", Strings.toString(docResult));
        Map<String, Object> respDoc = XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(docResult), false);
        logger.info("--> field [{}] on doc [{}] rendered [{}]", fieldAccess, doc, respDoc);
        Optional.ofNullable(ObjectPath.eval("error", respDoc))
            .ifPresent(error -> fail("expected no error, but copying/reading from [" + fieldAccess + "] failed with: " + error));
        String value = ObjectPath.eval("doc._source.value", respDoc);
        assertNotNull("expected a value at [doc._source.value] in the document, but was missing or null. Doc: " + respDoc, value);
        assertThat(
            "expected the value at [doc._source.value] to be the expected value, but it was not. Doc: " + respDoc,
            value,
            equalTo(expected)
        );
    }

    private void simulateWriteTo(String fieldToWrite, String expectedObjPath) throws ExecutionException, InterruptedException {
        simulateWriteTo(fieldToWrite, expectedObjPath, "{}");
    }

    private void simulateWriteTo(String fieldToWrite, String expectedObjPath, String doc) throws ExecutionException, InterruptedException {
        SimulatePipelineRequest request = new SimulatePipelineRequest(new BytesArray("""
            {
              "pipeline" :
              {
                "description": "_description",
                "processors": [
                  {
                    "set" : {
                      "value" : "val",
                      "field" : """ + "\"" + fieldToWrite + "\"" + """
                  }
                }
              ]
            },
            "docs": [
              {
                "_index": "index",
                "_id": "id",
                "_source": """ + doc + """
                }
              ]
            }
            """), XContentType.JSON);
        // logger.info("--> req: {}", request.getSource().utf8ToString());
        SimulatePipelineResponse response = client().execute(SimulatePipelineAction.INSTANCE, request).get();
        SimulateDocumentResult docResult = response.getResults().get(0);
        // logger.info("--> res: {}", Strings.toString(docResult));
        Map<String, Object> respDoc = XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(docResult), false);
        logger.info("--> field [{}] on doc [{}] rendered [{}]", fieldToWrite, doc, respDoc);
        Optional.ofNullable(ObjectPath.eval("error", respDoc))
            .ifPresent(error -> fail("expected no error, but copying/reading from [" + fieldToWrite + "] failed with: " + error));
        String value = ObjectPath.eval("doc._source." + expectedObjPath, respDoc);
        assertNotNull(
            "expected a value at [doc._source." + expectedObjPath + "] in the document, but was missing or null. Doc: " + respDoc,
            value
        );
        assertThat(
            "expected the value at [doc._source.value] to have the value [val], but it did not. Doc: " + respDoc,
            value,
            equalTo("val")
        );
    }
}
