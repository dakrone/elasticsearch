/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmAction;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmActionContext;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.datastreams.lifecycle.transitions.steps.CloneStep;
import org.elasticsearch.datastreams.lifecycle.transitions.steps.ForceMergeStep;
import org.elasticsearch.datastreams.lifecycle.transitions.steps.ReadOnlyStep;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_BLOCKS_WRITE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

// DEBUG is a good thing to leave this at, because TRACE can be quite chatty
@TestLogging(value = "org.elasticsearch.datastreams.lifecycle:DEBUG", reason = "extra logging for DLM")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 2)
public class DLMServiceFrozenActionIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DLMServiceFrozenActionIT.class);

    private static TimeValue timeForStep = null;
    private final static AtomicBoolean advanceAfterReadOnly = new AtomicBoolean(false);
    private final static AtomicBoolean advanceAfterClone = new AtomicBoolean(false);

    @Before
    public void setup() {
        // Reset time for step invocation back to null so that it will not advance to the frozen action
        timeForStep = null;
        // Reset no-ops to block execution
        advanceAfterReadOnly.set(false);
        advanceAfterClone.set(false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            TestDataStreamsPluginWithActions.class,
            MockTransportService.TestPlugin.class,
            DataStreamLifecycleServiceIT.TestSystemDataStreamPlugin.class,
            MapperExtrasPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), "1s")
            .build();
    }

    public void testEndToEnd() throws Exception {
        final String DATASTREAM = "foo-data";

        logger.info("==> putting template in place");
        addIndexTemplate(DATASTREAM);

        int numDocs = randomIntBetween(100, 200);
        logger.info("==> indexing [{}] documents", numDocs);
        final String backingIndex = indexDocuments(DATASTREAM, numDocs);

        logger.info("==> forcing rollover");
        assertAcked(client().execute(RolloverAction.INSTANCE, new RolloverRequest(DATASTREAM, null)));

        logger.info("==> executing read only step");
        // Update the time for the step so that it starts to be executed
        timeForStep = TimeValue.timeValueDays(0);
        assertIndexBlocked(backingIndex);

        logger.info("==> executing clone step");
        advanceAfterReadOnly.set(true);
        String cloneIndexName = CloneStep.getDLMCloneIndexName(backingIndex);
        logger.info("==> waiting for cloned merge index [{}] to exist", cloneIndexName);
        assertIndexExists(cloneIndexName);

        logger.info("==> executing force merge step");
        advanceAfterClone.set(true);
        logger.info("==> waiting for cloned merge index [{}] to have 1 segment per shard", cloneIndexName);
        assertSingleSegmentIndex(cloneIndexName);
    }

    private void addIndexTemplate(final String dataStreamName) {
        TransportPutComposableIndexTemplateAction.Request req = new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "-template");
        req.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .template(
                    Template.builder()
                        .lifecycle(
                            DataStreamLifecycle.dataLifecycleBuilder()
                                .dataRetention(TimeValue.timeValueDays(30))
                                .frozenAfter(TimeValue.timeValueDays(1))
                                .enabled(true)
                        )
                        .settings(
                            Settings.builder()
                                // Random number of primary shards
                                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 4))
                                // Use at least 1 replica explicitly, so that a clone always happens
                                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "1-2")
                        )
                        .build()
                )
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, req));
    }

    /**
     * Indexes a random number of documents, randomly flushing and refreshing, returning the backing index that was used.
     */
    private String indexDocuments(final String dataStreamName, final int numDocs) {
        String index = null;
        for (int i = 0; i < numDocs; i++) {
            DocWriteResponse response = create(dataStreamName, "id-" + i, """
                {"@timestamp": "2026-03-03"}
                """);
            index = response.getIndex();
            // We randomly flush and refresh so that multiple segments are created
            if (i % 15 == 0) {
                refresh(dataStreamName);
                if (randomBoolean()) {
                    flush(dataStreamName);
                }
            }
        }
        return index;
    }

    private void assertIndexBlocked(final String indexName) throws Exception {
        assertBusy(() -> {
            GetSettingsResponse settingsResp = client().admin()
                .indices()
                .getSettings(new GetSettingsRequest(TimeValue.MAX_VALUE).indices(indexName))
                .get();
            boolean indexBlocked = INDEX_BLOCKS_WRITE_SETTING.get(settingsResp.getIndexToSettings().get(indexName));
            logger.info("--> waiting for [{}] to be blocked: [{}]", indexName, indexBlocked);
            assertTrue(indexBlocked);
        }, 30, TimeUnit.SECONDS);
    }

    private void assertIndexExists(final String indexName) throws Exception {
        assertBusy(() -> {
            try {
                logger.info("--> checking for [{}] to exist", indexName);
                var resp = client().admin().indices().resolveIndex(new ResolveIndexAction.Request(new String[] { indexName })).get();
                assertEquals(1, resp.getIndices().size());
            } catch (Exception e) {
                Throwable realException = ExceptionsHelper.unwrap(e, IndexNotFoundException.class);
                if (realException != null) {
                    logger.info("--> [{}] does not exist", indexName);
                    fail("expected " + indexName + " to exist but it does not");
                } else {
                    logger.error("--> unexpected exception", e);
                    throw e;
                }
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void assertSingleSegmentIndex(final String indexName) throws Exception {
        assertBusy(() -> {
            try {
                logger.info("--> checking segment count of [{}]", indexName);
                IndicesStatsResponse stats = client().admin().indices().stats(new IndicesStatsRequest().segments(true)).get();
                for (ShardStats shardStats : stats.getShards()) {
                    assertNotNull(shardStats.getStats().getSegments());
                    long segCount = shardStats.getStats().getSegments().getCount();
                    logger.info("--> segment count: [{}]", segCount);
                    assertTrue("expected 0 or 1 segments but was " + segCount, segCount <= 1);
                }
            } catch (IndexNotFoundException e) {
                logger.info("--> [{}] does not exist", indexName);
                fail("expected " + indexName + " to exist but it does not");
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Behaves just like the existing DataStreamsPlugin, but injects the frozen DLM action
     */
    public static class TestDataStreamsPluginWithActions extends DataStreamsPlugin {

        public TestDataStreamsPluginWithActions(Settings settings) {
            super(settings);
        }

        @Override
        protected List<DlmAction> allDLMActions() {
            return List.of(new TestFrozenAction("frozen_after"));
        }
    }

    // TODO: extend real frozen action, changing only what is necessary for testing, once it exists
    private static class TestFrozenAction implements DlmAction {
        private final String name;

        TestFrozenAction(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Function<DataStreamLifecycle, TimeValue> applyAfterTime() {
            return (lifecycle) -> timeForStep;
        }

        @Override
        public List<DlmStep> steps() {
            // Here we interject a TestNoOpWaitingStep in between each "real"
            // step so that the invocation can be controlled. It's not strictly
            // necessary, but it would allow us to pause and do things in
            // between each step in case we want to test failure, or manually
            // mess with state.
            var readOnlyStep = new ReadOnlyStep();
            var cloneStep = new CloneStep();
            var forceMergeStep = new ForceMergeStep();
            return List.of(
                readOnlyStep,
                new TestNoOpWaitingStep(readOnlyStep, advanceAfterReadOnly),
                cloneStep,
                new TestNoOpWaitingStep(cloneStep, advanceAfterClone),
                forceMergeStep
            );
        }

        @Override
        public boolean canRunOnProject(DlmActionContext dlmActionContext) {
            return true;
        }
    }

    /**
     * This step is a special step that allows us to control when it "advances".
     */
    private static class TestNoOpWaitingStep implements DlmStep {
        private final DlmStep delegate;
        private final AtomicBoolean advance;

        public TestNoOpWaitingStep(DlmStep delegate, AtomicBoolean advance) {
            this.delegate = delegate;
            this.advance = advance;
        }

        @Override
        public boolean stepCompleted(Index index, ProjectState projectState) {
            return advance.get();
        }

        @Override
        public void execute(DlmStepContext dlmStepContext) {
            // No-op
            if (advance.get() == false) {
                logger.info("--> no-op blocking [{}]", stepName());
            }
        }

        @Override
        public String stepName() {
            return "Post " + delegate.stepName() + " No-Op Waiting Step";
        }

        @Override
        public List<String> possibleOutputIndexNamePatterns(String indexName) {
            return delegate.possibleOutputIndexNamePatterns(indexName);
        }
    }
}
