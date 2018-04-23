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

package org.elasticsearch.indices.state;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.freeze.FreezeIndexResponse;
import org.elasticsearch.action.admin.indices.unfreeze.UnfreezeIndexResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class FreezeUnfreezeIndexIT extends ESIntegTestCase {
    public void testSimpleFreezeUnfreeze() {
        Client client = client();
        createIndex("test1");
        waitForRelocation(ClusterHealthStatus.GREEN);

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test1").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1");
    }

    public void testSimpleFreezeMissingIndex() {
        Client client = client();
        Exception e = expectThrows(IndexNotFoundException.class, () ->
                client.admin().indices().prepareFreeze("test1").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testSimpleUnfreezeMissingIndex() {
        Client client = client();
        Exception e = expectThrows(IndexNotFoundException.class, () ->
                client.admin().indices().prepareUnfreeze("test1").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testFreezeOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        waitForRelocation(ClusterHealthStatus.GREEN);
        Exception e = expectThrows(IndexNotFoundException.class, () ->
                client.admin().indices().prepareFreeze("test1", "test2").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testFreezeOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        waitForRelocation(ClusterHealthStatus.GREEN);
        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test1", "test2")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");
    }

    public void testUnfreezeOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        waitForRelocation(ClusterHealthStatus.GREEN);
        Exception e = expectThrows(IndexNotFoundException.class, () ->
                client.admin().indices().prepareUnfreeze("test1", "test2").execute().actionGet());
        assertThat(e.getMessage(), is("no such index"));
    }

    public void testUnfreezeOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        waitForRelocation(ClusterHealthStatus.GREEN);
        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test1", "test2")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1");
    }

    public void testFreezeUnfreezeMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        waitForRelocation(ClusterHealthStatus.GREEN);

        FreezeIndexResponse freezeIndexResponse1 = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse1.isAcknowledged(), equalTo(true));
        FreezeIndexResponse freezeIndexResponse2 = client.admin().indices().prepareFreeze("test2").execute().actionGet();
        assertThat(freezeIndexResponse2.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2");
        assertIndexIsUnfrozen("test3");

        UnfreezeIndexResponse unfreezeIndexResponse1 = client.admin().indices().prepareUnfreeze("test1").execute().actionGet();
        assertThat(unfreezeIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse1.isShardsAcknowledged(), equalTo(true));
        UnfreezeIndexResponse unfreezeIndexResponse2 = client.admin().indices().prepareUnfreeze("test2").execute().actionGet();
        assertThat(unfreezeIndexResponse2.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse2.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1", "test2", "test3");
    }

    public void testFreezeUnfreezeWildcard() {
        Client client = client();
        createIndex("test1", "test2", "a");
        waitForRelocation(ClusterHealthStatus.GREEN);

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test*").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2");
        assertIndexIsUnfrozen("a");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test*").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1", "test2", "a");
    }

    public void testFreezeUnfreezeAll() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        waitForRelocation(ClusterHealthStatus.GREEN);

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("_all").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2", "test3");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("_all").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1", "test2", "test3");
    }

    public void testFreezeUnfreezeAllWildcard() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        waitForRelocation(ClusterHealthStatus.GREEN);

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("*").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2", "test3");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("*").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1", "test2", "test3");
    }

    public void testFreezeNoIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
                client.admin().indices().prepareFreeze().execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testFreezeNullIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
                client.admin().indices().prepareFreeze((String[])null).execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testUnfreezeNoIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
                client.admin().indices().prepareUnfreeze().execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testUnfreezeNullIndex() {
        Client client = client();
        Exception e = expectThrows(ActionRequestValidationException.class, () ->
                client.admin().indices().prepareUnfreeze((String[])null).execute().actionGet());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testUnfreezeAlreadyUnfrozenIndex() {
        Client client = client();
        createIndex("test1");
        waitForRelocation(ClusterHealthStatus.GREEN);

        //no problem if we try to unfreeze an index that's already in Unfrozen state
        UnfreezeIndexResponse unfreezeIndexResponse1 = client.admin().indices().prepareUnfreeze("test1").execute().actionGet();
        assertThat(unfreezeIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse1.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1");
    }

    public void testFreezeAlreadyFrozenIndex() {
        Client client = client();
        createIndex("test1");
        waitForRelocation(ClusterHealthStatus.GREEN);

        //freezing the index
        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");

        //no problem if we try to freeze an index that's already in frozen state
        freezeIndexResponse = client.admin().indices().prepareFreeze("test1").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");
    }

    public void testSimpleFreezeUnfreezeAlias() {
        Client client = client();
        createIndex("test1");
        waitForRelocation(ClusterHealthStatus.GREEN);

        IndicesAliasesResponse aliasesResponse = client.admin().indices()
            .prepareAliases().addAlias("test1", "test1-alias").execute().actionGet();
        assertThat(aliasesResponse.isAcknowledged(), equalTo(true));

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test1-alias").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test1-alias").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1");
    }

    public void testFreezeUnfreezeAliasMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2");
        waitForRelocation(ClusterHealthStatus.GREEN);

        IndicesAliasesResponse aliasesResponse1 = client.admin().indices()
            .prepareAliases().addAlias("test1", "test-alias").execute().actionGet();
        assertThat(aliasesResponse1.isAcknowledged(), equalTo(true));
        IndicesAliasesResponse aliasesResponse2 = client.admin().indices()
            .prepareAliases().addAlias("test2", "test-alias").execute().actionGet();
        assertThat(aliasesResponse2.isAcknowledged(), equalTo(true));

        FreezeIndexResponse freezeIndexResponse = client.admin().indices().prepareFreeze("test-alias").execute().actionGet();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen("test1", "test2");

        UnfreezeIndexResponse unfreezeIndexResponse = client.admin().indices().prepareUnfreeze("test-alias").execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen("test1", "test2");
    }

    public void testUnfreezeWaitingForActiveShardsFailed() throws Exception {
        Client client = client();
        Settings settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                .build();
        assertAcked(client.admin().indices().prepareCreate("test").setSettings(settings).get());
        assertAcked(client.admin().indices().prepareFreeze("test").get());

        UnfreezeIndexResponse response = client.admin().indices().prepareUnfreeze("test").setTimeout("1ms").setWaitForActiveShards(2).get();
        assertThat(response.isShardsAcknowledged(), equalTo(false));
        assertBusy(() -> assertThat(client.admin().cluster().prepareState().get().getState().metaData().index("test").getState(),
                        equalTo(IndexMetaData.State.OPEN)));
        ensureGreen("test");
    }

    private static void assertIndexIsUnfrozen(String... indices) {
        checkIndexState(IndexMetaData.State.OPEN, indices);
    }

    private static void assertIndexIsFrozen(String... indices) {
        checkIndexState(IndexMetaData.State.FROZEN, indices);
    }

    private static void checkIndexState(IndexMetaData.State expectedState, String... indices) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
        for (String index : indices) {
            IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().indices().get(index);
            assertThat(indexMetaData, notNullValue());
            assertThat(indexMetaData.getState(), equalTo(expectedState));
        }
    }

    public void testFreezeUnfreezeWithDocs() throws IOException, ExecutionException, InterruptedException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().
                startObject().
                startObject("type").
                startObject("properties").
                startObject("test")
                .field("type", "keyword")
                .endObject().
                endObject().
                endObject()
                .endObject());

        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type", mapping, XContentType.JSON));
        waitForRelocation(ClusterHealthStatus.GREEN);
        int docs = between(10, 100);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs ; i++) {
            builder[i] = client().prepareIndex("test", "type", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder);
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("test").setForce(true).execute().get();
        }
        freeze("test");

        // check the index still contains the records that we indexed
        unfreeze("test");
        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchQuery("test", "init")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, docs);
    }

    public void testFreezeUnfreezeIndexWithBlocks() {
        createIndex("test");
        waitForRelocation(ClusterHealthStatus.GREEN);

        int docs = between(10, 100);
        for (int i = 0; i < docs ; i++) {
            client().prepareIndex("test", "type", "" + i).setSource("test", "init").execute().actionGet();
        }

        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);

                // Freezing an index is not blocked
                freeze("test");

                // Unfreezing an index is not blocked
                unfreeze("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Freezing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareFreeze("test"));
                assertIndexIsUnfrozen("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        freeze("test");

        // Unfreezing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_READ_ONLY_ALLOW_DELETE, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareUnfreeze("test"));
                assertIndexIsFrozen("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    public void testFreezeUsesFrozenThreadPool() throws Exception {
        Client client = client();
        createIndex("test1", "test2", "test3");
        waitForRelocation(ClusterHealthStatus.GREEN);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            builders.add(client.prepareIndex(randomFrom("test1", "test2", "test3"), "_doc", "" + i).
                setSource("{\"foo\": " + i + "}", XContentType.JSON));
        }
        indexRandom(true, builders);

        SearchResponse resp = client.prepareSearch("test*").setQuery(QueryBuilders.matchQuery("foo", 7)).get();
        assertSearchHits(resp, "7");

        long nonFrozenCompletedCounts = client.admin().cluster().prepareNodesStats().clear().setThreadPool(true).get()
            .getNodes().stream()
            .mapToLong(ns ->
                StreamSupport.stream(ns.getThreadPool().spliterator(), false)
                    .filter(stats -> stats.getName().equals(ThreadPool.Names.FROZEN))
                    .mapToLong(ThreadPoolStats.Stats::getCompleted)
                    .reduce(0, (count, comp) -> count + comp))
            .reduce(0, (count, comp) -> count + comp);

        freeze("test1", "test2");
        assertIndexIsUnfrozen("test3");

        // Yay functional programming
        long preFrozenCompletedCounts = client.admin().cluster().prepareNodesStats().clear().setThreadPool(true).get()
            .getNodes().stream()
            .mapToLong(ns ->
                StreamSupport.stream(ns.getThreadPool().spliterator(), false)
                .filter(stats -> stats.getName().equals(ThreadPool.Names.FROZEN))
                .mapToLong(ThreadPoolStats.Stats::getCompleted)
                .reduce(0, (count, comp) -> count + comp))
            .reduce(0, (count, comp) -> count + comp);

        assertThat(nonFrozenCompletedCounts, equalTo(preFrozenCompletedCounts));

        assertBusy(() -> {
            SearchResponse frozenResponse = client.prepareSearch("test*")
                .setQuery(QueryBuilders.matchQuery("foo", 7)).get();
            assertSearchHits(frozenResponse, "7");
        });

        long postFrozenCompletedCounts = client.admin().cluster().prepareNodesStats().clear().setThreadPool(true).get()
            .getNodes().stream()
            .mapToLong(ns ->
                StreamSupport.stream(ns.getThreadPool().spliterator(), false)
                    .filter(stats -> stats.getName().equals(ThreadPool.Names.FROZEN))
                    .mapToLong(ThreadPoolStats.Stats::getCompleted)
                    .reduce(0, (count, comp) -> count + comp))
            .reduce(0, (count, comp) -> count + comp);
        logger.info("--> pre-search completed: {} post-search completed: {}", preFrozenCompletedCounts, postFrozenCompletedCounts);

        assertThat("expected post frozen threadpool completed count to be higher",
            postFrozenCompletedCounts, greaterThan(preFrozenCompletedCounts));

        unfreeze("test1", "test2");
        assertIndexIsUnfrozen("test1", "test2", "test3");

        resp = client.prepareSearch("test*").setQuery(QueryBuilders.matchQuery("foo", 7)).get();
        assertSearchHits(resp, "7");
    }

    public void testFreezeUnfreezeThenIndex() throws Exception {
        Client client = client();
        createIndex("test");
        waitForRelocation(ClusterHealthStatus.GREEN);
        int docs = between(1, 10);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs ; i++) {
            builder[i] = client().prepareIndex("test", "type", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder);

        assertHitCount(client.prepareSearch("test").get(), docs);

        freeze("test");
        unfreeze("test");

        ensureGreen("test");
        client().prepareIndex("test", "type", "" + docs + 1).setSource("test", "foo").get();
        refresh("test");
        assertHitCount(client.prepareSearch("test").get(), docs + 1);
    }

    public void testSnapshotFrozenIndex() throws Exception {
        Client client = client();
        createIndex("test");
        waitForRelocation(ClusterHealthStatus.GREEN);
        int docs = between(10, 100);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs ; i++) {
            builder[i] = client().prepareIndex("test", "type", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder);

        assertHitCount(client.prepareSearch("test").get(), docs);

        freeze("test");

        Path snapPath = randomRepoPath();
        client.admin().cluster().preparePutRepository("fs").setType("fs")
            .setSettings(Settings.builder().put("location", snapPath.toAbsolutePath())).get();
        client.admin().cluster().prepareCreateSnapshot("fs", "snap1").setIndices("test").setWaitForCompletion(true).get();

        unfreeze("test");

        int moreDocs = between(2, 10);
        IndexRequestBuilder[] builder2 = new IndexRequestBuilder[moreDocs];
        for (int i = 0; i < moreDocs ; i++) {
            builder2[i] = client().prepareIndex("test", "type", "" + i).setSource("test", "init");
        }
        indexRandom(true, builder2);

        client.admin().indices().prepareClose("test").get();
        client.admin().cluster().prepareRestoreSnapshot("fs", "snap1").setWaitForCompletion(true).get();
        client.admin().indices().prepareOpen("test").get();
        ensureGreen("test");

        assertHitCount(client.prepareSearch("test").get(), docs);
    }

    public void testDeleteFrozenIndex() throws Exception {
        Client client = client();
        createIndex("test");
        waitForRelocation(ClusterHealthStatus.GREEN);

        freeze("test");

        assertAcked(client.admin().indices().prepareDelete("test"));
        // No weirdness with recreating the index with the same name
        createIndex("test");
    }

    public void testFreezeUnfreezeDuringSearch() throws Exception {
        Client client = client();
        createIndex("test", "test2", "test3");
        waitForRelocation(ClusterHealthStatus.GREEN);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            builders.add(client.prepareIndex(randomFrom("test1", "test2", "test3"), "_doc", "" + i).
                setSource("{\"foo\": " + i + "}", XContentType.JSON));
        }
        indexRandom(true, builders);

        final CountDownLatch searchStart = new CountDownLatch(1);
        final CountDownLatch freezeStart = new CountDownLatch(1);
        final AtomicBoolean keepSearching = new AtomicBoolean(true);
        Thread searchingThread = new Thread(() -> {
            try {
                searchStart.await();
            } catch (Exception e) {
                fail("got exception while waiting to start searching: " + e);
            }
            logger.info("--> searching thread starting");
            for (int i = 0; i < randomIntBetween(1, 10); i++) {
                SearchResponse frozenResponse = client.prepareSearch("test*")
                    .setQuery(QueryBuilders.matchQuery("foo", 7)).get();
                assertSearchHits(frozenResponse, "7");
            }
            // Start freezing
            freezeStart.countDown();
            while (keepSearching.get()) {
                SearchResponse frozenResponse = client.prepareSearch("test*")
                    .setQuery(QueryBuilders.matchQuery("foo", 7)).get();
                assertSearchHits(frozenResponse, "7");
            }
        });

        searchingThread.start();
        searchStart.countDown();
        freezeStart.await();

        freeze("test");

        unfreeze("test");
        ensureGreen("test");

        SearchResponse resp = client.prepareSearch("test*").setQuery(QueryBuilders.matchQuery("foo", 45)).get();
        assertSearchHits(resp, "45");

        keepSearching.set(false);
        searchingThread.join();
    }

    public void testFreezeAndIncreaseReplicas() throws Exception {
        assumeTrue("need to be able to increment replicas at least to 1 but there are only " +
                cluster().numDataNodes() + " data node(s)", cluster().numDataNodes() > 1);
        Client client = client();
        createIndex("test", Settings.builder().put("index.number_of_replicas", 0).build());
        ensureGreen("test");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        int docs = randomIntBetween(20, 40);
        for (int i = 0; i < docs; i++) {
            builders.add(client.prepareIndex("test", "_doc", "" + i).setSource("{\"foo\": " + i + "}", XContentType.JSON));
        }
        indexRandom(false, builders);

        freeze("test");

        int replicaCount = randomIntBetween(1, maximumNumberOfReplicas());
        logger.info("--> updating replicas to {}", replicaCount);
        client.admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.number_of_replicas", replicaCount).build()).get();
        ensureGreen("test");

        assertHitCount(client.prepareSearch("test").get(), docs);
    }

    private void freeze(String... indices) {
        logger.info("--> freezing [{}]", Strings.arrayToCommaDelimitedString(indices));
        FreezeIndexResponse freezeIndexResponse = client().admin().indices().prepareFreeze(indices).get();
        assertThat(freezeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsFrozen(indices);
    }

    private void unfreeze(String... indices) {
        logger.info("--> unfreezing [{}]", Strings.arrayToCommaDelimitedString(indices));
        UnfreezeIndexResponse unfreezeIndexResponse = client().admin().indices().prepareUnfreeze(indices).execute().actionGet();
        assertThat(unfreezeIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(unfreezeIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsUnfrozen(indices);
    }
}
