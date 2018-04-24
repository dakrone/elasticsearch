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

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public class LazyEngine extends Engine {
    private final Translog translog;
    private final IndexCommit lastCommit;
    private SegmentInfos lastCommittedSegmentInfos;

    protected LazyEngine(EngineConfig engineConfig) {
        super(engineConfig);
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(
            engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
            engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis()
        );
        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            this.translog =  openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier());
            assert translog.getGeneration() != null;
            List<IndexCommit> indexCommits = DirectoryReader.listCommits(store.directory());
            lastCommit = indexCommits.get(indexCommits.size()-1);
            searcherFactory = new RamAccountingSearcherFactory(engineConfig.getCircuitBreakerService());
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } catch (AssertionError e) {
            // IndexWriter throws AssertionError on init, if asserts are enabled, if any files don't exist, but tests that
            // randomly throw FNFE/NSFE can also hit this:
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            } else {
                throw e;
            }
        }
    }

    private Translog openTranslog(EngineConfig engineConfig, TranslogDeletionPolicy translogDeletionPolicy, LongSupplier globalCheckpointSupplier) throws IOException {
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        final String translogUUID = loadTranslogUUIDFromLastCommit();
        // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier, engineConfig.getPrimaryTermSupplier());
    }

    /**
     * Reads the current stored translog ID from the last commit data.
     */
    @Nullable
    private String loadTranslogUUIDFromLastCommit() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().getUserData();
        if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
            throw new IllegalStateException("commit doesn't contain translog generation id");
        }
        return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return null;
    }

    @Override
    public String getHistoryUUID() {
        return null;
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        throw new UnsupportedOperationException(); // TODO fix this
    }

    private DirectoryReader currentReader; // this reader might be closed but we still hold the reference
    private final SearcherFactory searcherFactory;

    private IndexSearcher getOrCreateSearcher() throws IOException {
        synchronized (searcherFactory) {
            if (currentReader != null || currentReader.tryIncRef() == false) {
                currentReader = DirectoryReader.open(lastCommit);
                currentReader.getReaderCacheHelper().addClosedListener(key -> {
                    synchronized (searcherFactory) {
                        if (this.currentReader.getReaderCacheHelper().getKey() == key) {
                            this.currentReader = null; // null it out on close
                        }
                    }
                });
            }
            return searcherFactory.newSearcher(currentReader, null);
        }
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        boolean success = false;
        store.incRef();
        try {
            final IndexSearcher searcher = searcherFactory.newSearcher(currentReader, null);
            Searcher engineSearcher = new Searcher(source, null) {
                private volatile IndexSearcher lazySearcher = searcher;
                private final AtomicBoolean released = new AtomicBoolean(false);

                @Override
                public IndexReader reader() {
                    return searcher().getIndexReader();
                }

                @Override
                public synchronized IndexSearcher searcher() {
                    if (lazySearcher == null) {
                        try {
                            lazySearcher = getOrCreateSearcher();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                    return lazySearcher;
                }

                @Override
                public synchronized void releaseResources() throws IOException {
                    final IndexSearcher lazySearcher = this.lazySearcher;
                    if (lazySearcher != null) {
                        this.lazySearcher = null;
                        lazySearcher.getIndexReader().decRef();
                    }
                }

                @Override
                public void close() {
                    if (!released.compareAndSet(false, true)) {
                        /* In general, searchers should never be released twice or this would break reference counting. There is one rare case
                         * when it might happen though: when the request and the Reaper thread would both try to release it in a very short amount
                         * of time, this is why we only log a warning instead of throwing an exception.
                         */
                        logger.warn("Searcher was released twice", new IllegalStateException("Double release"));
                        return;
                    }
                    try {
                        releaseResources();
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot close", e);
                    } catch (AlreadyClosedException e) {
                        // This means there's a bug somewhere: don't suppress it
                        throw new AssertionError(e);
                    } finally {
                        store.decRef();
                    }
                }
            };
            success = true;
            return engineSearcher;
        } catch (IOException ex) {
            ensureOpen(ex); // throw EngineCloseException here if we are already closed
            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to acquire searcher, source {}", source), ex);
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            if (success == false) {
                store.decRef();
            }
        }
    }

    @Override
    public Translog getTranslog() {
        return translog;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        return false;
    }

    @Override
    public void syncTranslog() throws IOException {
    }

    @Override
    public LocalCheckpointTracker getLocalCheckpointTracker() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return Arrays.asList(getSegmentInfo(lastCommittedSegmentInfos, verbose));
    }

    @Override
    public void refresh(String source) throws EngineException {

    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public CommitId flush() throws EngineException {
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public void trimTranslog() throws EngineException {
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return new Engine.IndexCommitRef(lastCommit, () -> {});
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return acquireLastIndexCommit(false);
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {

    }

    @Override
    public void activateThrottling() {
        throw new UnsupportedOperationException("lazy engine can't throttle");
    }

    @Override
    public void deactivateThrottling() {
        throw new UnsupportedOperationException("lazy engine can't throttle");
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() throws IOException {

    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public Engine recoverFromTranslog() throws IOException {
        return this;
    }

    @Override
    public void skipTranslogRecovery() {
    }

    @Override
    public void maybePruneDeletes() {
    }
}
