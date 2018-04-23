/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
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
    private final LocalCheckpointTracker localCheckpointTracker;
    private final String historyUUID;
    private SegmentInfos lastCommittedSegmentInfos;

    public LazyEngine(EngineConfig engineConfig) {
        super(engineConfig);

        // The deletion policy for the translog should not keep any translogs around, so the min age/size is set to -1
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(-1, -1);

        store.incRef();
        boolean success = false;
        Translog translog = null;

        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            translog =  openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier());
            assert translog.getGeneration() != null;
            this.translog = translog;
            List<IndexCommit> indexCommits = DirectoryReader.listCommits(store.directory());
            lastCommit = indexCommits.get(indexCommits.size()-1);
            historyUUID = lastCommit.getUserData().get(HISTORY_UUID_KEY);
            // We don't want any translogs hanging around for recovery, so we need to set these accordingly
            final long lastGen = Long.parseLong(lastCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
            translogDeletionPolicy.setTranslogGenerationOfLastCommit(lastGen);
            translogDeletionPolicy.setMinTranslogGenerationForRecovery(lastGen);

            searcherFactory = new RamAccountingSearcherFactory(engineConfig.getCircuitBreakerService());
            localCheckpointTracker = createLocalCheckpointTracker();
            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(translog);
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new LazyEngine");
    }

    private Translog openTranslog(EngineConfig engineConfig, TranslogDeletionPolicy translogDeletionPolicy,
                                  LongSupplier globalCheckpointSupplier) throws IOException {
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        final String translogUUID = loadTranslogUUIDFromLastCommit();
        // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier,
            engineConfig.getPrimaryTermSupplier());
    }

    /**
     * Reads the current stored translog ID from the last commit data.
     */
    @Nullable
    private String loadTranslogUUIDFromLastCommit() throws IOException {
        final Map<String, String> commitUserData = lastCommittedSegmentInfos.getUserData();
        if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
            throw new IllegalStateException("commit doesn't contain translog generation id");
        }
        return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
    }

    private LocalCheckpointTracker createLocalCheckpointTracker() throws IOException {
        final long maxSeqNo;
        final long localCheckpoint;
        final SequenceNumbers.CommitInfo seqNoStats =
            SequenceNumbers.loadSeqNoInfoFromLuceneCommit(lastCommittedSegmentInfos.userData.entrySet());
        maxSeqNo = seqNoStats.maxSeqNo;
        localCheckpoint = seqNoStats.localCheckpoint;
        logger.trace("recovered maximum sequence number [{}] and local checkpoint [{}]", maxSeqNo, localCheckpoint);
        return new LocalCheckpointTracker(maxSeqNo, localCheckpoint);
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
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

    @SuppressForbidden(reason = "needed to keep searcher open if more than one search ongoing")
    private IndexSearcher getOrCreateSearcher() throws IOException {
        synchronized (searcherFactory) {
            if (currentReader == null || currentReader.tryIncRef() == false) {
                currentReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(lastCommit), shardId);
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
            final IndexSearcher searcher = getOrCreateSearcher();
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
                @SuppressForbidden(reason = "needed to release reader after the query has completed")
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
                        /* In general, searchers should never be released twice or this would break
                         * reference counting. There is one rare case when it might happen though:
                         * when the request and the Reaper thread would both try to release it in a
                         * very short amount of time, this is why we only log a warning instead of
                         * throwing an exception.
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
        return localCheckpointTracker;
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
        try {
            translog.rollGeneration();
            translog.trimUnreferencedReaders();
        } catch (IOException e) {
            maybeFailEngine("flush", e);
            throw new FlushFailedEngineException(shardId, e);
        }
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public void trimTranslog() throws EngineException {
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade,
                           boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return new Engine.IndexCommitRef(lastCommit, () -> {});
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return acquireLastIndexCommit(false);
    }

    /**
     * Closes the engine without acquiring the write lock. This should only be
     * called while the write lock is hold or in a disaster condition ie. if the engine
     * is failed.
     */
    @Override
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() :
                "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                IOUtils.close(translog);
            } catch (Exception e) {
                logger.warn("Failed to close translog", e);
            } finally {
                try {
                    store.decRef();
                    logger.debug("engine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
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
