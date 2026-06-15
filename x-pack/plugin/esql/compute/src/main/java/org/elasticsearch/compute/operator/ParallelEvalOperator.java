/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: make Accountable also?
public class ParallelEvalOperator implements Operator {
    //
    private final ExchangeBuffer in;
    private final List<EvalOperator> workers;
    private final Executor executor;
    private final FailureCollector failureCollector = new FailureCollector();

    private final AtomicInteger runningWorkerTasks;
    private final SubscribableListener<Void> allWorkersDone = new SubscribableListener<>();

    private boolean finishCalled = false;
    private volatile boolean closed = false;
    /**
     * True once the results phase (feeding per-worker output pages into a stack) has completed.
     */
    private boolean resultsDone = false;
    private final LinkedList<Page> results = new LinkedList<>();

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(EvalOperator.class);

    private final DriverContext driverContext;
    private final List<List<Page>> workerOutputs;

    public ParallelEvalOperator(
        EvalOperator.ParallelWorkerConfig config,
        DriverContext driverContext,
        EvalOperator.EvalOperatorFactory factory,
        EvalOperator initialWorker
    ) {
        this.in = new ExchangeBuffer(config.maxInFlightPages());
        this.driverContext = driverContext;
        this.executor = config.executor();

        int backgroundWorkerCount = config.workerCount();
        if (backgroundWorkerCount < 1) {
            throw new IllegalArgumentException(
                "ParallelEvalOperator requires at least one background worker, got " + backgroundWorkerCount
            );
        }
        this.workers = new ArrayList<>(backgroundWorkerCount);
        this.workerOutputs = new ArrayList<>(backgroundWorkerCount);
        for (int i = 0; i < backgroundWorkerCount; i++) {
            workers.add(factory.getWorkerOperator(driverContext));
            workerOutputs.add(new ArrayList<>());
        }

        this.runningWorkerTasks = new AtomicInteger(backgroundWorkerCount);
        for (int i = 0; i < workers.size(); i++) {
            driverContext.addAsyncAction();
            scheduleWorker(workers.get(i), workerOutputs.get(i));
        }
    }

    private void scheduleWorker(EvalOperator worker, List<Page> workerOutput) {
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                runWorker(worker, workerOutput);
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                in.finish(true);
                workerPermanentlyExited(worker, true);
            }
        });
    }

    /**
     * Called when a background worker has permanently stopped (either normally or due to an
     * error). Closes the worker (safe because we are on the worker thread that owns its
     * {@link org.elasticsearch.compute.data.LocalCircuitBreaker}), then decrements the
     * running-worker counter, and notifies the driver thread when all workers are done.
     *
     * @param abort true when called from an error/rejection handler, meaning {@link EvalOperator#finish}
     *              may not have been called yet and we must call it before closing.
     */
    private void workerPermanentlyExited(EvalOperator worker, boolean abort) {
        if (abort) {
            try {
                worker.finish();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        try {
            worker.close();
        } catch (Exception e) {
            failureCollector.unwrapAndCollect(e);
        }

        if (runningWorkerTasks.decrementAndGet() == 0) {
            allWorkersDone.onResponse(null);
        }
        driverContext.removeAsyncAction();
    }

    private void runWorker(EvalOperator worker, List<Page> workerOutput) {
        try {
            Page page;
            while (closed == false && (page = in.pollPage()) != null) {
                worker.addInput(page);
                Page outputPage = worker.getOutput();
                if (outputPage != null) {
                    try {
                        outputPage.allowPassingToDifferentDriver();
                    } catch (Exception e) {
                        outputPage.releaseBlocks();
                        throw e;
                    }
                    if (closed) {
                        outputPage.releaseBlocks();
                    } else {
                        workerOutput.add(outputPage);
                    }
                }
            }
        } catch (Exception e) {
            failureCollector.unwrapAndCollect(e);
            in.finish(true);
            workerPermanentlyExited(worker, true);
            return;
        }

        if (in.noMoreInputs() || closed) {
            // Finished draining (or aborting). Produce output pages and transfer them to the driver.
            boolean abort = closed || failureCollector.hasFailure();
            workerPermanentlyExited(worker, abort);
        } else {
            // Buffer temporarily empty; reschedule when more pages arrive or buffer finishes.
            in.waitForReading().listener().addListener(ActionListener.running(() -> scheduleWorker(worker, workerOutput)));
        }
    }

    @Override
    public boolean needsInput() {
        return finishCalled == false && in.waitForWriting() == Operator.NOT_BLOCKED;
    }

    @Override
    public void addInput(Page page) {
        page.allowPassingToDifferentDriver();
        in.addPage(page);
    }

    @Override
    public void finish() {
        if (finishCalled == false) {
            finishCalled = true;
            in.finish(false);
        }
    }

    @Override
    public boolean isFinished() {
        // TODO: previously this checked for the merge worker, but there is no such operator for eval, does this work?
        return in.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        // TODO: not sure how to define this
        return false;
    }

    @Override
    public Page getOutput() {
        if (failureCollector.hasFailure()) {
            // Release any pages accumulated before throwing.
            for (List<Page> pages : workerOutputs) {
                for (Page p : pages) {
                    p.releaseBlocks();
                }
                pages.clear();
            }
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        if (allWorkersDone.isDone() == false) {
            return null;
        }
        if (resultsDone == false) {
            // Feed all pages produced by background workers into a stack so that we can easily retrieve them.
            // Pages are removed
            // from the list before addInput so that: (a) on success, the list is empty when we
            // finish and close() has nothing to double-release; (b) on exception, the throwing
            // page is already cleaned up by TopNOperator.addInput's finally block and the
            // remaining pages stay in the list for close() to release.
            for (List<Page> pages : workerOutputs) {
                var it = pages.iterator();
                while (it.hasNext()) {
                    Page p = it.next();
                    it.remove();
                    results.add(p);
                }
            }
            resultsDone = true;
        }
        return results.pollFirst();
    }

    @Override
    public String toString() {
        return "ParallelEvalOperator[workers=" + (workers.size() + 1) + "]";
    }

    @Override
    public void close() {
        closed = true;
        in.finish(true);
        // Release any merge pages already queued; workers still running will release their own.
        for (List<Page> pages : workerOutputs) {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            pages.clear();
        }
        // Close mergeTarget here (driver thread). Background workers close themselves.
        // TODO: what do I need to do to close background workers?
        // mergeTarget.close(); -- this doesn't work, we don't have a merge target
    }
}
