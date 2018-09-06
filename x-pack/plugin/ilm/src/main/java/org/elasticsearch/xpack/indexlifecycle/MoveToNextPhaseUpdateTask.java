/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.function.LongSupplier;

public class MoveToNextPhaseUpdateTask extends ClusterStateUpdateTask {
    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private final PolicyStepsRegistry stepsRegistry;
    private final Client client;
    private final LongSupplier nowSupplier;

    public MoveToNextPhaseUpdateTask(Index index, String policy, PolicyStepsRegistry policyStepsRegistry, Step.StepKey currentStepKey,
                                     Client client, LongSupplier nowSupplier) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.stepsRegistry = policyStepsRegistry;
        this.client = client;
        this.nowSupplier = nowSupplier;
    }

    Index getIndex() {
        return index;
    }

    String getPolicy() {
        return policy;
    }

    Step.StepKey getCurrentStepKey() {
        return currentStepKey;
    }

    @Override
    public ClusterState execute(ClusterState currentState) {
        IndexMetaData indexMetaData = currentState.getMetaData().index(index);
        if (indexMetaData == null) {
            // Index must have been since deleted, ignore it
            return currentState;
        }
        Settings indexSettings = indexMetaData.getSettings();
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
            && currentStepKey.equals(IndexLifecycleRunner.getCurrentStepKey(indexSettings))) {
            return IndexLifecycleRunner.moveClusterStateToNextPhase(index, currentState,
                stepsRegistry, currentStepKey, client, nowSupplier);
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        throw new ElasticsearchException("policy [" + policy + "] for index [" + index.getName() +
            "] failed trying to move to phase after [" + currentStepKey, e);
    }
}
