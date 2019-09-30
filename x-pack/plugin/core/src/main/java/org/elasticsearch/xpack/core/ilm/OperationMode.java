/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import java.util.Optional;

/**
 * Enum representing the different modes that Index Lifecycle Service can operate in.
 */
public enum OperationMode {
    /**
     * This represents a state where no policies are executed
     */
    STOPPED {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == RUNNING;
        }
    },

    /**
     * this represents a state where only sensitive actions (like {@link ShrinkAction}) will be executed
     * until they finish, at which point the operation mode will move to <code>STOPPED</code>.
     */
    STOPPING {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == RUNNING || nextMode == STOPPED;
        }
    },

    /**
     * Normal operation where all policies are executed as normal.
     */
    RUNNING {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == STOPPING;
        }
    };

    public abstract boolean isValidChange(OperationMode nextMode);

    /**
     * Returns true if ILM is in the stopped or stopped state
     */
    public static boolean ilmStoppedOrStopping(ClusterState state) {
        return Optional.ofNullable((IndexLifecycleMetadata) state.metaData().custom(IndexLifecycleMetadata.TYPE))
            .map(IndexLifecycleMetadata::getOperationMode)
            .map(mode -> OperationMode.STOPPING == mode || OperationMode.STOPPED == mode)
            .orElse(false);
    }

    /**
     * Returns true if SLM is in the stopped or stopped state
     */
    public static boolean slmStoppedOrStopping(ClusterState state) {
        return Optional.ofNullable((SnapshotLifecycleMetadata) state.metaData().custom(SnapshotLifecycleMetadata.TYPE))
            .map(SnapshotLifecycleMetadata::getOperationMode)
            .map(mode -> OperationMode.STOPPING == mode || OperationMode.STOPPED == mode)
            .orElse(false);
    }
}
