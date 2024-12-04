/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rename;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportRenameIndexAction extends TransportMasterNodeAction<RenameIndexRequest, AcknowledgedResponse> {
    public static final String NAME = "indices:admin/rename";
    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>(NAME);
    private static final Logger logger = LogManager.getLogger(TransportRenameIndexAction.class);

    @Inject
    protected TransportRenameIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RenameIndexRequest::new,
            indexNameExpressionResolver,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(Task task, RenameIndexRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {

    }

    @Override
    protected ClusterBlockException checkBlock(RenameIndexRequest request, ClusterState state) {
        return null;
    }
}
