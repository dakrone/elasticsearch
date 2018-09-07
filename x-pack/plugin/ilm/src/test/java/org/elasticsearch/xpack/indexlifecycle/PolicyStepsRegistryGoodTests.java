/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;

import java.util.Arrays;
import java.util.Collection;

public class PolicyStepsRegistryGoodTests extends ESSingleNodeTestCase {

    /** The plugin classes that should be added to the node. */
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    @TestLogging("logger.org.elasticsearch.xpack.indexlifecycle:TRACE,logger.org.elasticsearch.xpack.core.indexlifecycle:TRACE")
    public void testIt() throws Exception {
        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicy("policy");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(policy);
        client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
    }
}
