/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public class PhaseCompleteStepTests extends AbstractStepTestCase<PhaseCompleteStep> {

    @Override
    public PhaseCompleteStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        return new PhaseCompleteStep(stepKey);
    }

    @Override
    public PhaseCompleteStep mutateInstance(PhaseCompleteStep instance) {
        StepKey key = instance.getKey();
        key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));

        return new PhaseCompleteStep(key);
    }

    @Override
    public PhaseCompleteStep copyInstance(PhaseCompleteStep instance) {
        return new PhaseCompleteStep(instance.getKey());
    }
}
