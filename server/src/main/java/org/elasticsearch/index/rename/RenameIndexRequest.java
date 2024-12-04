/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rename;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

public class RenameIndexRequest extends MasterNodeRequest<RenameIndexRequest> {
    private String index;
    private String newIndex;

    public RenameIndexRequest(TimeValue masterNodeTimeout, String index, String newIndex) {
        super(masterNodeTimeout);
        this.index = index;
        this.newIndex = newIndex;
    }

    public RenameIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.newIndex = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        // TODO: validate
        return null;
    }

    public String newIndex() {
        return newIndex;
    }

    public String index() {
        return index;
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.index);
        out.writeString(this.newIndex);
    }
}
