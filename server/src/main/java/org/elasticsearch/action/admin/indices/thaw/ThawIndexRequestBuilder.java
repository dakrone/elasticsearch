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

package org.elasticsearch.action.admin.indices.thaw;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for thaw index request
 */
public class ThawIndexRequestBuilder extends
    AcknowledgedRequestBuilder<ThawIndexRequest, ThawIndexResponse, ThawIndexRequestBuilder> {

    public ThawIndexRequestBuilder(ElasticsearchClient client, ThawIndexAction action) {
        super(client, action, new ThawIndexRequest());
    }

    public ThawIndexRequestBuilder(ElasticsearchClient client, ThawIndexAction action, String... indices) {
        super(client, action, new ThawIndexRequest(indices));
    }

    /**
     * Sets the indices to be frozen
     *
     * @param indices the indices to be frozen
     * @return the request itself
     */
    public ThawIndexRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and indices wildcard expressions
     * @return the request itself
     */
    public ThawIndexRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
