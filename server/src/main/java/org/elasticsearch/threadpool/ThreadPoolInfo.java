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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class ThreadPoolInfo implements Writeable, Iterable<ThreadPool.Info>, ToXContentFragment {

    private final List<ThreadPool.Info> infos;

    public ThreadPoolInfo(List<ThreadPool.Info> infos) {
        List<ThreadPool.Info> copy = new ArrayList<>(infos);
        copy.sort(Comparator.comparing(ThreadPool.Info::getName));
        this.infos = Collections.unmodifiableList(copy);
    }

    public ThreadPoolInfo(StreamInput in) throws IOException {
        this(in.readList(ThreadPool.Info::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(infos);
    }

    @Override
    public Iterator<ThreadPool.Info> iterator() {
        return infos.iterator();
    }

    public static ThreadPoolInfo fromXContent(XContentParser parser) throws IOException {
        List<ThreadPool.Info> infos = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            infos.add(ThreadPool.Info.fromXContent(parser));
        }
        return new ThreadPoolInfo(infos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (ThreadPool.Info info : infos) {
            info.toXContent(builder, params);
        }
        return builder;
    }
}
