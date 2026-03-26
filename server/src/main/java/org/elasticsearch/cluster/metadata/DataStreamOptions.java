/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.DataStreamFailureStore.FAILURE_STORE;

/**
 * Holds data stream dedicated configuration options such as failure store, (in the future lifecycle). Currently, it
 * supports the following configurations:
 * - failure store
 */
public record DataStreamOptions(@Nullable DataStreamFailureStore failureStore, @Nullable Boolean relaxRestrictions)
    implements
        SimpleDiffable<DataStreamOptions>,
        ToXContentObject {

    private static final TransportVersion RELAX_RESTRICTIONS_VERSION = TransportVersion.fromName("data_stream_relax_restrictions");
    private static final TransportVersion INTRODUCE_FAILURES_LIFECYCLE = TransportVersion.fromName("introduce_failures_lifecycle");

    public static final ParseField FAILURE_STORE_FIELD = new ParseField(FAILURE_STORE);
    public static final ParseField RELAX_RESTRICTIONS = new ParseField("relax_restrictions");
    public static final DataStreamOptions FAILURE_STORE_ENABLED = new DataStreamOptions(new DataStreamFailureStore(true, null), null);
    public static final DataStreamOptions FAILURE_STORE_DISABLED = new DataStreamOptions(new DataStreamFailureStore(false, null), null);
    public static final DataStreamOptions EMPTY = new DataStreamOptions(null, null);

    public static final ConstructingObjectParser<DataStreamOptions, Void> PARSER = new ConstructingObjectParser<>(
        "options",
        false,
        (args, unused) -> new DataStreamOptions((DataStreamFailureStore) args[0], (Boolean) args[1])
    );

    static {
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamFailureStore.fromXContent(p),
            FAILURE_STORE_FIELD
        );
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), RELAX_RESTRICTIONS);
    }

    public static DataStreamOptions read(StreamInput in) throws IOException {
        return new DataStreamOptions(
            in.readOptionalWriteable(DataStreamFailureStore::new),
            in.getTransportVersion().supports(RELAX_RESTRICTIONS_VERSION) ? in.readOptionalBoolean() : null
        );
    }

    public static Diff<DataStreamOptions> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamOptions::read, in);
    }

    public Boolean relaxRestrictions() {
        return Boolean.TRUE.equals(relaxRestrictions);
    }

    /**
     * @return true if none of the options are defined
     */
    public boolean isEmpty() {
        return failureStore == null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(INTRODUCE_FAILURES_LIFECYCLE) || failureStore == null || failureStore().enabled() != null) {
            out.writeOptionalWriteable(failureStore);
        } else {
            // When communicating with older versions we need to ensure we do not sent an invalid failure store config.
            // If the enabled flag is not defined, we treat it as null.
            out.writeOptionalWriteable(null);
        }
        if (out.getTransportVersion().supports(RELAX_RESTRICTIONS_VERSION)) {
            out.writeOptionalBoolean(this.relaxRestrictions);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (failureStore != null) {
            builder.field(FAILURE_STORE_FIELD.getPreferredName(), failureStore);
        }
        if (relaxRestrictions != null) {
            builder.field(RELAX_RESTRICTIONS.getPreferredName(), relaxRestrictions);
        }
        builder.endObject();
        return builder;
    }

    public static DataStreamOptions fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * This class is only used in template configuration. It wraps the fields of {@link DataStreamOptions} with {@link ResettableValue}
     * to allow a user to signal when they want to reset any previously encountered values during template composition.
     */
    public record Template(ResettableValue<DataStreamFailureStore.Template> failureStore, @Nullable Boolean relaxRestrictions)
        implements
            Writeable,
            ToXContentObject {
        public static final Template EMPTY = new Template(ResettableValue.undefined(), null);

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
            "data_stream_options_template",
            false,
            (args, unused) -> new Template(
                args[0] == null ? ResettableValue.undefined() : (ResettableValue<DataStreamFailureStore.Template>) args[0],
                (Boolean) args[1]
            )
        );

        static {
            PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, s) -> ResettableValue.create(DataStreamFailureStore.Template.fromXContent(p)),
                ResettableValue.reset(),
                FAILURE_STORE_FIELD
            );
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), RELAX_RESTRICTIONS);
        }

        public Template(DataStreamFailureStore.Template template) {
            this(ResettableValue.create(template), null);
        }

        public Template {
            assert failureStore != null : "Template does not accept null values, please use Resettable.undefined()";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(INTRODUCE_FAILURES_LIFECYCLE)
                || failureStore.get() == null
                || failureStore().mapAndGet(DataStreamFailureStore.Template::enabled).get() != null) {
                ResettableValue.write(out, failureStore, (o, v) -> v.writeTo(o));
                // When communicating with older versions we need to ensure we do not sent an invalid failure store config.
            } else {
                // If the enabled flag is not defined, we treat failure store as not defined, if reset we treat the failure store as reset
                ResettableValue<DataStreamFailureStore.Template> bwcFailureStore = failureStore.get().enabled().shouldReset()
                    ? ResettableValue.reset()
                    : ResettableValue.undefined();
                ResettableValue.write(out, bwcFailureStore, (o, v) -> v.writeTo(o));
            }
            if (out.getTransportVersion().supports(RELAX_RESTRICTIONS_VERSION)) {
                out.writeOptionalBoolean(this.relaxRestrictions);
            }
        }

        public static Template read(StreamInput in) throws IOException {
            ResettableValue<DataStreamFailureStore.Template> failureStore = ResettableValue.read(in, DataStreamFailureStore.Template::read);
            Boolean relaxRestrictions = in.getTransportVersion().supports(RELAX_RESTRICTIONS_VERSION) ? in.readOptionalBoolean() : null;
            return new Template(failureStore, relaxRestrictions);
        }

        public static Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        /**
         * Converts the template to XContent, depending on the {@param params} set by {@link ResettableValue#hideResetValues(Params)}
         * it may or may not display any explicit nulls when the value is to be reset.
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            failureStore.toXContent(builder, params, FAILURE_STORE_FIELD.getPreferredName());
            if (relaxRestrictions != null) {
                builder.field(RELAX_RESTRICTIONS.getPreferredName(), relaxRestrictions);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }

    public static Builder builder(Template template) {
        return new Builder(template);
    }

    /**
     * Builds and composes the data stream options or the respective template.
     */
    public static class Builder {
        private DataStreamFailureStore.Builder failureStore = null;
        private Boolean relaxRestrictions = null;

        public Builder(Template template) {
            if (template != null) {
                if (template.failureStore().get() != null) {
                    failureStore = DataStreamFailureStore.builder(template.failureStore().get());
                }
                relaxRestrictions = template.relaxRestrictions;
            }
        }

        public Builder(DataStreamOptions options) {
            if (options != null) {
                if (options.failureStore() != null) {
                    failureStore = DataStreamFailureStore.builder(options.failureStore());
                }
                relaxRestrictions = options.relaxRestrictions();
            }
        }

        /**
         * Composes this builder with the values of the provided template. This is not a replacement necessarily, the
         * inner values will be merged.
         */
        public Builder composeTemplate(DataStreamOptions.Template options) {
            return failureStore(options.failureStore(), options.relaxRestrictions());
        }

        /**
         * Composes the current failure store configuration with the provided value. This is not a replacement necessarily, if both
         * instance contain data the configurations are merged.
         */
        public Builder failureStore(
            ResettableValue<DataStreamFailureStore.Template> newFailureStore,
            @Nullable Boolean newRelaxRestrictions
        ) {
            if (newFailureStore.shouldReset()) {
                failureStore = null;
            } else if (newFailureStore.isDefined()) {
                if (failureStore == null) {
                    failureStore = DataStreamFailureStore.builder(newFailureStore.get());
                } else {
                    failureStore.composeTemplate(newFailureStore.get());
                }
            }
            this.relaxRestrictions = newRelaxRestrictions;
            return this;
        }

        public Builder relaxRestrictions(Boolean relaxRestrictions) {
            this.relaxRestrictions = relaxRestrictions;
            return this;
        }

        public Template buildTemplate() {
            return new Template(ResettableValue.create(failureStore == null ? null : failureStore.buildTemplate()), relaxRestrictions);
        }

        public DataStreamOptions build() {
            return new DataStreamOptions(failureStore == null ? null : failureStore.build(), relaxRestrictions);
        }
    }
}
