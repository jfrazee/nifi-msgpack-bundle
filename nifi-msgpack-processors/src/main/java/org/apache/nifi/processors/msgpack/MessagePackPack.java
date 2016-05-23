/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.msgpack;

import java.io.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.jackson.dataformat.JsonArrayFormat;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.StreamCallback;

@Tags({"msgpack", "messagepack", "json", "serialization"})
@CapabilityDescription("Serialize JSON in MessagePack format")
@SeeAlso({})
@ReadsAttribute(
    attribute = "mime.type",
    description = "If the input format is set to use mime.type attribute, " +
        "this attribute is used to determine the input type. Otherwise, this " +
        "attribute is ignored."
)
@WritesAttribute(
    attribute="mime.type",
    description="If the FlowFile is successfully converted, the MIME type " +
        "will be updated to application/msgpack"
)
public class MessagePackPack extends AbstractProcessor {
    private static final String USE_MIME_TYPE = "use mime.type attribute";
    private static final String JSON = "JSON";
    private static final String XML = "XML";

    private static final String MIME_TYPE = "application/msgpack";
    private static final String MIME_EXT = ".msgpack";
    private static final String MIME_EXT_KEY = "mime.extension";

    public static final PropertyDescriptor INPUT_FORMAT = new PropertyDescriptor
            .Builder()
            .name("input-format")
            .displayName("Input Format")
            .description("")
            .required(true)
            .defaultValue(USE_MIME_TYPE)
            .allowableValues(USE_MIME_TYPE, JSON, XML)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description(
                "Any FlowFile that is successfully converted is routed to " +
                "this relationship"
            )
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                "Any FlowFile that fails to be converted is routed to " +
                "this relationship"
            )
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(INPUT_FORMAT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ObjectMapper reader = new ObjectMapper();
        final ObjectMapper writer = new ObjectMapper(new MessagePackFactory());
        writer.setAnnotationIntrospector(new JsonArrayFormat());

        final AtomicBoolean failed = new AtomicBoolean(false);
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream is, OutputStream os) throws IOException {
                try (final OutputStream msgpack = new BufferedOutputStream(os)) {
                    final JsonNode json = reader.readTree(is);
                    final byte[] bytes = writer.writeValueAsBytes(json);
                    msgpack.write(bytes);
                    msgpack.flush();
                }
                catch (JsonProcessingException e) {
                    getLogger().error(e.getMessage(), e);
                    failed.set(true);
                }
            }
        });

        if (failed.get()) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), MIME_TYPE);
        flowFile = session.putAttribute(flowFile, MIME_EXT_KEY, MIME_EXT);

        session.transfer(flowFile, REL_SUCCESS);
    }
}
