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
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;

import org.junit.Before;
import org.junit.Test;

public class TestMessagePackPack {

    private static final String CONTENT = "{\"message\":\"Hello, World!\"}";
    private static final int[] MSGPACK_INTS = {129, 167, 109, 101, 115, 115, 97, 103, 101, 173, 72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
    private static byte[] MSGPACK_BYTES;

    private TestRunner runner;

    @Before
    public void init() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes);
        for (int i : MSGPACK_INTS) {
            data.write(i);
        }
        MSGPACK_BYTES = bytes.toByteArray();

        runner = TestRunners.newTestRunner(MessagePackPack.class);
    }

    @Test
    public void testPackJson() throws IOException {
        final Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("mime.type", "application/json");

        runner.enqueue(CONTENT.getBytes(StandardCharsets.UTF_8), attributes);
        runner.run();

        runner.assertTransferCount(MessagePackPack.REL_SUCCESS, 1);
        runner.assertTransferCount(MessagePackPack.REL_FAILURE, 0);

        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(MessagePackPack.REL_SUCCESS)) {
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/msgpack");
            flowFile.assertAttributeExists("mime.extension");
            flowFile.assertAttributeEquals("mime.extension", ".msgpack");
            flowFile.assertContentEquals(MSGPACK_BYTES);
        }
    }

}
