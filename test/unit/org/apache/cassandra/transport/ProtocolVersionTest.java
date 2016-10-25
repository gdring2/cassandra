/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.transport;

import org.junit.Assert;
import org.junit.Test;

public class ProtocolVersionTest
{
    @Test
    public void testDecode()
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            Assert.assertEquals(version, ProtocolVersion.decode(version.asInt()));

        for (ProtocolVersion version : ProtocolVersion.UNSUPPORTED)
        { // unsupported old versions
            try
            {
                Assert.assertEquals(version, ProtocolVersion.decode(version.asInt()));
                Assert.fail("Expected invalid protocol exception");
            }
            catch (ProtocolException ex)
            {
                Assert.assertNotNull(ex.getForcedProtocolVersion());
                Assert.assertEquals(version, ex.getForcedProtocolVersion());
            }
        }

        try
        { // unsupported newer version
            Assert.assertEquals(null, ProtocolVersion.decode(63));
            Assert.fail("Expected invalid protocol exception");
        }
        catch (ProtocolException ex)
        {
            Assert.assertNotNull(ex.getForcedProtocolVersion());
            Assert.assertEquals(ProtocolVersion.MAX_SUPPORTED_VERSION, ex.getForcedProtocolVersion());
        }
    }

    @Test
    public void testSupportedVersions()
    {
        Assert.assertTrue(ProtocolVersion.supportedVersions().size() >= 2); // at least one OS and one DSE
        Assert.assertNotNull(ProtocolVersion.CURRENT);

        Assert.assertFalse(ProtocolVersion.V4.isBeta());
        Assert.assertTrue(ProtocolVersion.V5.isBeta());
    }

    @Test
    public void testComparisons()
    {
        Assert.assertEquals(0, ProtocolVersion.V1.compareTo(ProtocolVersion.V1));
        Assert.assertEquals(0, ProtocolVersion.V2.compareTo(ProtocolVersion.V2));
        Assert.assertEquals(0, ProtocolVersion.V3.compareTo(ProtocolVersion.V3));
        Assert.assertEquals(0, ProtocolVersion.V4.compareTo(ProtocolVersion.V4));

        Assert.assertEquals(-1, ProtocolVersion.V1.compareTo(ProtocolVersion.V2));
        Assert.assertEquals(-1, ProtocolVersion.V2.compareTo(ProtocolVersion.V3));
        Assert.assertEquals(-1, ProtocolVersion.V3.compareTo(ProtocolVersion.V4));

        Assert.assertEquals(1, ProtocolVersion.V4.compareTo(ProtocolVersion.V3));
        Assert.assertEquals(1, ProtocolVersion.V3.compareTo(ProtocolVersion.V2));
        Assert.assertEquals(1, ProtocolVersion.V2.compareTo(ProtocolVersion.V1));
    }
}
