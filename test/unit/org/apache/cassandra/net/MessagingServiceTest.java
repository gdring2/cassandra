/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessagingServiceTest
{
    private static volatile MessagingService messagingService;

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureStrategy(new ParameterizedClass("org.apache.cassandra.net.MessagingServiceTest$MockBackPressureStrategy", Collections.emptyMap()));
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));
        messagingService = MessagingService.test();
    }

    @Before
    public void before() throws UnknownHostException
    {
        MockBackPressureStrategy.applied = false;
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.2"));
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.3"));
    }

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 0; i < 5000; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout", logs.get(0));
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout", logs.get(0));
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
    }
    
    @Test
    public void testUpdatesBackPressureOnSendWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).getBackPressureState();
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        MessageOut<?> ignored = null;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), noCallback, ignored);
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), bpCallback, ignored);
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), bpCallback, ignored);
        assertTrue(backPressureState.onSend);
    }

    @Test
    public void testUpdatesBackPressureOnReceiveWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).getBackPressureState();
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        boolean timeout = false;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), noCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertTrue(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);
    }
    
    @Test
    public void testUpdatesBackPressureOnTimeoutWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).getBackPressureState();
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        boolean timeout = true;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), noCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertTrue(backPressureState.onTimeout);
    }

    @Test
    public void testAppliesBackPressureWhenEnabled() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")));
        assertFalse(MockBackPressureStrategy.applied);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")));
        assertTrue(MockBackPressureStrategy.applied);
    }
    
    @Test
    public void testDoesntApplyBackPressureToBroadcastAddress() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.1")));
        assertFalse(MockBackPressureStrategy.applied);
    }

    public static class MockBackPressureStrategy implements BackPressureStrategy<MockBackPressureStrategy.MockBackPressureState>
    {
        public static volatile boolean applied = false;

        public MockBackPressureStrategy(Map<String, Object> args)
        {
        }

        @Override
        public void apply(Iterable<MockBackPressureState> states)
        {
            if (!Iterables.isEmpty(states))
                applied = true;
        }

        @Override
        public MockBackPressureState newState(InetAddress host)
        {
            return new MockBackPressureState(host);
        }

        public static class MockBackPressureState implements BackPressureState
        {
            private final InetAddress host;
            public volatile boolean onSend = false;
            public volatile boolean onReceive = false;
            public volatile boolean onTimeout = false;

            private MockBackPressureState(InetAddress host)
            {
                this.host = host;
            }

            @Override
            public void onMessageSent(MessageOut<?> message)
            {
                onSend = true;
            }

            @Override
            public void onResponseReceived()
            {
                onReceive = true;
            }

            @Override
            public void onResponseTimeout()
            {
                onTimeout = true;
            }

            @Override
            public double getBackPressureRateLimit()
            {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public InetAddress getHost()
            {
                return host;
            }
        }
    }

    private static class BackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return true;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    private static class NoBackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return false;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }
}
