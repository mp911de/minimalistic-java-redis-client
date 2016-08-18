/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redisclient.protocol;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * @author Mark Paluch
 */
public class RespEndpointTest {

    private static NioEventLoopGroup eventLoopGroup;
    private static EventExecutorGroup eventExecutors;

    @BeforeClass
    public static void beforeAll() throws Exception {

        eventLoopGroup = new NioEventLoopGroup();
        eventExecutors = new DefaultEventExecutor();
    }

    @AfterClass
    public static void afterAll() throws Exception {
        eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
        eventExecutors.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    }

    @Test
    public void shouldConnect() throws Exception {

        Bootstrap bootstrap = new Bootstrap();

        RespEndpoint endpoint = new RespEndpoint(bootstrap.group(eventLoopGroup).channelFactory(NioSocketChannel::new),
                eventExecutors);

        endpoint.connect(new InetSocketAddress("127.0.0.1", 6379)).get();
        endpoint.disconnect().get();
    }
}