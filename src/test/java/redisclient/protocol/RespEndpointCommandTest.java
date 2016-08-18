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

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import redisclient.codec.StringCodec;
import redisclient.output.StatusOutput;
import redisclient.output.ValueOutput;

/**
 * @author Mark Paluch
 */
public class RespEndpointCommandTest {

    public static final StringCodec STRING_CODEC = new StringCodec(StandardCharsets.UTF_8);
    private static NioEventLoopGroup eventLoopGroup;
    private static EventExecutorGroup eventExecutors;
    private static RespEndpoint endpoint;

    @BeforeClass
    public static void beforeAll() throws Exception {

        eventLoopGroup = new NioEventLoopGroup();
        eventExecutors = new DefaultEventExecutor();

        Bootstrap bootstrap = new Bootstrap();

        endpoint = new RespEndpoint(bootstrap.group(eventLoopGroup).channelFactory(NioSocketChannel::new), eventExecutors);

        endpoint.connect(new InetSocketAddress("127.0.0.1", 6379)).get();

    }

    @AfterClass
    public static void afterAll() throws Exception {
        endpoint.disconnect().get();

        eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
        eventExecutors.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    }

    @Test
    public void shouldSendMessage() throws Exception {

        List<RedisMessage> chunks = Arrays.asList(
                new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(STRING_CODEC.encodeKey("SET"))),
                new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(STRING_CODEC.encodeKey("key"))),
                new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(STRING_CODEC.encodeKey("value"))));

        SimpleCommand<String> command = new SimpleCommand<>(new StatusOutput<>(STRING_CODEC), new ArrayRedisMessage(chunks));

        AsyncCommand<String> async = new AsyncCommand<>(command);

        endpoint.send(async);

        assertEquals("OK", async.future().get());
    }

    @Test
    public void shouldReadMessage() throws Exception {

        shouldSendMessage();

        List<RedisMessage> chunks = Arrays.asList(
                new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(STRING_CODEC.encodeKey("GET"))),
                new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(STRING_CODEC.encodeKey("key"))));

        SimpleCommand<String> command = new SimpleCommand<>(new ValueOutput<>(STRING_CODEC), new ArrayRedisMessage(chunks));

        AsyncCommand<String> async = new AsyncCommand<>(command);

        endpoint.send(async);
        assertEquals("value", async.future().get());
    }
}