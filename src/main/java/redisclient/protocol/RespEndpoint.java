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

import java.net.SocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @author Mark Paluch
 */
public class RespEndpoint {

    private volatile Channel channel;
    private final Bootstrap bootstrap;
    private final EventExecutorGroup eventExecutor;

    public RespEndpoint(Bootstrap bootstrap, EventExecutorGroup eventExecutor) {
        this.bootstrap = bootstrap;
        this.eventExecutor = eventExecutor;
    }

    public Future<Boolean> connect(SocketAddress socketAddress) {

        final DefaultPromise<Boolean> future = new DefaultPromise(eventExecutor.next());
        final DefaultPromise<Channel> channelActive = new DefaultPromise(eventExecutor.next());

        ChannelFuture channelFuture = bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) throws Exception {

				channel.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
				channel.pipeline().addLast(new RedisEncoder());
                channel.pipeline().addLast(new RedisDecoder());
                channel.pipeline().addLast(new RedisStateHandler());
                channel.pipeline().addLast(new ChannelDuplexHandler() {

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        RespEndpoint.this.channel = ctx.channel();
                        channelActive.setSuccess(ctx.channel());
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                    	if(!channelActive.isDone()) {
							channelActive.setFailure(cause);
						}
						super.exceptionCaught(ctx, cause);
					}
                });
            }
        }).connect(socketAddress);

        channelActive.addListener(f -> {

        	if(future.isDone()){
				return;
			}

			if (f.isSuccess() && channel != null) {
				future.setSuccess(true);

			} else if (f.isSuccess() && channel == null) {
				future.setFailure(new IllegalStateException("Channel disconnected"));
			} else {
				future.setFailure(f.cause());
			}
		});

		channelFuture.addListener((ChannelFutureListener) f -> {

			if (!f.isSuccess() && !future.isDone()) {
				future.setFailure(f.cause());
			}
		});

        return future;
    }

    public Future<Boolean> disconnect() {

        final DefaultPromise<Boolean> future = new DefaultPromise(eventExecutor.next());
        if (channel == null) {
            return future.setSuccess(true);
        }

        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                if (f.isSuccess()) {
                    future.setSuccess(true);
                } else {
                    future.setFailure(f.cause());
                }
            }
        });

        return future;
    }

    public void send(RespMessage message) {

		if (channel != null && channel.isActive() && channel.isWritable()) {
            channel.writeAndFlush(message, channel.voidPromise());
			return;
		}

        throw new IllegalStateException("Not connected");
    }

}
