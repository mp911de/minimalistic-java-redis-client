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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author Mark Paluch
 */
class RedisStateHandler extends ChannelDuplexHandler {

    private final Queue<RespMessage> queue = new ConcurrentLinkedQueue<RespMessage>();
    private final RedisMessageStateMachine stateMachine = new RedisMessageStateMachine();

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        RespMessage respMessage;

        while ((respMessage = queue.poll()) != null) {
            respMessage.cancel();
        }

        super.channelInactive(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        stateMachine.close();
        super.channelUnregistered(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof RedisMessage) {

            while (!queue.isEmpty()) {

                RespMessage message = queue.peek();
                try {

                    if (stateMachine.decode((RedisMessage) msg, message.output())) {
                        queue.poll();
                        message.complete();
                    } else {
                        return;
                    }

                } catch (Exception e) {
                    message.completeExceptionally(e);
                }
            }
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if (msg instanceof RespMessage) {

            RespMessage respMessage = (RespMessage) msg;
            queue.add(respMessage);
            ctx.write(respMessage.message(), promise);
        }
    }
}
