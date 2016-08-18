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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.AbstractStringRedisMessage;
import io.netty.handler.codec.redis.ArrayHeaderRedisMessage;
import io.netty.handler.codec.redis.BulkStringHeaderRedisMessage;
import io.netty.handler.codec.redis.BulkStringRedisContent;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import redisclient.output.CommandOutput;

/**
 * State machine that handles Redis server responses using {@link RedisMessage} according to the
 * <a href="http://redis.io/topics/protocol">Unified Request Protocol (RESP)</a>.
 * 
 * @author Mark Paluch
 */
class RedisMessageStateMachine {

    private static final String QUEUED = "QUEUED";
    private Deque<State> stack = new ArrayDeque<State>();
    private final ByteBuf buffer = Unpooled.directBuffer(8192);

    static class State {
        enum Type {
            ROOT, MULTI;
        }

        Type type;
        long expected = -1;
        long actual = 0;

        public State(Type type) {
            this.type = type;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [type=").append(type);
            sb.append(", expected=").append(expected);
            sb.append(", actual=").append(actual);
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * Decodes a {@link RedisMessage}.
     * 
     * @param segment the segment
     * @param output the command output
     * @return {@literal true} if a command was fully decoded.
     */
    public boolean decode(RedisMessage segment, CommandOutput output) {

        if (stack.isEmpty()) {
            stack.add(new State(State.Type.ROOT));
        }

        if (output == null) {
            return stack.isEmpty();
        }

        State state = stack.peek();

        if (segment instanceof ArrayHeaderRedisMessage) {
            ArrayHeaderRedisMessage arrayHeader = (ArrayHeaderRedisMessage) segment;

            if (arrayHeader.isNull()) {
                state.actual++;
                output.set((ByteBuffer) null);
                complete(output, stack.size());
            } else {

                if (state.type == State.Type.ROOT) {
                    state.type = State.Type.MULTI;
                } else {
                    state.actual++;
                    state = new State(State.Type.MULTI);
                    stack.push(state);
                }

                state.expected = arrayHeader.length();
                output.multi((int) state.expected);
            }
        } else {

            if (segment instanceof ErrorRedisMessage) {

                AbstractStringRedisMessage errorSegment = (AbstractStringRedisMessage) segment;
                state.actual++;
                output.setError(errorSegment.content());
                complete(output, stack.size());

            } else if (segment instanceof SimpleStringRedisMessage) {

                AbstractStringRedisMessage stringSegment = (AbstractStringRedisMessage) segment;
                state.actual++;

                if (!QUEUED.equals(stringSegment.content())) {
                    output.set(stringSegment.content());
                }
                complete(output, stack.size());
            } else if (segment instanceof BulkStringHeaderRedisMessage) {

                state.actual++;
                state = new State(State.Type.MULTI);
                stack.push(state);

                BulkStringHeaderRedisMessage bulkHeader = (BulkStringHeaderRedisMessage) segment;
                state.type = State.Type.MULTI;
                state.expected = bulkHeader.bulkStringLength();

                buffer.clear();
                if (!bulkHeader.isNull() && buffer.writableBytes() < bulkHeader.bulkStringLength()) {
                    buffer.capacity(buffer.capacity() + (buffer.writableBytes() - bulkHeader.bulkStringLength()));
                }
            } else if (segment instanceof BulkStringRedisContent) {

                BulkStringRedisContent bulkStringSegment = (BulkStringRedisContent) segment;

                if (buffer.writableBytes() < bulkStringSegment.content().readableBytes()) {
                    buffer.capacity(buffer.capacity() + (buffer.writableBytes() - bulkStringSegment.content().readableBytes()));
                }
                state.actual += bulkStringSegment.content().readableBytes();

                buffer.writeBytes(bulkStringSegment.content());

                if (buffer.readableBytes() == state.expected) {
                    output.set(buffer.nioBuffer());
                    complete(output, stack.size());

                    buffer.clear();
                }
            } else if (segment instanceof IntegerRedisMessage) {
                IntegerRedisMessage integerSegment = (IntegerRedisMessage) segment;

                state.actual++;

                output.set(integerSegment.value());
                complete(output, stack.size());
            }
        }

        while (state.type == State.Type.ROOT || (state.type == State.Type.MULTI && state.actual >= state.expected)) {
            stack.pop();
            complete(output, stack.size());

            if (stack.isEmpty()) {
                break;
            }

            state = stack.peek();
        }

        return stack.isEmpty();
    }

    private void complete(CommandOutput output, int size) {
        output.complete(size);
    }

    public void close() {
        buffer.clear();
        buffer.release();
    }
}