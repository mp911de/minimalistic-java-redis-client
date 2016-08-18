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

import java.util.concurrent.CompletableFuture;

import io.netty.handler.codec.redis.RedisMessage;
import redisclient.output.CommandOutputSupport;

/**
 * @author Mark Paluch
 */
public class AsyncCommand<T> implements RespMessage {

    private CompletableFuture<T> future = new CompletableFuture<T>();
    private RespMessage command;

    public AsyncCommand(RespMessage command) {
        this.command = command;
    }

    @Override
    public void cancel() {

        command.cancel();
        future.cancel(true);
    }

    @Override
    public void complete() {

        command.complete();
        if (output() != null) {
            if (output().hasError()) {
                future.completeExceptionally(new IllegalStateException(output().getError()));
            } else {
                future.complete(output().get());
            }
        } else {
            future.complete(null);
        }
    }

    @Override
    public void completeExceptionally(Throwable t) {

        command.completeExceptionally(t);
        future.completeExceptionally(t);
    }

    @Override
    public CommandOutputSupport<?, ?, T> output() {
        return (CommandOutputSupport<?, ?, T>) command.output();
    }

    public CompletableFuture<T> future() {
        return future;
    }

    @Override
    public RedisMessage message() {
        return command.message();
    }
}
