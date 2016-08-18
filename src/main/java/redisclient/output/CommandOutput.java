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

package redisclient.output;

import java.nio.ByteBuffer;

/**
 * @author Mark Paluch
 */
public interface CommandOutput {

    /**
     * Set the command output to a sequence of bytes, or null. Concrete {@link CommandOutputSupport} implementations must
     * override this method unless they only receive an integer value which cannot be null.
     *
     * @param bytes The command output, or null.
     */
    void set(ByteBuffer bytes);

    /**
     * Set the command output to simple string, or null. Concrete {@link CommandOutputSupport} implementations must override
     * this method unless they only receive an integer value which cannot be null.
     *
     * @param string The command output, or null.
     */
    void set(String string);

    /**
     * Set the command output to a 64-bit signed integer. Concrete {@link CommandOutputSupport} implementations must override
     * this method unless they only receive a byte array value.
     *
     * @param integer The command output.
     */
    void set(long integer);

    /**
     * Set command output to an error message from the client.
     *
     * @param error Error message.
     */
    void setError(String error);

    /**
     * Mark the command output complete.
     *
     * @param depth Remaining depth of output queue.
     *
     */
    void complete(int depth);

    void multi(int depth);
}
