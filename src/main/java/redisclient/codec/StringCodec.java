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

package redisclient.codec;

import static java.nio.charset.CoderResult.*;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * A {@link RedisCodec} that handles UTF-8 encoded keys and values.
 * 
 * @author Mark Paluch
 */
public class StringCodec implements RedisCodec<String, String> {

    private final static byte[] EMPTY = new byte[0];

    private final Charset charset;
    private final CharsetDecoder decoder;
    private  CharBuffer chars;

    /**
     * Initialize a new instance that encodes and decodes strings using the UTF-8 charset;
     */
    public StringCodec(Charset charset) {
        this.charset = charset;
        this.decoder = charset.newDecoder();
        this.chars = CharBuffer.allocate(1024);
    }

    public String decodeKey(ByteBuffer bytes) {
        return decode(bytes);
    }

    public String decodeValue(ByteBuffer bytes) {
        return decode(bytes);
    }

    public ByteBuffer encodeKey(String key) {
        return encode(key);
    }

    public ByteBuffer encodeValue(String value) {
        return encode(value);
    }

    private synchronized String decode(ByteBuffer bytes) {
        chars.clear();
        bytes.mark();

        decoder.reset();
        while (decoder.decode(bytes, chars, true) == OVERFLOW || decoder.flush(chars) == OVERFLOW) {
            chars = CharBuffer.allocate(chars.capacity() * 2);
            bytes.reset();
        }

        return chars.flip().toString();
    }

    private ByteBuffer encode(String string) {
        if (string == null) {
            return ByteBuffer.wrap(EMPTY);
        }

        return charset.encode(string);
    }
}
