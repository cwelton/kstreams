/*
 * Copyright 2016 Caleb Welton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cwelton.kstreams.processor;

import cwelton.kstreams.model.Item;
import cwelton.kstreams.model.Thing;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * JoinProcessor2 - Like JoinProcessor, but alternate implementation for differing message formats.
 *
 * 1. Returns a tuple of [item-1, thing] based on items received on the two input topics.
 * 2. Each item received is emitted exactly once.
 * 3. If two, or more, messages are received on the same topic before a message is received
 *    on the other topic then one half of the tuple will be null.
 *
 * Created by cwelton on 8/24/16.
 */
public class JoinProcessor2<V> extends AbstractProcessor<String, V> {

    private Item left;
    private Thing right;

    /**
     * process() - handle a single input item from one of the input streams
     * @param key
     * @param value
     *
     * Note: AbstractProcessor itself is a templated class which describes the class
     * of input tuples.  Ideally this could be adjusted so that we could simply implement
     * the two specific process implementation instead and let the JVM perform the
     * disambiguation, however that doesn't work with the existing templating mechanisms.
     *
     */
    @Override
    public void process(String key, V value) {
       if (value instanceof Item) {
           process(key, (Item) value);
       } else if (value instanceof Thing) {
           process(key, (Thing) value);
       } else {
            throw new IllegalArgumentException("Unexpected value type '"+value.getClass().getName()+"'");
       }
    }

    public void process(String key, Item value) {
        if (left != null) {
            emit();
        }
        left = value;
        if (left != null && right != null) {
            emit();
        }
    }

    public void process(String key, Thing value) {
        if (right != null) {
            emit();
        }
        right = value;
        if (left != null && right != null) {
            emit();
        }
    }

    protected void emit() {
        List<Object> pair = new ArrayList<>();
        pair.add(left);
        pair.add(right);
        context().forward(null, pair);
        context().commit();
        left = null;
        right = null;
    }
}
