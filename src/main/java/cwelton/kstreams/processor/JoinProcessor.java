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
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * JoinProcessor - Returns a list of last seen value on each each of two input topics.
 *
 * 1. Returns a tuple of [item-1, item-2] based on items received on the two input topics.
 * 2. Each item received is emitted exactly once.
 * 3. If two, or more, messages are received on the same topic before a message is received
 *    on the other topic then one half of the tuple will be null.
 *
 * Created by cwelton on 8/24/16.
 */
public class JoinProcessor extends AbstractProcessor<String, Item> {

    private Item left;
    private Item right;

    @Override
    public void process(String key, Item value) {
        if (context().topic().equals("item-1")) {
            if (left != null) {
                emit();
            }
            left = value;
        } else if (context().topic().equals("item-2")) {
            if (right != null) {
                emit();
            }
            right = value;
        } else {
            throw new IllegalStateException("unknown input topic '"+context().topic()+"'");
        }
        if (left != null && right != null) {
            emit();
        }
    }

    protected void emit() {
        List<Item> pair = new ArrayList<>();
        pair.add(left);
        pair.add(right);
        context().forward(null, pair);
        context().commit();
        left = null;
        right = null;
    }
}
