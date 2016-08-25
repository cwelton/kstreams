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
import junit.framework.TestCase;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

/**
 * Created by cwelton on 8/24/16.
 */
public class StreamTest extends TestCase {

    private class MockContext extends ProcessorContextImpl {
        protected Object key;
        protected Object value;
        protected String topic;

        public MockContext(StreamsConfig config) {
            super(null,null,config,null,null,null);
            key = null;
            value = null;
        }

        @Override
        public void commit() {}

        @Override
        public <K, V> void forward(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String topic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Object getValue() {
            Object result = value;
            value = null;
            return result;
        }


    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "junit-cwelton-kstreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        return props;
    }

    @Test
    public void testPassThroughProcessor() throws Exception {
        StreamsConfig config = new StreamsConfig(getProperties());
        MockContext context = new MockContext(config);
        Processor processor = new PassThroughProcessor();
        processor.init(context);
        Item item1 = new Item("item-1");
        Item item2 = new Item("item-2");

        // Pass through processor forwards items to children without modification
        // Looking at the context.value should show the last item processed.
        processor.process(null, item1);
        assertEquals(context.getValue(), item1);

        processor.process(null, item2);
        assertEquals(context.getValue(), item2);
    }

    @Test
    public void testJoinProcessor() throws Exception {
        StreamsConfig config = new StreamsConfig(getProperties());
        MockContext context = new MockContext(config);
        Processor processor = new JoinProcessor();
        processor.init(context);
        Item item1 = new Item("value-1");
        Item item2 = new Item("value-2");
        Item item3 = new Item("value-3");
        Item item4 = new Item("value-4");
        Item item5 = new Item("value-5");

        // First message doesn't cause any tuple to be emitted
        context.topic = "item-1";
        processor.process(null, item1);
        List<Item> value = (List) context.getValue();
        assertEquals(value, null);

        // Second message on same topic results in seeing a tuple of (first message, null)
        processor.process(null, item2);
        value = (List) context.getValue();
        assertEquals(value.get(0), item1);
        assertEquals(value.get(1), null);
        context.value = null;

        // A message on the other topic results in seeing a tuple of (second message, third message)
        context.topic = "item-2";
        processor.process(null, item3);
        value = (List) context.getValue();
        assertEquals(value.get(0), item2);
        assertEquals(value.get(1), item3);
        context.value = null;

        // Next message will not cause any tuple to be emitted
        processor.process(null, item4);
        value = (List) context.getValue();
        assertEquals(value, null);

        // One more message will result in seeing a tuple of (null, fourth message)
        processor.process(null, item5);
        value = (List) context.getValue();
        assertEquals(value.get(0), null);
        assertEquals(value.get(1), item4);
        context.value = null;

        // Final message on the first topic will result in seeing (final message, fifth message)
        context.topic = "item-1";
        processor.process(null, item1);
        value = (List) context.getValue();
        assertEquals(value.get(0), item1);
        assertEquals(value.get(1), item5);
    }


    @Test
    public void testJoin2Processor() throws Exception {
        StreamsConfig config = new StreamsConfig(getProperties());
        MockContext context = new MockContext(config);
        Processor processor = new JoinProcessor2();
        processor.init(context);
        Item item1 = new Item("value-1");
        Item item2 = new Item("value-2");
        Thing item3 = new Thing(3);
        Thing item4 = new Thing(4);
        Thing item5 = new Thing(5);

        // First message doesn't cause any tuple to be emitted
        context.topic = "item-1";
        processor.process(null, item1);
        List<Item> value = (List) context.getValue();
        assertEquals(value, null);

        // Second message on same topic results in seeing a tuple of (first message, null)
        processor.process(null, item2);
        value = (List) context.getValue();
        assertEquals(value.get(0), item1);
        assertEquals(value.get(1), null);
        context.value = null;

        // A message on the other topic results in seeing a tuple of (second message, third message)
        context.topic = "item-2";
        processor.process(null, item3);
        value = (List) context.getValue();
        assertEquals(value.get(0), item2);
        assertEquals(value.get(1), item3);
        context.value = null;

        // Next message will not cause any tuple to be emitted
        processor.process(null, item4);
        value = (List) context.getValue();
        assertEquals(value, null);

        // One more message will result in seeing a tuple of (null, fourth message)
        processor.process(null, item5);
        value = (List) context.getValue();
        assertEquals(value.get(0), null);
        assertEquals(value.get(1), item4);
        context.value = null;

        // Final message on the first topic will result in seeing (final message, fifth message)
        context.topic = "item-1";
        processor.process(null, item1);
        value = (List) context.getValue();
        assertEquals(value.get(0), item1);
        assertEquals(value.get(1), item5);
    }
}
