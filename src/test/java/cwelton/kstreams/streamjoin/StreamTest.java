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
package cwelton.kstreams.streamjoin;

import cwelton.kstreams.model.Item;
import junit.framework.TestCase;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by cwelton on 8/24/16.
 */
public class StreamTest extends TestCase {

    private class MockContext extends ProcessorContextImpl {
        public Object key;
        public Object value;

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
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "junit-cwelton-kstreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        return props;
    }

    @Test
    public void test() throws Exception {
        StreamsConfig config = new StreamsConfig(getProperties());
        MockContext context = new MockContext(config);
        PassThroughProcessor processor = new PassThroughProcessor();
        processor.init(context);
        Item item1 = new Item("item-1");
        Item item2 = new Item("item-2");

        // Pass through processor forwards items to children without modification
        // Looking at the context.value should show the last item processed.
        processor.process(null, item1);
        assertEquals(context.value, item1);

        processor.process(null, item2);
        assertEquals(context.value, item2);
    }
}
