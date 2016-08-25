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
package cwelton.kstreams.streamsplit;

import cwelton.kstreams.model.Item;
import cwelton.kstreams.processor.PassThroughProcessor;
import cwelton.kstreams.processor.SplitProcessor;
import cwelton.kstreams.serializer.JsonDeserializer;
import cwelton.kstreams.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * StreamSplitDriver - Provides example of splitting an input stream to two input streams.
 *
 * By adding both output streams with our processor as a source:
 *     <code>.addSink("SINK-1", "item-2", stringSerializer, itemJsonSerializer, "PROCESS")</code>
 *     <code>.addSink("SINK-2", "item-union", stringSerializer, itemJsonSerializer, "PROCESS")</code>
 *
 * Further the SplitProcessor will choose which output to send a message to.
 *
 * Created by cwelton on 8/24/16.
 */
public class StreamSplitDriver {

    private StreamsConfig config;

    public StreamSplitDriver(StreamsConfig config) {
        this.config = config;
    }

    public void run() {

        JsonDeserializer<Item> itemJsonDeserializer = new JsonDeserializer<>(Item.class);
        JsonSerializer<Item> itemJsonSerializer = new JsonSerializer<>();

        StringDeserializer stringDeserializer = new StringDeserializer();
        StringSerializer stringSerializer = new StringSerializer();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder
                .addSource("SOURCE", stringDeserializer, itemJsonDeserializer, "item-1")
                .addProcessor("PROCESS", SplitProcessor::new, "SOURCE")
                .addSink("SINK-1", "item-2", stringSerializer, itemJsonSerializer, "PROCESS")
                .addSink("SINK-2", "item-union", stringSerializer, itemJsonSerializer, "PROCESS");

        System.out.println("Starting Split Example");
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, config);
        streaming.start();
        System.out.println("Now started Split Example");
    }
}
