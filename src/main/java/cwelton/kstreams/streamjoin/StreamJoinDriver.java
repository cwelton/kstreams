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
import cwelton.kstreams.serializer.JsonDeserializer;
import cwelton.kstreams.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * StreamJoinDriver - Provides example of an output stream that merges two input streams.
 *
 * Similar to StreamUnionDriver, but with a more complex processor that creates output messages
 * constructed of {"left": string, "right": string} where "left" is the last message received on
 * the "item-1" topic, and "right" is the last message received on the "item-2" topic.
 *
 * Created by cwelton on 8/24/16.
 */
public class StreamJoinDriver {

    private StreamsConfig config;

    public StreamJoinDriver(StreamsConfig config) {
        this.config = config;
    }

    public void run() {

        JsonDeserializer<Item> itemJsonDeserializer = new JsonDeserializer<>(Item.class);
        JsonSerializer<Item> itemJsonSerializer = new JsonSerializer<>();

        StringDeserializer stringDeserializer = new StringDeserializer();
        StringSerializer stringSerializer = new StringSerializer();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder
                .addSource("SOURCE-1", stringDeserializer, itemJsonDeserializer, "item-1")
                .addSource("SOURCE-2", stringDeserializer, itemJsonDeserializer, "item-2")
                .addProcessor("PROCESS", JoinProcessor::new, "SOURCE-1", "SOURCE-2")
                .addSink("SINK", "item-union", stringSerializer, itemJsonSerializer, "PROCESS");

        System.out.println("Starting Join Example");
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, config);
        streaming.start();
        System.out.println("Now started Join Example");
    }
}
