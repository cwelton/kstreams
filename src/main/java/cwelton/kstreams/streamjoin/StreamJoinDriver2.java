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
import cwelton.kstreams.model.Thing;
import cwelton.kstreams.serializer.JsonDeserializer;
import cwelton.kstreams.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * StreamJoinDriver2 - Provides example of an output stream that merges two input streams.
 *
 * Identical to StreamJoinDriver, except shows a different way of handling consumption of
 * multiple input topics when the topics have different message formats.
 *
 * Created by cwelton on 8/24/16.
 */
public class StreamJoinDriver2 {

    private StreamsConfig config;

    public StreamJoinDriver2(StreamsConfig config) {
        this.config = config;
    }

    public void run() {
        StringDeserializer stringDeserializer = new StringDeserializer();
        StringSerializer stringSerializer = new StringSerializer();
        JsonSerializer jsonSerializer = new JsonSerializer<>();

        // Different Serializers for different input streams
        JsonDeserializer<Item> itemJsonDeserializer = new JsonDeserializer<>(Item.class);
        JsonDeserializer<Thing> thingJsonDeserializer = new JsonDeserializer<>(Thing.class);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder
                .addSource("SOURCE-1", stringDeserializer, itemJsonDeserializer, "item-1")
                .addSource("SOURCE-2", stringDeserializer, thingJsonDeserializer, "thing")
                .addProcessor("PROCESS", JoinProcessor2::new, "SOURCE-1", "SOURCE-2")
                .addSink("SINK", "item-union", stringSerializer, jsonSerializer, "PROCESS");

        System.out.println("Starting Join2 Example");
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, config);
        streaming.start();
        System.out.println("Now started Join2 Example");
    }
}
