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
package cwelton.kstreams;

import cwelton.kstreams.streamjoin.StreamJoinDriver;
import cwelton.kstreams.streamjoin.StreamJoinDriver2;
import cwelton.kstreams.streamjoin.StreamUnionDriver;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

/**
 * Created by cwelton on 8/24/16.
 */
public class Main {
    private final static String[] default_args = {"join2"};

    public static void usage() {
        System.err.println("usage: <command> [union|join|join2]");
        System.err.println("   union - read from topics 'item-1' and 'item-2' write to item-union");
        System.err.println("   join  - read from topics 'item-1' and 'item-2' write to item-union");
        System.err.println("   join2 - read from topics 'item-1' and 'thing' write to item-union (default)");
        System.exit(1);
    }

    public static void main(String[] args) {
        StreamsConfig config = new StreamsConfig(getProperties());

        if (args.length > 1)
            usage();
        if (args.length == 0)
            args = default_args;

        switch (args[0]) {
            case "union": {
                StreamUnionDriver unionDriver = new StreamUnionDriver(config);
                unionDriver.run();
                break;
            }

            case "join": {
                StreamJoinDriver joinDriver = new StreamJoinDriver(config);
                joinDriver.run();
                break;
            }

            case "join2": {
                StreamJoinDriver2 joinDriver = new StreamJoinDriver2(config);
                joinDriver.run();
                break;
            }

            default:
                usage();
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Cwelton-Processor-Job");
        props.put("group.id", "test-consumer-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-processor-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zk1:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
