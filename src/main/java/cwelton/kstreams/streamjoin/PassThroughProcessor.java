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
import org.apache.kafka.streams.processor.AbstractProcessor;

/**
 * Created by cwelton on 8/24/16.
 */
public class PassThroughProcessor extends AbstractProcessor<String, Item> {

    @Override
    public void process(String key, Item value) {
        context().forward(key, value);
        context().commit();
    }
}
