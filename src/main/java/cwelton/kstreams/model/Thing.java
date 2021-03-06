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

package cwelton.kstreams.model;

/**
 * Created by cwelton on 8/24/16.
 */
public class Thing {
    private Integer value;

    public Thing(Integer val) {
        value = val;
    }

    private Thing(Builder builder) {
        this.value = builder.value;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer val) {
        value = val;
    }

    public static final class Builder {
        private Integer value;

        private Builder() {
        }

        public Builder value(Integer val) {
            value = val;
            return this;
        }

        public Thing build() {
            return new Thing(this);
        }
    }
}
