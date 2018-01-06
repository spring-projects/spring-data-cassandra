/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Unit tests for {@link BasicMapId}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class BasicMapIdUnitTests {

	@Test
	public void testMapConstructor() {

		Map<String, Object> map = new HashMap<>();
		map.put("field1", "value1");
		map.put("field2", 2);

		BasicMapId basicMapId = new BasicMapId(map);

		assertThat(map.get("field1")).isEqualTo(basicMapId.get("field1"));
		assertThat(map.get("field2")).isEqualTo(basicMapId.get("field2"));
	}
}
