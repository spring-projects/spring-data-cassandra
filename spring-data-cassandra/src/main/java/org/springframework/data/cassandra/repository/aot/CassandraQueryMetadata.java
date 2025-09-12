/*
 * Copyright 2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.aot;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.repository.aot.generate.QueryMetadata;

/**
 * Metadata for an AOT Cassandra query.
 *
 * @author Mark Paluch
 * @since 5.0
 */
record CassandraQueryMetadata(AotQuery result) implements QueryMetadata {

	@Override
	public Map<String, Object> serialize() {

		Map<String, Object> serialized = new LinkedHashMap<>();

		if (result() instanceof StringAotQuery sq) {
			serialized.put("query", sq.getQueryString());
		} else if (result() instanceof DerivedAotQuery dq) {
			serialized.put("query", dq.getQueryString());
		}

		if (result() instanceof StringAotQuery.NamedStringAotQuery nsq) {
			serialized.put("name", nsq.getQueryName());
		}

		return serialized;
	}
}
