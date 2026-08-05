/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.cassandra.cache;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Internal envelope that pairs the value type with its serialized {@link JsonNode} so that arbitrary Spring Cache
 * values can be stored and restored through the shared Cassandra store.
 *
 * @author Anıl Şenocak
 * @since 5.2
 */
public final class SpringCacheEntry {
	private final String type;
	private final JsonNode value;

	@JsonCreator
	public SpringCacheEntry(final @JsonProperty("type") String type, final @JsonProperty("value") JsonNode value) {
		this.type = Objects.requireNonNull(type, "Type must not be null");
		this.value = Objects.requireNonNull(value, "Value must not be null");
	}

	public String getType() {
		return type;
	}

	public JsonNode getValue() {
		return value;
	}

	@Override
	public boolean equals(final Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof SpringCacheEntry that)) {
			return false;
		}
		return this.type.equals(that.type) && this.value.equals(that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, value);
	}
}
