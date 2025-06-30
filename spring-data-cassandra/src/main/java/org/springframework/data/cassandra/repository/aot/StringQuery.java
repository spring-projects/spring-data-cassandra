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

import java.util.Optional;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.util.StringUtils;

/**
 * Helper to capture setting for AOT queries.
 *
 * @author Chris Bono
 * @since 4.0
 */
class StringQuery extends Query {

	private Query delegate;
	private @Nullable String raw;
	private @Nullable String sort;
	private @Nullable String fields;

	public StringQuery(Query query) {
		this.delegate = query;
	}

	public StringQuery(String query) {
		this.delegate = new Query();
		this.raw = query;
	}

	@Nullable
	String getQueryString() {

		if (StringUtils.hasText(raw)) {
			return raw;
		}

		Document queryObj = getQueryObject();
		if (queryObj.isEmpty()) {
			return null;
		}
		return toJson(queryObj);
	}

	public Query sort(String sort) {
		this.sort = sort;
		return this;
	}



//	@Nullable
//	String getSortString() {
//		if (StringUtils.hasText(sort)) {
//			return sort;
//		}
//		Document sort = getSortObject();
//		if (sort.isEmpty()) {
//			return null;
//		}
//		return toJson(sort);
//	}
//
//	@Nullable
//	String getFieldsString() {
//		if (StringUtils.hasText(fields)) {
//			return fields;
//		}
//
//		Document fields = getFieldsObject();
//		if (fields.isEmpty()) {
//			return null;
//		}
//		return toJson(fields);
//	}
//
//	StringQuery fields(String fields) {
//		this.fields = fields;
//		return this;
//	}

//	String toJson(Document source) {
//		return BsonUtils.writeJson(source).toJsonString();
//	}
}
