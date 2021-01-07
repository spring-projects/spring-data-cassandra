/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.Sort.Order.*;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

/**
 * Unit tests for {@link Query}.
 *
 * @author Mark Paluch
 */
class QueryUnitTests {

	@Test // DATACASS-343
	void shouldCreateFromChainedCriteria() {

		Query query = Query.query(Criteria.where("userId").is("foo")).and(Criteria.where("userComment").is("bar"));

		assertThat(query).hasSize(2);
		assertThat(query.getCriteriaDefinitions()).contains(Criteria.where("userId").is("foo"));
		assertThat(query.getCriteriaDefinitions()).contains(Criteria.where("userComment").is("bar"));
	}

	@Test // DATACASS-343
	void shouldRepresentQueryToString() {

		Query query = Query.query(Criteria.where("userId").is("foo")).and(Criteria.where("userComment").is("bar"))
				.sort(Sort.by("foo", "bar")) //
				.columns(Columns.from("foo").ttl("bar")) //
				.limit(5);

		assertThat(query.toString()).isEqualTo(
				"Query: userId = 'foo' AND userComment = 'bar', Columns: foo, TTL(bar), Sort: foo: ASC,bar: ASC, Limit: 5");
	}

	@Test // DATACASS-343
	void shouldConfigureQueryObject() {

		Query query = Query.query(Criteria.where("foo").is("bar"));
		Sort sort = Sort.by("a", "b");
		Columns columns = Columns.from("a", "b");

		query = query.sort(sort).columns(columns).limit(10).withAllowFiltering();

		assertThat(query).hasSize(1);
		assertThat(query.getColumns()).isEqualTo(columns);
		assertThat(query.getSort()).isEqualTo(sort);
		assertThat(query.getLimit()).isEqualTo(10);
		assertThat(query.isAllowFiltering()).isTrue();
	}

	@Test // DATACASS-56
	void shouldApplyPageRequests() {

		ByteBuffer pagingState = ByteBuffer.allocate(0);

		CassandraPageRequest pageRequest = CassandraPageRequest.of(PageRequest.of(0, 42, Direction.ASC, "foo"), pagingState)
				.next();

		Query query = Query.empty().pageRequest(pageRequest);

		assertThat(query.getSort()).isEqualTo(Sort.by(asc("foo")));
		assertThat(query.getPagingState()).contains(pagingState);
		assertThat(query.getQueryOptions())
				.hasValueSatisfying(actual -> assertThat(actual).extracting("pageSize").isEqualTo(42));
	}
}
