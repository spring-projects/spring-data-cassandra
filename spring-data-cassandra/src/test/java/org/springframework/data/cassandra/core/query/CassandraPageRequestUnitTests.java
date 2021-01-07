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
 * Unit tests for {@link CassandraPageRequest}.
 *
 * @author Mark Paluch
 */
class CassandraPageRequestUnitTests {

	private ByteBuffer pagingState = ByteBuffer.allocate(1);

	@Test // DATACASS-56
	void shouldNotAllowNonZeroPageConstruction() {

		assertThatThrownBy(() -> CassandraPageRequest.of(1, 1)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> CassandraPageRequest.of(1, 1, Sort.unsorted()))
				.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> CassandraPageRequest.of(1, 1, Direction.ASC, "foo"))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // DATACASS-56
	void shouldCreateFirstUnsortedPageRequest() {

		CassandraPageRequest pageRequest = CassandraPageRequest.first(10);

		assertThat(pageRequest.hasNext()).isFalse();
		assertThat(pageRequest.getPageSize()).isEqualTo(10);
		assertThat(pageRequest.getSort()).isEqualTo(Sort.unsorted());
	}

	@Test // DATACASS-56
	void shouldCreateFirstSortedPageRequest() {

		CassandraPageRequest pageRequest = CassandraPageRequest.first(10, Direction.ASC, "foo");

		assertThat(pageRequest.hasNext()).isFalse();
		assertThat(pageRequest.getPageSize()).isEqualTo(10);
		assertThat(pageRequest.getSort()).isEqualTo(Sort.by(asc("foo")));
	}

	@Test // DATACASS-56
	void shouldFailIfNoNextPageIsAvailable() {

		CassandraPageRequest pageRequest = CassandraPageRequest.first(10, Direction.ASC, "foo");

		assertThatThrownBy(pageRequest::next).isInstanceOf(IllegalStateException.class);
	}

	@Test // DATACASS-56
	void shouldCreateNextPageRequest() {

		CassandraPageRequest pageRequest = CassandraPageRequest.first(10, Direction.ASC, "foo");
		CassandraPageRequest next = CassandraPageRequest.of(pageRequest, pagingState).next();

		assertThat(next.hasNext()).isFalse();
		assertThat(next.getPageSize()).isEqualTo(10);
	}

	@Test // DATACASS-56
	void shouldNotAllowPreviousPageNavigationToIntermediatePages() {

		CassandraPageRequest next = CassandraPageRequest.of(PageRequest.of(5, 10), pagingState);

		assertThatThrownBy(next::previous).isInstanceOf(IllegalStateException.class);
	}

	@Test // DATACASS-56
	void shouldCheckEquality() {

		CassandraPageRequest first = CassandraPageRequest.first(10);
		CassandraPageRequest anotherFirst = CassandraPageRequest.first(10);

		assertThat(first.hashCode()).isEqualTo(anotherFirst.hashCode());

		assertThat(first).isEqualTo(anotherFirst);
		assertThat(first).isEqualTo(first);

		CassandraPageRequest withPaging = CassandraPageRequest.of(first, pagingState).next();
		CassandraPageRequest anotherWithPaging = CassandraPageRequest.of(anotherFirst, pagingState).next();

		assertThat(withPaging.hashCode()).isEqualTo(anotherWithPaging.hashCode());
		assertThat(withPaging.hashCode()).isNotEqualTo(first.hashCode());

		assertThat(withPaging).isEqualTo(anotherWithPaging);
		assertThat(withPaging).isEqualTo(withPaging);
		assertThat(withPaging).isNotEqualTo(anotherFirst);
	}
}
