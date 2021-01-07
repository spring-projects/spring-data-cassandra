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
package org.springframework.data.cassandra.core.cql.session;

import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.ReactiveRowMapperResultSetExtractor;

/**
 * Default implementation of {@link ReactiveSessionFactory}.
 * <p>
 * This implementation returns always the same {@link ReactiveSession}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class DefaultReactiveSessionFactory implements ReactiveSessionFactory {

	private final Mono<ReactiveSession> session;

	/**
	 * Create a new {@link ReactiveRowMapperResultSetExtractor}.
	 *
	 * @param session the {@link ReactiveSession} provides connections to Cassandra, must not be {@literal null}.
	 */
	public DefaultReactiveSessionFactory(ReactiveSession session) {
		this.session = Mono.just(session);
	}

	@Override
	public Mono<ReactiveSession> getSession() {
		return session;
	}
}
