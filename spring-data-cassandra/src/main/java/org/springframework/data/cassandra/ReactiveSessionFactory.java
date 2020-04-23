/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.cassandra;

import reactor.core.publisher.Mono;

/**
 * Strategy interface to produce {@link ReactiveSession} instances.
 * <p>
 * Spring provides a {@link org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory}
 * implementation that just returns the same {@link ReactiveSession} instance. Implementations are free to return the
 * same session or route calls to different sessions. DefaultSessionFactory
 *
 * @author Mark Paluch
 * @see 2.0
 * @see ReactiveSession
 * @see org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory
 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate
 */
@FunctionalInterface
public interface ReactiveSessionFactory {

	/**
	 * Return a {@link ReactiveSession} to be used directly or inside a callback inside {@link ReactiveCqlTemplate}.
	 *
	 * @return a {@link ReactiveSession}.
	 */
	Mono<ReactiveSession> getSession();
}
