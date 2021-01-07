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
package org.springframework.data.cassandra.core.cql.session.lookup;

import org.springframework.data.cassandra.SessionFactory;

/**
 * Strategy interface for looking up {@link SessionFactory} by name.
 * <p>
 * Implementing classes resolve session factories keyed by {@link String} from an underlying source such as a
 * {@link java.util.Map} or the {@link org.springframework.beans.factory.BeanFactory}.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see AbstractRoutingSessionFactory
 */
@FunctionalInterface
public interface SessionFactoryLookup {

	/**
	 * Implementations must implement this method to retrieve the {@link SessionFactory} identified by the given name from
	 * their backing store.
	 *
	 * @param sessionFactoryName the name of the {@link SessionFactory}.
	 * @return the {@link SessionFactory} (never {@literal null}).
	 * @throws SessionFactoryLookupFailureException if the lookup failed.
	 */
	SessionFactory getSessionFactory(String sessionFactoryName) throws SessionFactoryLookupFailureException;

}
