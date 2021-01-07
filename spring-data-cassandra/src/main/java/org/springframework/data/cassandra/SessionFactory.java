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
package org.springframework.data.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;

/**
 * A factory for Apache Cassandra {@link Session sessions}.
 *
 * A {@link SessionFactory} object is the preferred means of getting a connection. The {@link SessionFactory} interface
 * is implemented by a {@link CqlSession} provider.
 *
 * A {@link SessionFactory} object can have properties that can be modified when necessary. For example, if the
 * {@link CqlSession} is moved to a different server, the property for the server can be changed. The benefit is that
 * because the data source's properties can be changed, any code accessing that {@link SessionFactory} does not need to
 * be changed.
 *
 * @author Mark Paluch
 * @see com.datastax.oss.driver.api.core.CqlSession
 * @see com.datastax.oss.driver.api.core.session.Session
 * @since 2.0
 */
@FunctionalInterface
public interface SessionFactory {

	/**
	 * Attempts to establish a {@link CqlSession} with the connection infrastructure that this {@link SessionFactory}
	 * object represents.
	 *
	 * @return a {@link CqlSession} to Apache Cassandra.
	 * @see com.datastax.oss.driver.api.core.CqlSession
	 */
	CqlSession getSession();

}
