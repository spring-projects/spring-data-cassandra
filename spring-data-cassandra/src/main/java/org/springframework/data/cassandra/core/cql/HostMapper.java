/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import java.util.Collection;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * An interface used by {@link CqlTemplate} for mapping {@link Host}s of a {@link com.datastax.driver.core.Metadata} on
 * a per-item basis.. Implementations of this interface perform the actual work of mapping each host to a result object,
 * but don't need to worry about exception handling. {@link DriverException} will be caught and handled by the calling
 * {@link CqlTemplate}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see CqlTemplate
 */
@FunctionalInterface
public interface HostMapper<T> {

	/**
	 * Implementations must implement this method to map each {@link Host} in the
	 * {@link com.datastax.driver.core.Metadata}.
	 *
	 * @param hosts the {@link Iterable} of {@link Host}s to map, must not be {@literal null}.
	 * @return the result objects for the given hosts.
	 * @throws DriverException if a {@link DriverException} is encountered mapping values (that is, there's no need to
	 *           catch {@link DriverException}).
	 */
	Collection<T> mapHosts(Iterable<Host> hosts) throws DriverException;
}
