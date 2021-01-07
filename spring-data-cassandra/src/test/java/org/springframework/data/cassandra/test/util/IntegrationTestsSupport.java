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
package org.springframework.data.cassandra.test.util;

import java.util.UUID;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract base integration test class that starts an embedded Cassandra instance. Test clients can use the
 * {@link #cluster} instance to create sessions and get access. Expect the {@link #cluster} instance to be closed once
 * the test has been run.
 * <p>
 * This class is intended to be subclassed by integration test classes.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@ExtendWith(CassandraExtension.class)
public abstract class IntegrationTestsSupport {

	/**
	 * Create and return a random {@link UUID} as a {@link String}.
	 *
	 * @return a random {@link UUID} as a {@link String}.
	 * @see java.util.UUID#randomUUID()
	 */
	protected static String uuid() {
		return UUID.randomUUID().toString();
	}

}
