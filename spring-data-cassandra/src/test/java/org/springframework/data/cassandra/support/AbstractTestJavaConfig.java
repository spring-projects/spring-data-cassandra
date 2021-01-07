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
package org.springframework.data.cassandra.support;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractSessionConfiguration;

/**
 * Java-based configuration for integration tests using defaults for a smooth test run.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@Configuration
public abstract class AbstractTestJavaConfig extends AbstractSessionConfiguration {

	private static final CassandraConnectionProperties PROPERTIES = new CassandraConnectionProperties();

	@Override
	protected int getPort() {
		return PROPERTIES.getCassandraPort();
	}

}
