/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.support;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractSessionConfiguration;

import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.QueryOptions;

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

	@Override
	protected NettyOptions getNettyOptions() {
		return IntegrationTestNettyOptions.INSTANCE;
	}

	@Override
	protected QueryOptions getQueryOptions() {

		// The driver blocks otherwise up to 1 sec on schema refreshes.
		// see also https://datastax-oss.atlassian.net/browse/JAVA-1120
		// ideally, this issue will be resolved with Cassandra Java Driver 3.0.2
		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setRefreshSchemaIntervalMillis(0);
		return queryOptions;
	}
}
