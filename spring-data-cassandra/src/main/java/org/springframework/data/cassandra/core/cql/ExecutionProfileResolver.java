/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.Statement;

import org.springframework.util.Assert;

/**
 * Resolver for a {@link com.datastax.oss.driver.api.core.config.DriverExecutionProfile} used with
 * {@link com.datastax.oss.driver.api.core.cql.Statement#setExecutionProfileName(String)} or
 * {@link com.datastax.oss.driver.api.core.cql.Statement#setExecutionProfile(DriverExecutionProfile)}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
@FunctionalInterface
public interface ExecutionProfileResolver {

	/**
	 * Apply an execution profile based on the {@link Statement}.
	 *
	 * @param statement the statement to inspect and to apply the {@code driver execution profile} to.
	 * @return the statement with the profile applied.
	 */
	Statement<?> apply(Statement<?> statement);

	/**
	 * Create a no-op {@link ExecutionProfileResolver} that preserves the {@link Statement} settings.
	 *
	 * @return no-op {@link ExecutionProfileResolver} that preserves the {@link Statement} settings.
	 */
	static ExecutionProfileResolver none() {
		return statement -> statement;
	}

	/**
	 * Create a {@link ExecutionProfileResolver} from a {@link DriverExecutionProfile} to apply the profile object.
	 *
	 * @param driverExecutionProfile must not be {@literal null}.
	 * @return a {@link ExecutionProfileResolver} that applies the given {@link DriverExecutionProfile}.
	 * @see Statement#setExecutionProfile(DriverExecutionProfile)
	 */
	static ExecutionProfileResolver from(DriverExecutionProfile driverExecutionProfile) {

		Assert.notNull(driverExecutionProfile, "DriverExecutionProfile must not be null");

		return statement -> statement.setExecutionProfile(driverExecutionProfile);
	}

	/**
	 * Create a {@link ExecutionProfileResolver} from a {@code profileName}.
	 *
	 * @param profileName must not be {@literal null} or empty.
	 * @return a {@link ExecutionProfileResolver} that applies the given {@code profileName}.
	 * @see Statement#setExecutionProfileName(String)
	 */
	static ExecutionProfileResolver from(String profileName) {

		Assert.hasText(profileName, "DriverExecutionProfile name must not be empty");

		return statement -> statement.setExecutionProfileName(profileName);
	}
}
