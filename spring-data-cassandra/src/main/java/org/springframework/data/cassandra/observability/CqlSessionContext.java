/*
 * Copyright 2013-2022 the original author or authors.
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
package org.springframework.data.cassandra.observability;

import io.micrometer.observation.Observation;

import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * A {@link Observation.Context} for {@link CqlSession}.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class CqlSessionContext extends Observation.Context {

	private final @Nullable Statement<?> statement;
	private final String methodName;
	private final @Nullable CqlSession delegateSession;

	public CqlSessionContext(@Nullable Statement<?> statement, String methodName, @Nullable CqlSession delegateSession) {

		this.statement = statement;
		this.methodName = methodName;
		this.delegateSession = delegateSession;
	}

	@Nullable
	public Statement<?> getStatement() {
		return statement;
	}

	public String getMethodName() {
		return methodName;
	}

	@Nullable
	public CqlSession getDelegateSession() {
		return delegateSession;
	}
}
