/*
 * Copyright 2022-present the original author or authors.
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
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;

import org.jspecify.annotations.Nullable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;

/**
 * A {@link Observation.Context} for {@link CqlSession}.
 *
 * @author Greg Turnquist
 * @author Mark Paluch
 * @since 4.0
 */
public class CassandraObservationContext extends SenderContext<Object> {

	private final Statement<?> statement;

	private final boolean prepare;
	private final String methodName;
	private final String sessionName;
	private final String keyspaceName;

	private volatile @Nullable Node node;

	public CassandraObservationContext(Statement<?> statement, String remoteServiceName, boolean prepare,
			String methodName, String sessionName, String keyspaceName) {

		super((carrier, key, value) -> {}, Kind.CLIENT);

		this.statement = statement;
		this.prepare = prepare;
		this.methodName = methodName;
		this.sessionName = sessionName;
		this.keyspaceName = keyspaceName;

		setRemoteServiceName(remoteServiceName);
	}

	public Statement<?> getStatement() {
		return statement;
	}

	public boolean isPrepare() {
		return prepare;
	}

	public String getMethodName() {
		return methodName;
	}

	public String getSessionName() {
		return sessionName;
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public @Nullable Node getNode() {
		return node;
	}
}
