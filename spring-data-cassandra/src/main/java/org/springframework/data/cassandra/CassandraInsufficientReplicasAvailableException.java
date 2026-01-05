/*
 * Copyright 2013-present the original author or authors.
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

import java.io.Serial;

import org.springframework.dao.TransientDataAccessException;

/**
 * Spring data access exception for Cassandra when insufficient replicas are available for a given consistency level.
 *
 * @author Matthew T. Adams
 */
public class CassandraInsufficientReplicasAvailableException extends TransientDataAccessException {

	private static final @Serial long serialVersionUID = 6415130674604814905L;

	private int numberRequired;
	private int numberAlive;

	/**
	 * Constructor for {@link CassandraInsufficientReplicasAvailableException}.
	 *
	 * @param msg the detail message.
	 */
	public CassandraInsufficientReplicasAvailableException(String msg) {
		super(msg);
	}

	/**
	 * Constructor for {@link CassandraInsufficientReplicasAvailableException}.
	 *
	 * @param numberRequired the required number of replicas.
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public CassandraInsufficientReplicasAvailableException(int numberRequired, int numberAlive, String msg,
			Throwable cause) {
		super(msg, cause);
		this.numberRequired = numberRequired;
		this.numberAlive = numberAlive;
	}

	public int getNumberRequired() {
		return numberRequired;
	}

	public int getNumberAlive() {
		return numberAlive;
	}
}
