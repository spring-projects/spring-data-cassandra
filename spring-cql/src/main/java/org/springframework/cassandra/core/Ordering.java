/*
 * Copyright 2013-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core;

import java.util.Comparator;

/**
 * Enum for Cassandra primary key column ordering. Implements {@link Comparator} in that {@link Ordering#ASCENDING} is
 * ordered before {@link Ordering#DESCENDING}.
 *
 * @author Matthew T. Adams
 */
public enum Ordering implements Comparator<Ordering> {

	/**
	 * Ascending Cassandra column ordering.
	 */
	ASCENDING("ASC"),

	/**
	 * Descending Cassandra column ordering.
	 */
	DESCENDING("DESC");

	private String cql;

	Ordering(String cql) {
		this.cql = cql;
	}

	/**
	 * Returns the CQL keyword of this {@link Ordering}.
	 */
	public String cql() {
		return cql;
	}

	@Override
	public int compare(Ordering l, Ordering r) {
		if (l == r) {
			return 0;
		}
		if (l == null) {
			return 1;
		}
		if (r == null) {
			return -1;
		}
		return (l == ASCENDING && r == DESCENDING) ? 1 : -1;
	}
}
