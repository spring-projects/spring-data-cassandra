/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.util.Comparator;

/**
 * {@link Comparator} implementation that uses, in order, the...
 * <ul>
 * <li>{@link PrimaryKeyColumn#type()}, then, if ordered the same,</li>
 * <li>{@link PrimaryKeyColumn#ordinal()}, then, if ordered the same</li>
 * <li>{@link PrimaryKeyColumn#name()}, then, if ordered the same,</li>
 * <li>{@link PrimaryKeyColumn#ordering()}.</li>
 * </ul>
 *
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn
 */
public enum CassandraPrimaryKeyColumnAnnotationComparator implements Comparator<PrimaryKeyColumn> {

	/**
	 * Comparator instance.
	 */
	INSTANCE;

	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(PrimaryKeyColumn left, PrimaryKeyColumn right) {

		int comparison = left.type().compareTo(right.type());

		comparison = (comparison != 0 ? comparison : Integer.compare(left.ordinal(), right.ordinal()));
		comparison = (comparison != 0 ? comparison : left.name().compareTo(right.name()));
		comparison = (comparison != 0 ? comparison : left.ordering().compareTo(right.ordering()));

		return comparison;
	}
}
