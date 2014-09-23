/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.mapping;

import java.util.Comparator;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;

/**
 * {@link Comparator} implementation that uses, in order, the
 * <ul>
 * <li>{@link PrimaryKeyColumn#type()}, then, if ordered the same,</li>
 * <li>{@link PrimaryKeyColumn#ordinal()}, then, if ordered the same</li>
 * <li>{@link PrimaryKeyColumn#name()}, then, if ordered the same,</li>
 * <li>{@link PrimaryKeyColumn#ordering()}.</li>
 * </ul>
 * 
 * @see PrimaryKeyType#compareTo(PrimaryKeyType)
 * @see Ordering#compareTo(Ordering)
 * @author Matthew T. Adams
 */
public enum CassandraPrimaryKeyColumnAnnotationComparator implements Comparator<PrimaryKeyColumn> {

	/**
	 * The sole instance of this class.
	 */
	IT;

	@Override
	public int compare(PrimaryKeyColumn left, PrimaryKeyColumn right) {

		int comparison = left.type().compareTo(right.type());
		if (comparison != 0) {
			return comparison;
		}

		comparison = new Integer(left.ordinal()).compareTo(right.ordinal());
		if (comparison != 0) {
			return comparison;
		}

		comparison = left.name().compareTo(right.name());
		if (comparison != 0) {
			return comparison;
		}

		return left.ordering().compareTo(right.ordering());
	}
}
