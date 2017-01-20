/*
 * Copyright 2013-2017 the original author or authors
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
import java.util.Optional;

import org.springframework.cassandra.core.cql.CqlIdentifier;

/**
 * {@link Comparator} implementation that orders {@link CassandraPersistentProperty} instances.
 * <p>
 * Composite primary key properties and primary key properties sort before non-primary key properties. Ordering rules:
 * <ul>
 * <li>Composite primary keys first (equal if both {@link CassandraPersistentProperty} are a composite primary key)</li>
 * <li>Primary key columns (see {@link CassandraPrimaryKeyColumnAnnotationComparator}, compare by ordinal/name/ordering)
 * </li>
 * <li>Regular columns, compared by column name (see
 * {@link org.springframework.cassandra.core.cql.CqlIdentifier#compareTo(CqlIdentifier)})</li>
 * </ul>
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author John Blum
 * @see java.util.Comparator
 * @see org.springframework.data.cassandra.mapping.CassandraPersistentProperty
 */
public enum CassandraPersistentPropertyComparator implements Comparator<CassandraPersistentProperty> {

	/**
	 * Comparator instance.
	 */
	INSTANCE,

	/**
	 * @deprecated as of 1.5, use {@link #INSTANCE}
	 */
	@Deprecated
	IT;

	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(CassandraPersistentProperty left, CassandraPersistentProperty right) {

		if (left == null && right == null) {
			return 0;
		} else if (left != null && right == null) {
			return 1;
		} else if (left == null) {
			return -1;
		} else if (left.equals(right)) {
			return 0;
		}

		boolean leftIsCompositePrimaryKey = left.isCompositePrimaryKey();
		boolean rightIsCompositePrimaryKey = right.isCompositePrimaryKey();

		if (leftIsCompositePrimaryKey && rightIsCompositePrimaryKey) {
			return 0;
		}

		boolean leftIsPrimaryKey = left.isPrimaryKeyColumn();
		boolean rightIsPrimaryKey = right.isPrimaryKeyColumn();

		if (leftIsPrimaryKey && rightIsPrimaryKey) {

			Optional<PrimaryKeyColumn> optionalLeftAnnotation = left.findAnnotation(PrimaryKeyColumn.class);
			Optional<PrimaryKeyColumn> optionaRightAnnotation = right.findAnnotation(PrimaryKeyColumn.class);

			return optionalLeftAnnotation.map(leftAnnotation -> {
				return optionaRightAnnotation.map(rightAnnotation -> CassandraPrimaryKeyColumnAnnotationComparator.INSTANCE
						.compare(leftAnnotation, rightAnnotation)).orElse(0);

			}).orElse(0);
		}

		boolean leftIsKey = leftIsCompositePrimaryKey || leftIsPrimaryKey;
		boolean rightIsKey = rightIsCompositePrimaryKey || rightIsPrimaryKey;

		if (leftIsKey && !rightIsKey) {
			return -1;
		} else if (!leftIsKey && rightIsKey) {
			return 1;
		}

		// else, neither property is a composite primary key nor a primary key; compare @Column annotations
		return left.getColumnName().compareTo(right.getColumnName());
	}
}
