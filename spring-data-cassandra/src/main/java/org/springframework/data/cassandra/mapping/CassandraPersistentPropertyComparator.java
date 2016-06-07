/*
 * Copyright 2013-2016 the original author or authors
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

/**
 * {@link Comparator} implementation that orders {@link CassandraPersistentProperty} instances.
 * <p/>
 * Composite primary key properties and primary key properties sort before non-primary key properties.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author John Blum
 * @see java.util.Comparator
 * @see org.springframework.data.cassandra.mapping.CassandraPersistentProperty
 */
public enum CassandraPersistentPropertyComparator implements Comparator<CassandraPersistentProperty> {
	IT;

	@Override
	public int compare(CassandraPersistentProperty left, CassandraPersistentProperty right) {

		if (left == null && right == null) {
			return 0;
		}
		else if (left != null && right == null) {
			return 1;
		}
		else if (left == null) {
			return -1;
		}
		else if (left.equals(right)) {
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
			return CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(left.findAnnotation(PrimaryKeyColumn.class),
				right.findAnnotation(PrimaryKeyColumn.class));
		}

		boolean leftIsKey = leftIsCompositePrimaryKey || leftIsPrimaryKey;
		boolean rightIsKey = rightIsCompositePrimaryKey || rightIsPrimaryKey;

		if (leftIsKey && !rightIsKey) {
			return -1;
		}
		else if (!leftIsKey && rightIsKey) {
			return 1;
		}

		// else, neither property is a composite primary key nor a primary key; compare @Column annotations

		Column leftColumn = left.findAnnotation(Column.class);
		Column rightColumn = right.findAnnotation(Column.class);

		if (leftColumn != null && rightColumn != null) {
			return CassandraColumnAnnotationComparator.IT.compare(leftColumn, rightColumn);
		}
		else if (leftColumn != null) {
			return leftColumn.value().compareTo(left.getName());
		}
		else if (rightColumn != null) {
			return left.getName().compareTo(rightColumn.value());
		}
		else {
			return left.getName().compareTo(right.getName());
		}
	}
}
