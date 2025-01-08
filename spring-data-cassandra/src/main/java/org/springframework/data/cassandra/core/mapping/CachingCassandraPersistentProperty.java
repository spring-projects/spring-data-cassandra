/*
 * Copyright 2021-2025 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.lang.Nullable;

/**
 * {@link BasicCassandraPersistentProperty} that pre-computes primary key and embedded flags.
 *
 * @author Mark Paluch
 * @author Aleksei Zotov
 * @since 3.1.4
 */
public class CachingCassandraPersistentProperty extends BasicCassandraPersistentProperty {

	private final @Nullable Ordering primaryKeyOrdering;
	private final boolean isCompositePrimaryKey;
	private final boolean isClusterKeyColumn;
	private final boolean isPartitionKeyColumn;
	private final boolean isPrimaryKeyColumn;
	private final boolean isEmbedded;
	private final boolean isStaticColumn;
	private final Map<Class<? extends Annotation>, Optional<AnnotatedType>> findAnnotatedTypeCache = new ConcurrentHashMap<>();

	public CachingCassandraPersistentProperty(Property property, CassandraPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder) {
		super(property, owner, simpleTypeHolder);

		primaryKeyOrdering = super.getPrimaryKeyOrdering();
		isCompositePrimaryKey = super.isCompositePrimaryKey();
		isClusterKeyColumn = super.isClusterKeyColumn();
		isPartitionKeyColumn = super.isPartitionKeyColumn();
		isPrimaryKeyColumn = super.isPrimaryKeyColumn();
		isEmbedded = super.isEmbedded();
		isStaticColumn = super.isStaticColumn();
	}

	@Nullable
	@Override
	public Ordering getPrimaryKeyOrdering() {
		return primaryKeyOrdering;
	}

	@Override
	public boolean isCompositePrimaryKey() {
		return isCompositePrimaryKey;
	}

	@Override
	public boolean isClusterKeyColumn() {
		return isClusterKeyColumn;
	}

	@Override
	public boolean isPartitionKeyColumn() {
		return isPartitionKeyColumn;
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return isPrimaryKeyColumn;
	}

	@Override
	public boolean isStaticColumn() {
		return isStaticColumn;
	}

	@Override
	public boolean isEmbedded() {
		return isEmbedded;
	}

	@Override
	public AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType) {
		return findAnnotatedTypeCache
				.computeIfAbsent(annotationType, key -> Optional.ofNullable(super.findAnnotatedType(key))).orElse(null);
	}
}
