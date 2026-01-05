/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.cassandra.core;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;

/**
 * Utility methods to create {@link OptimisticLockingFailureException}s.
 * <p>
 * Strictly for internal use within the framework.
 *
 * @author Mark Paluch
 * @since 4.5.7
 */
class OptimisticLockingUtils {

	/**
	 * Create an {@link OptimisticLockingFailureException} for an insert failure.
	 *
	 * @param entity entity to be inserted.
	 * @return the exception.
	 */
	public static OptimisticLockingFailureException insertFailed(EntityOperations.AdaptibleEntity<?> entity) {

		return new OptimisticLockingFailureException(String.format(
				"Failed to insert versioned entity with id '%s' (version '%s') in table [%s]; Row already exists.",
				entity.getIdentifier(), entity.getVersion(), entity.getPersistentEntity().getTableName()));
	}

	/**
	 * Create an {@link OptimisticLockingFailureException} for an update failure.
	 *
	 * @param entity entity to be updated.
	 * @return the exception.
	 */
	public static OptimisticLockingFailureException updateFailed(EntityOperations.AdaptibleEntity<?> entity) {

		return new OptimisticLockingFailureException(String.format(
				"Failed to update versioned entity with id '%s' (version '%s') in table [%s]; Was the entity updated or deleted concurrently?",
				entity.getIdentifier(), entity.getVersion(), entity.getPersistentEntity().getTableName()));
	}

	/**
	 * Create an {@link OptimisticLockingFailureException} for a delete failure.
	 *
	 * @param entity actual entity to be deleted.
	 * @param persistentEntity the {@link CassandraPersistentEntity} metadata.
	 * @return the exception.
	 */
	public static OptimisticLockingFailureException deleteFailed(EntityOperations.AdaptibleEntity<?> entity) {

		return new OptimisticLockingFailureException(String.format(
				"Failed to delete versioned entity with id '%s' (version '%s') in table [%s]; Was the entity updated or deleted concurrently?",
				entity.getIdentifier(), entity.getVersion(), entity.getPersistentEntity().getTableName()));
	}

}
