/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Value object representing a Find by ID query supporting also {@link MapId}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
class FindByIdQuery {

	private final @Nullable String idProperty;
	private final List<Object> idCollection;

	private FindByIdQuery(@Nullable String idProperty, List<Object> idCollection) {

		this.idProperty = idProperty;
		this.idCollection = idCollection;
	}

	/**
	 * Create a new {@link FindByIdQuery} given {@link Iterable} of {@code ID}s. Id's can be either scalar values or
	 * {@link MapId}s. In case of the latter, this method discovers the {@link #getIdProperty() Id property name}.
	 *
	 * @param ids
	 * @return
	 */
	static FindByIdQuery forIds(Iterable<?> ids) {

		Assert.notNull(ids, "The given Iterable of ids must not be null");

		List<Object> idCollection = new ArrayList<>();
		String idField = null;

		for (Object id : ids) {

			if (id instanceof MapId) {

				MapId mapId = (MapId) id;
				Iterator<String> iterator = mapId.keySet().iterator();

				if (mapId.size() > 1) {
					throw new InvalidDataAccessApiUsageException("MapId with multiple keys are not supported");
				}

				if (!iterator.hasNext()) {
					throw new InvalidDataAccessApiUsageException("MapId is empty");
				} else {

					idField = iterator.next();
					idCollection.add(mapId.get(idField));
				}
			} else {
				idCollection.add(id);
			}
		}

		return new FindByIdQuery(idField, idCollection);
	}

	/**
	 * Check if the {@link Iterable} of {@code ID}s contains composite keys.
	 *
	 * @param ids
	 * @return
	 */
	static boolean hasCompositeKeys(Iterable<?> ids) {

		Assert.notNull(ids, "The given Iterable of ids must not be null");

		for (Object id : ids) {

			if (id instanceof MapId) {

				MapId mapId = (MapId) id;

				if (mapId.size() > 1) {
					return true;
				}
			}
		}

		return false;
	}

	@Nullable
	String getIdProperty() {
		return idProperty;
	}

	List<Object> getIdCollection() {
		return idCollection;
	}
}
