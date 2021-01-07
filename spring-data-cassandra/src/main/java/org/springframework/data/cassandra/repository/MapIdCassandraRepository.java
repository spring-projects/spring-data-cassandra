/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository;

import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.core.mapping.MapIdentifiable;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Cassandra repository interface using {@link MapId} to represent Ids.
 * <p/>
 * This interface uses {@link MapId} for the id type, allowing you to annotate entity fields or properties with
 * {@link PrimaryKeyColumn @PrimaryKeyColumn}. Use this interface if you do not require a composite primary key class
 * and want to specify the Id with {@link MapId}.
 * <p/>
 * Steps to use this interface:
 * <ul>
 * <li>Define your entity, including a field or property for each column, including those for partition and (optional)
 * cluster columns.</li>
 * <li>Annotate each partition &amp; cluster field or property with {@link PrimaryKeyColumn @PrimaryKeyColumn}</li>
 * <li>Define your repository interface to be a subinterface of this interface, which uses a provided id type,
 * {@link MapId} (implemented by {@link BasicMapId}).</li>
 * <li>Whenever you need a {@link MapId}, you can use the static factory method {@link BasicMapId#id()} (which is
 * convenient if you import statically) and the builder method {@link MapId#with(String, Object)} to easily construct an
 * id.</li>
 * <li>Optionally, entity class authors can have their entities implement {@link MapIdentifiable}, to make it easier and
 * quicker for entity clients to get the entity's identity.</li>
 * </ul>
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @since 2.0
 * @see CassandraRepository
 * @see MapId
 * @see MapIdentifiable
 */
@NoRepositoryBean
public interface MapIdCassandraRepository<T> extends CassandraRepository<T, MapId> {}
