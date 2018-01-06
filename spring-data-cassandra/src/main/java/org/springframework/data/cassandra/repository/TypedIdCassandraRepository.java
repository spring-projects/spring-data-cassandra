/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository;

import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Cassandra-specific extension of the {@link CrudRepository} interface that allows the specification of a type for the
 * identity of the {@link Table @Table} (or {@link Persistable @Persistable}) type.
 * <p/>
 * If a single column comprises the identity of the entity, then you must do one of two things:
 * <ul>
 * <li>annotate the field or property in your entity with {@link PrimaryKey @PrimaryKey} and declare your repository
 * interface to be a subinterface of <em>this</em> interface, specifying the entity type and id type, or</li>
 * <li>annotate the field or property in your entity with {@link PrimaryKeyColumn @PrimaryKeyColumn} and declare your
 * repository interface to be a subinterface of {@link MapIdCassandraRepository}.</li>
 * </ul>
 * If multiple columns comprise the identity of the entity, then you must employ one of the following two strategies.
 * <ul>
 * <li><em>Strategy: use an explicit primary key class</em>
 * <ul>
 * <li>Define a primary key class (annotated with {@link PrimaryKeyClass @PrimaryKeyClass}) that represents your
 * entity's identity.</li>
 * <li>Define your entity to include a field or property of the type of your primary key class, and annotate that field
 * with {@link PrimaryKey @PrimaryKey}.</li>
 * <li>Define your repository interface to be a subinterface of <em>this</em> interface, including your entity type and
 * your primary key class type.</li>
 * </ul>
 * <li><em>Strategy: embed identity fields or properties directly in your entity and use
 * {@link MapIdCassandraRepository}</em></li>
 * <ul>
 * <li>Define your entity, including a field or property for each column, including those for partition and (optional)
 * cluster columns.</li>
 * <li>Annotate each partition &amp; cluster field or property with {@link PrimaryKeyColumn @PrimaryKeyColumn}</li>
 * <li>Define your repository interface to be a subinterface of {@link MapIdCassandraRepository}, which uses a provided
 * id type, {@link MapId} (implemented by {@link BasicMapId}).</li>
 * <li>Whenever you need a {@link MapId}, you can use the static factory method {@link BasicMapId#id()} (which is
 * convenient if you import statically) and the builder method {@link MapId#with(String, Object)} to easily construct an
 * id.</li>
 * </ul>
 * </ul>
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @deprecated since 2.0, use {@link CassandraRepository}.
 */
@NoRepositoryBean
@Deprecated
public interface TypedIdCassandraRepository<T, ID> extends CassandraRepository<T, ID> {}
