package org.springframework.data.cassandra.repository;

import java.io.Serializable;

import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Basic Cassandra repository interface.
 * <p/>
 * This interface uses {@link MapId} for the id type, allowing you to annotate entity fields or properties with
 * {@link PrimaryKeyColumn @PrimaryKeyColumn}. For a full discussion of this interface, including the use of custom
 * primary key classes, see {@link TypedIdCassandraRepository}.
 * <p/>
 * Steps to use this interface:
 * <ul>
 * <li>Define your entity, including a field or property for each column, including those for partition and (optional)
 * cluster columns.</li>
 * <li>Annotate each partition &amp; cluster field or property with {@link PrimaryKeyColumn @PrimaryKeyColumn}</li>
 * <li>Define your repository interface to be a subinterface of this interface, which uses a provided id type,
 * {@link MapId} (implemented by {@link BasicMapId}).</li>
 * <li>Whenever you need a {@link MapId}, you can use the static factory method {@link BasicMapId#id()} (which is
 * convenient if you import statically) and the builder method {@link MapId#with(String, Serializable)} to easily
 * construct an id.</li>
 * <li>Optionally, entity class authors can have their entities implement {@link MapIdentifiable}, to make it easier and
 * quicker for entity clients to get the entity's identity.</li>
 * </ul>
 * 
 * @param <T> The type of the persistent entity.
 * @see TypedIdCassandraRepository
 * @see MapId
 * @See {@link MapIdentifiable}
 * @author Matthew T. Adams
 */
@NoRepositoryBean
public interface CassandraRepository<T> extends TypedIdCassandraRepository<T, MapId> {}
