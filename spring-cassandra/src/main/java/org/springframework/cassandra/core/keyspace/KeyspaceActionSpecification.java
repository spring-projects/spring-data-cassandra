package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.util.Assert;

/**
 * Abstract builder class to support the construction of keyspace specifications.
 * 
 * @author John McPeek
 * @author David Webb
 * @param <T> The subtype of the {@link KeyspaceActionSpecification}
 */
public abstract class KeyspaceActionSpecification<T extends KeyspaceActionSpecification<T>> {

	/**
	 * The name of the keyspace.
	 */
	private CqlIdentifier name;

	/**
	 * Sets the keyspace name.
	 * 
	 * @return this
	 */
	public T name(String name) {
		return name(cqlId(name));
	}

	/**
	 * Sets the keyspace name.
	 * 
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public T name(CqlIdentifier name) {
		Assert.notNull(name);
		this.name = name;
		return (T) this;
	}

	public CqlIdentifier getName() {
		return name;
	}

	/**
	 * Determine the KeyspaceActionSpecifications are the same if they have the same "name" and same class.
	 * 
	 * @param that The object to compare this to.
	 * @return Are this and that the same?
	 */
	@Override
	public boolean equals(Object that) {
		if (this == that) {
			return true;
		}
		if (that == null) {
			return false;
		}
		if (!(that instanceof KeyspaceActionSpecification)) {
			return false;
		}
		KeyspaceActionSpecification<?> other = (KeyspaceActionSpecification<?>) that;
		return this.name.equals(other.name) && this.getClass().equals(that.getClass());
	}

	@Override
	public int hashCode() {
		return name.hashCode() ^ getClass().hashCode();
	}
}
