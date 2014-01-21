package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.cassandra.core.cql.CqlStringUtils.identifize;

/**
 * Abstract builder class to support the construction of keyspace specifications.
 * 
 * @author John McPeek
 * @author David Webb
 * @param <T> The subtype of the {@link KeyspaceActionSpecification}
 */
public abstract class KeyspaceActionSpecification<T extends KeyspaceActionSpecification<T>> {

	/**
	 * The name of the table.
	 */
	private String name;

	/**
	 * Sets the keyspace name.
	 * 
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public T name(String name) {
		checkIdentifier(name);
		this.name = name;
		return (T) this;
	}

	public String getName() {
		return name;
	}

	public String getNameAsIdentifier() {
		return identifize(name);
	}

	/**
	 * For debugging KeyspaceActionSprcifications
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Keyspace Action Specification {name: " + name + ", class: " + this.getClass() + "}");
		return sb.toString();
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
		KeyspaceActionSpecification<?> thatSpec = (KeyspaceActionSpecification<?>) that;
		return this.name.equals(thatSpec.name) && this.getClass().equals(that.getClass());
	}

	@Override
	public int hashCode() {
		return this.name.hashCode() ^ this.getClass().hashCode();
	}

}
