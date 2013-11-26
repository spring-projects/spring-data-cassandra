package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.cassandra.core.cql.CqlStringUtils.identifize;
import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;
import static org.springframework.cassandra.core.KeyType.PARTITION;
import static org.springframework.cassandra.core.KeyType.PRIMARY;
import static org.springframework.cassandra.core.Ordering.ASCENDING;

import org.springframework.cassandra.core.KeyType;
import org.springframework.cassandra.core.Ordering;

import com.datastax.driver.core.DataType;

/**
 * Builder class to help construct CQL statements that involve column manipulation. Not threadsafe.
 * <p/>
 * Use {@link #name(String)} and {@link #type(String)} to set the name and type of the column, respectively. To specify
 * a <code>PRIMARY KEY</code> column, use {@link #primary()} or {@link #primary(Ordering)}. To specify that the
 * <code>PRIMARY KEY</code> column is or is part of the partition key, use {@link #partition()} instead of
 * {@link #primary()} or {@link #primary(Ordering)}.
 * 
 * @author Matthew T. Adams
 */
public class ColumnSpecification {

	/**
	 * Default ordering of primary key fields; value is {@link Ordering#ASCENDING}.
	 */
	public static final Ordering DEFAULT_ORDERING = ASCENDING;

	private String name;
	private DataType type; // TODO: determining if we should be coupling this to Datastax Java Driver type?
	private KeyType keyType;
	private Ordering ordering;

	/**
	 * Sets the column's name.
	 * 
	 * @return this
	 */
	public ColumnSpecification name(String name) {
		checkIdentifier(name);
		this.name = name;
		return this;
	}

	/**
	 * Sets the column's type.
	 * 
	 * @return this
	 */
	public ColumnSpecification type(DataType type) {
		this.type = type;
		return this;
	}

	/**
	 * Identifies this column as a primary key column that is also part of a partition key. Sets the column's
	 * {@link #keyType} to {@link KeyType#PARTITION} and its {@link #ordering} to <code>null</code>.
	 * 
	 * @return this
	 */
	public ColumnSpecification partition() {
		return partition(true);
	}

	/**
	 * Toggles the identification of this column as a primary key column that also is or is part of a partition key. Sets
	 * {@link #ordering} to <code>null</code> and, if the given boolean is <code>true</code>, then sets the column's
	 * {@link #keyType} to {@link KeyType#PARTITION}, else sets it to <code>null</code>.
	 * 
	 * @return this
	 */
	public ColumnSpecification partition(boolean partition) {
		this.keyType = partition ? PARTITION : null;
		this.ordering = null;
		return this;
	}

	/**
	 * Identifies this column as a primary key column with default ordering. Sets the column's {@link #keyType} to
	 * {@link KeyType#PRIMARY} and its {@link #ordering} to {@link #DEFAULT_ORDERING}.
	 * 
	 * @return this
	 */
	public ColumnSpecification primary() {
		return primary(DEFAULT_ORDERING);
	}

	/**
	 * Identifies this column as a primary key column with the given ordering. Sets the column's {@link #keyType} to
	 * {@link KeyType#PRIMARY} and its {@link #ordering} to the given {@link Ordering}.
	 * 
	 * @return this
	 */
	public ColumnSpecification primary(Ordering order) {
		return primary(order, true);
	}

	/**
	 * Toggles the identification of this column as a primary key column. If the given boolean is <code>true</code>, then
	 * sets the column's {@link #keyType} to {@link KeyType#PARTITION} and {@link #ordering} to the given {@link Ordering}
	 * , else sets both {@link #keyType} and {@link #ordering} to <code>null</code>.
	 * 
	 * @return this
	 */
	public ColumnSpecification primary(Ordering order, boolean primary) {
		this.keyType = primary ? PRIMARY : null;
		this.ordering = primary ? order : null;
		return this;
	}

	/**
	 * Sets the column's {@link #keyType}.
	 * 
	 * @return this
	 */
	/* package */ColumnSpecification keyType(KeyType keyType) {
		this.keyType = keyType;
		return this;
	}

	/**
	 * Sets the column's {@link #ordering}.
	 * 
	 * @return this
	 */
	/* package */ColumnSpecification ordering(Ordering ordering) {
		this.ordering = ordering;
		return this;
	}

	public String getName() {
		return name;
	}

	public String getNameAsIdentifier() {
		return identifize(name);
	}

	public DataType getType() {
		return type;
	}

	public KeyType getKeyType() {
		return keyType;
	}

	public Ordering getOrdering() {
		return ordering;
	}

	public String toCql() {
		return toCql(null).toString();
	}

	public StringBuilder toCql(StringBuilder cql) {
		return (cql = noNull(cql)).append(name).append(" ").append(type);
	}

	@Override
	public String toString() {
		return toCql(null).append(" /* keyType=").append(keyType).append(", ordering=").append(ordering).append(" */ ")
				.toString();
	}
}