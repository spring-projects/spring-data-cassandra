package org.springframework.data.cassandra.cql.builder;

/**
 * Interface to represent option types.
 * 
 * @author Matthew T. Adams
 */
public interface Option {

	/**
	 * The type that values must be able to be coerced into for this option.
	 */
	Class<?> getType();

	/**
	 * The (usually lower-cased, underscore-separated) name of this table option.
	 */
	String getName();

	/**
	 * Whether this option takes a value.
	 */
	boolean takesValue();

	/**
	 * Whether this option should escape single quotes in its value.
	 */
	boolean escapesValue();

	/**
	 * Whether this option's value should be single-quoted.
	 */
	boolean quotesValue();

	/**
	 * Whether this option requires a value.
	 */
	boolean requiresValue();

	/**
	 * Checks that the given value can be coerced into the type given by {@link #getType()}.
	 */
	void checkValue(Object value);

	/**
	 * Tests whether the given value can be coerced into the type given by {@link #getType()}.
	 */
	boolean isCoerceable(Object value);

	/**
	 * Renders the given value to a string according to this option's settings. Given <code>null</code>, returns
	 * <code>null</code>.
	 * 
	 * @see #escapesValue()
	 * @see #quotesValue()
	 */
	String toString(Object value);
}
