package org.springframework.cassandra.core.cql;

import java.util.regex.Pattern;

import org.springframework.cassandra.core.ReservedKeyword;
import org.springframework.util.Assert;

/**
 * This encapsulates the logic for keyspace identifiers.
 * <p/>
 * Keyspace identifiers are converted to lower case. To render, use any of the methods {@link #toCql()},
 * {@link #toCql(StringBuilder)}, or {@link #toString()}.
 * 
 * @see #KeyspaceIdentifier(String)
 * @see #toCql()
 * @see #toCql(StringBuilder)
 * @see #toString()
 * @author Matthew T. Adams
 */
public final class KeyspaceIdentifier implements Comparable<KeyspaceIdentifier> {

	public static final String REGEX = "(?i)[a-z][\\w]{0,47}";
	public static final Pattern PATTERN = Pattern.compile(REGEX);

	/**
	 * Factory method for {@link KeyspaceIdentifier}. Convenient if imported statically.
	 */
	public static KeyspaceIdentifier ksId(CharSequence identifier) {
		return new KeyspaceIdentifier(identifier);
	}

	/**
	 * Returns <code>true</code> if the given {@link CharSequence} is a legal keyspace identifier.
	 */
	public static boolean isIdentifier(CharSequence chars) {
		return PATTERN.matcher(chars).matches() && !ReservedKeyword.isReserved(chars);
	}

	private String identifier;

	/**
	 * Creates a new {@link KeyspaceIdentifier}.
	 */
	public KeyspaceIdentifier(CharSequence identifier) {
		setIdentifier(identifier);
	}

	/**
	 * Tests & sets the given identifier.
	 */
	private void setIdentifier(CharSequence identifier) {

		Assert.notNull(identifier);

		String string = identifier.toString();
		Assert.hasText(string);

		if (!isIdentifier(string)) {
			throw new IllegalArgumentException(String.format("given string [%s] is not a valid keyspace identifier",
					identifier));
		}
		this.identifier = string.toLowerCase();
	}

	/**
	 * Renders this identifier appropriately.
	 */
	public String toCql() {
		return identifier;
	}

	/**
	 * Appends the rendering of this identifier to the given {@link StringBuilder}, then returns that
	 * {@link StringBuilder}. If <code>null</code> is given, a new {@link StringBuilder} is created, appended to, and
	 * returned.
	 */
	public StringBuilder toCql(StringBuilder sb) {
		sb = sb == null ? new StringBuilder() : sb;
		return sb.append(toCql());
	}

	/**
	 * Alias for {@link #toCql()}.
	 */
	@Override
	public String toString() {
		return toCql();
	}

	@Override
	public int hashCode() {
		return identifier.hashCode();
	}

	/**
	 * Compares this {@link KeyspaceIdentifier} to the given object. Note that if a {@link CharSequence} is given, a new
	 * {@link KeyspaceIdentifier} is created from it and compared, such that a {@link CharSequence} can be effectively
	 * equal to a {@link KeyspaceIdentifier}.
	 */
	@Override
	public boolean equals(Object that) {
		if (this == that) {
			return true;
		}
		if (that == null) {
			return false;
		}
		if (!(that instanceof KeyspaceIdentifier) && !(that instanceof CharSequence)) {
			return false;
		}

		KeyspaceIdentifier other = (that instanceof KeyspaceIdentifier) ? (KeyspaceIdentifier) that
				: ksId((CharSequence) that);

		return this.identifier.equals(other.identifier);
	}

	@Override
	public int compareTo(KeyspaceIdentifier that) {
		return this.identifier.compareTo(that.identifier);
	}
}
