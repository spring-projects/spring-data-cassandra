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
package org.springframework.data.cassandra.core.cql;

import java.util.regex.Pattern;

import com.datastax.oss.driver.api.core.CqlIdentifier;

import org.springframework.util.Assert;

/**
 * This encapsulates the logic for keyspace identifiers.
 * <p/>
 * Keyspace identifiers are converted to lower case. To render, use any of the methods {@link #toCql()},
 * {@link #toCql(StringBuilder)}, or {@link #toString()}.
 *
 * @see #toCql()
 * @see #toString()
 * @author Matthew T. Adams
 * @deprecated since 3.0, use {@link com.datastax.oss.driver.api.core.CqlIdentifier}.
 */
@Deprecated
public final class KeyspaceIdentifier implements Comparable<KeyspaceIdentifier> {

	public static final String REGEX = "(?i)[a-z][\\w]{0,47}";
	public static final Pattern PATTERN = Pattern.compile(REGEX);

	private final String identifier;

	/**
	 * Create a new {@link KeyspaceIdentifier}.
	 */
	private KeyspaceIdentifier(CharSequence identifier) {

		Assert.notNull(identifier, "Identifier must not be null");

		String string = identifier.toString();
		Assert.hasText(string, "Identifier must not be empty");

		if (!isIdentifier(string)) {
			throw new IllegalArgumentException(
					String.format("given string [%s] is not a valid keyspace identifier", identifier));
		}
		this.identifier = string.toLowerCase();
	}

	/**
	 * Factory method for {@link KeyspaceIdentifier}. Convenient if imported statically.
	 *
	 * @deprecated since 2.0, use {@link #of(CharSequence)}.
	 */
	@Deprecated
	public static KeyspaceIdentifier ksId(CharSequence identifier) {
		return new KeyspaceIdentifier(identifier);
	}

	/**
	 * Factory method for {@link KeyspaceIdentifier}. Convenient if imported statically.
	 *
	 * @since 2.0
	 */
	public static KeyspaceIdentifier of(CharSequence identifier) {
		return new KeyspaceIdentifier(identifier);
	}

	/**
	 * Returns {@code true} if the given {@link CharSequence} is a legal keyspace identifier.
	 */
	public static boolean isIdentifier(CharSequence chars) {
		return PATTERN.matcher(chars).matches() && !ReservedKeyword.isReserved(chars);
	}

	/**
	 * Renders this identifier appropriately.
	 */
	public String toCql() {
		return identifier;
	}

	/**
	 * Appends the rendering of this identifier to the given {@link StringBuilder}, then returns that
	 * {@link StringBuilder}. If {@literal null} is given, a new {@link StringBuilder} is created, appended to, and
	 * returned.
	 */
	public StringBuilder toCql(StringBuilder sb) {
		return sb.append(toCql());
	}

	/**
	 * Alias for {@link #toCql()}.
	 */
	@Override
	public String toString() {
		return toCql();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
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
				: of((CharSequence) that);

		return this.identifier.equals(other.identifier);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(KeyspaceIdentifier that) {
		return this.identifier.compareTo(that.identifier);
	}

	/**
	 * Create a {@link CqlIdentifier} from this {@link KeyspaceIdentifier}.
	 *
	 * @return the {@link CqlIdentifier} from this {@link KeyspaceIdentifier}.
	 * @since 3.0
	 */
	public CqlIdentifier toCqlIdentifier() {
		return CqlIdentifier.fromCql(this.identifier);
	}
}
