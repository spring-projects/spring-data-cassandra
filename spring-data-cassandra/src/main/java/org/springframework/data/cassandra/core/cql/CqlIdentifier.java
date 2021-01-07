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

import java.io.Serializable;
import java.util.regex.Pattern;

import org.springframework.util.Assert;

/**
 * This encapsulates the logic for CQL quoted and unquoted identifiers.
 * <p>
 * CQL identifiers, when unquoted, are converted to lower case. When quoted, they are returned as-is with no lower
 * casing and encased in double quotes. To render, use any of the methods {@link #toCql()},
 * {@link #toCql(StringBuilder)}, or {@link #toString()}.
 *
 * @author John McPeek
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author John Blum
 * @see #toCql()
 * @see #toString()
 * @deprecated since 3.0, use {@link com.datastax.oss.driver.api.core.CqlIdentifier} instead.
 */
@Deprecated
public final class CqlIdentifier implements Comparable<CqlIdentifier>, Serializable {

	private static final long serialVersionUID = -974441606330912437L;

	public static final String UNQUOTED_REGEX = "(?i)[a-z][\\w]*";

	public static final Pattern UNQUOTED = Pattern.compile(UNQUOTED_REGEX);

	public static final String QUOTED_REGEX = "(?i)[a-z]([\\w]*(\"\")+[\\w]*)+";

	public static final Pattern QUOTED = Pattern.compile(QUOTED_REGEX);

	private final String identifier;

	private final String unquoted;

	private final boolean quoted;

	/**
	 * Create a new {@link CqlIdentifier} without force-quoting it. It may end up quoted, depending on its value.
	 *
	 * @see #of(CharSequence)
	 */
	private CqlIdentifier(CharSequence identifier) {
		this(identifier, false);
	}

	/**
	 * Create a new CQL identifier, optionally force-quoting it. Force-quoting can be used to preserve identifier case.
	 * <ul>
	 * <li>If the given identifier is a legal quoted identifier or {@code forceQuote} is <code>true</code>,
	 * {@link #isQuoted()} will return {@code true} and the identifier will be quoted when rendered.</li>
	 * <li>If the given identifier is a legal unquoted identifier, {@link #isQuoted()} will return {@code false}, plus the
	 * name will be converted to lower case and rendered as such.</li>
	 * <li>If the given identifier is illegal, an {@link IllegalArgumentException} is thrown.</li>
	 * </ul>
	 *
	 * @see #of(CharSequence, boolean)
	 * @see #quoted(CharSequence)
	 */
	private CqlIdentifier(CharSequence identifier, boolean forceQuote) {

		Assert.notNull(identifier, "Identifier must not be null");

		String string = identifier.toString();

		Assert.hasText(string, "Identifier must not be empty");

		if (forceQuote || requiresQuoting(string)) {
			this.unquoted = string;
			this.identifier = "\"" + string + "\"";
			this.quoted = true;
		} else if (isUnquotedIdentifier(string)) {
			this.identifier = this.unquoted = string.toLowerCase();
			this.quoted = false;
		} else {
			throw new IllegalArgumentException(
					String.format("given string [%s] is not a valid quoted or unquoted identifier", identifier));
		}
	}

	/**
	 * Factory method for {@link CqlIdentifier}. Convenient if imported statically.
	 *
	 * @see #CqlIdentifier(CharSequence)
	 * @deprecated since 2.0, use {@link #of(CharSequence)}
	 */
	@Deprecated
	public static CqlIdentifier cqlId(CharSequence identifier) {
		return new CqlIdentifier(identifier);
	}

	/**
	 * Factory method for {@link CqlIdentifier}. Convenient if imported statically.
	 *
	 * @see #CqlIdentifier(CharSequence, boolean)
	 * @deprecated since 2.0, use {@link #of(CharSequence, boolean)}
	 */
	@Deprecated
	public static CqlIdentifier cqlId(CharSequence identifier, boolean forceQuote) {
		return new CqlIdentifier(identifier, forceQuote);
	}

	/**
	 * Factory method for {@link CqlIdentifier}.
	 *
	 * @since 2.0
	 */
	public static CqlIdentifier of(CharSequence identifier) {
		return new CqlIdentifier(identifier);
	}

	/**
	 * Factory method for {@link CqlIdentifier}.
	 *
	 * @since 2.0
	 */
	public static CqlIdentifier of(CharSequence identifier, boolean forceQuote) {
		return new CqlIdentifier(identifier, forceQuote);
	}

	/**
	 * Factory method for a force-quoted {@link CqlIdentifier}. Convenient if imported statically.
	 *
	 * @see #CqlIdentifier(CharSequence, boolean)
	 * @deprecated since 2.0, use {@link #quoted(CharSequence)}.
	 */
	@Deprecated
	public static CqlIdentifier quotedCqlId(CharSequence identifier) {
		return new CqlIdentifier(identifier, true);
	}

	/**
	 * Factory method for a force-quoted {@link CqlIdentifier}.
	 *
	 * @since 2.0.
	 */
	public static CqlIdentifier quoted(CharSequence identifier) {
		return new CqlIdentifier(identifier, true);
	}

	/**
	 * Returns {@code true} if the given {@link CharSequence} is a legal unquoted identifier.
	 */
	public static boolean isUnquotedIdentifier(CharSequence chars) {
		return UNQUOTED.matcher(chars).matches() && !ReservedKeyword.isReserved(chars);
	}

	/**
	 * Returns {@code true} if the given {@link CharSequence} is an identifier with quotes.
	 */
	public static boolean isQuotedIdentifier(CharSequence chars) {
		return chars != null && chars.length() > 1 && chars.charAt(0) == '"' && chars.charAt(chars.length() - 1) == '"';
	}

	/**
	 * Returns {@code true} if the given {@link CharSequence} requires quoting.
	 *
	 * @since 2.2
	 */
	public static boolean requiresQuoting(CharSequence chars) {
		return QUOTED.matcher(chars).matches() || ReservedKeyword.isReserved(chars);
	}

	/**
	 * Returns the identifier <em>without</em> encasing quotes, regardless of the value of {@link #isQuoted()}. For
	 * example, if {@link #isQuoted()} is {@code true}, then this value will be the same as {@link #toCql()} and
	 * {@link #toString()}.
	 * <p/>
	 * This is needed, for example, to get the correct {@link TableMetadata} from
	 * {@link KeyspaceMetadata#getTable(String)}: the given string must <em>not</em> be quoted.
	 */
	public String getUnquoted() {
		return unquoted;
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
	public StringBuilder toCql(StringBuilder builder) {
		return builder.append(toCql());
	}

	/**
	 * Whether or not this identifier is quoted.
	 */
	public boolean isQuoted() {
		return quoted;
	}

	/**
	 * Unquoted identifiers sort before quoted ones. Otherwise, they compare according to their identifiers.
	 */
	@Override
	@SuppressWarnings("all")
	public int compareTo(CqlIdentifier identifier) {

		int comparison = ((Boolean) this.quoted).compareTo(identifier.quoted);

		return (comparison != 0 ? comparison : this.identifier.compareTo(identifier.identifier));
	}

	/**
	 * Compares this {@link CqlIdentifier} to the given object. Note that if a {@link CharSequence} is given, a new
	 * {@link CqlIdentifier} is created from it and compared, such that a {@link CharSequence} can be effectively equal to
	 * a {@link CqlIdentifier}.
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (!(o instanceof CqlIdentifier))
			return false;

		CqlIdentifier that = (CqlIdentifier) o;

		if (quoted != that.quoted)
			return false;
		return identifier.equals(that.identifier);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		int result = identifier.hashCode();
		result = 31 * result + (quoted ? 1 : 0);
		return result;
	}

	/**
	 * Alias for {@link #toCql()}.
	 */
	@Override
	public String toString() {
		return toCql();
	}

	/**
	 * Create a Cassandra driver {@link com.datastax.oss.driver.api.core.CqlIdentifier} from this {@link CqlIdentifier}.
	 *
	 * @return the {@link com.datastax.oss.driver.api.core.CqlIdentifier} from this {@link CqlIdentifier}.
	 * @since 3.0
	 */
	public com.datastax.oss.driver.api.core.CqlIdentifier toCqlIdentifier() {
		return com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal(this.unquoted);
	}
}
