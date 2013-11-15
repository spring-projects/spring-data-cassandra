package org.springframework.data.cassandra.cql;

import java.util.regex.Pattern;

public class CqlStringUtils {

	protected static final String SINGLE_QUOTE = "\'";
	protected static final String DOUBLE_SINGLE_QUOTE = "\'\'";
	protected static final String DOUBLE_QUOTE = "\"";
	protected static final String DOUBLE_DOUBLE_QUOTE = "\"\"";

	/**
	 * Helper {@link StringBuilder} factory method. If given a non-<code>null</code> argument, returns that, else returns
	 * a new {@link StringBuilder}. Intended to be imported statically by other classes in the builder's fluent API
	 * implementation.
	 * 
	 * @param sb
	 * @return The given {@link StringBuilder} if not null, else a new one.
	 * 
	 * @author Matthew T. Adams
	 */
	public static StringBuilder ensureNotNull(StringBuilder sb) {
		return sb == null ? new StringBuilder() : sb;
	}

	public static final String IDENTIFIER_REGEX = "[a-zA-Z0-9_]*";
	public static final Pattern IDENTIFIER_PATTERN = Pattern.compile(IDENTIFIER_REGEX);

	public static boolean isIdentifier(CharSequence chars) {
		return IDENTIFIER_PATTERN.matcher(chars).matches();
	}

	public static final String QUOTED_IDENTIFIER_REGEX = "([a-zA-Z0-9_]|'{2}+|\"{2}+)*";
	public static final Pattern QUOTED_IDENTIFIER_PATTERN = Pattern.compile(IDENTIFIER_REGEX);

	public static boolean isQuotedIdentifier(CharSequence chars) {
		return QUOTED_IDENTIFIER_PATTERN.matcher(chars).matches();
	}

	public static void checkQuotedIdentifier(CharSequence chars) {
		if (!CqlStringUtils.isQuotedIdentifier(chars)) {
			throw new IllegalArgumentException("[" + chars + "] is not a valid CQL quoted identifier");
		}
	}

	/**
	 * Trims then escapes the given {@link CharSequence}. Given <code>null</code>, returns <code>null</code>.
	 */
	public static String scrub(Object thing) {
		return thing == null ? (String) null : escape(thing.toString().trim());
	}

	/**
	 * Doubles single quote characters and doubles double quote characters (' -&gt; '' and " -&gt; ""). Given
	 * <code>null</code>, returns <code>null</code>.
	 */
	public static String escape(Object thing) {
		return escapeDouble(escapeSingle(thing));
	}

	/**
	 * Doubles single quote characters (' -&gt; ''). Given <code>null</code>, returns <code>null</code>.
	 */
	public static String escapeSingle(Object things) {
		return things == null ? (String) null : things.toString().replace(SINGLE_QUOTE, DOUBLE_SINGLE_QUOTE);
	}

	/**
	 * Doubles double quote characters (" -&gt; ""). Given <code>null</code>, returns <code>null</code>.
	 */
	public static String escapeDouble(Object things) {
		return things == null ? (String) null : things.toString().replace(DOUBLE_QUOTE, DOUBLE_SINGLE_QUOTE);
	}

	/**
	 * Surrounds given object's {@link Object#toString()} with single quotes. Given <code>null</code>, returns
	 * <code>null</code>.
	 */
	public static String singleQuote(Object thing) {
		return thing == null ? (String) null : new StringBuilder().append(SINGLE_QUOTE).append(thing).append(SINGLE_QUOTE)
				.toString();
	}

	/**
	 * Surrounds given object's {@link Object#toString()} with double quotes. Given <code>null</code>, returns
	 * <code>null</code>.
	 */
	public static String doubleQuote(Object thing) {
		return thing == null ? (String) null : new StringBuilder().append(DOUBLE_QUOTE).append(thing).append(DOUBLE_QUOTE)
				.toString();
	}
}
