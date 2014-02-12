package org.springframework.cassandra.core;

import java.util.regex.Pattern;

import org.springframework.cassandra.core.cql.CqlStringUtils;

/**
 * This encapsulates the logic for CQL identifiers.
 * 
 * @author John McPeek
 * 
 */
public class CqlIdentifier {
	public static final String UNQUOTED_IDENTIFIER_REGEX = "[a-zA-Z_][a-zA-Z0-9_]*";
	public static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile(UNQUOTED_IDENTIFIER_REGEX);
	public static final String QUOTED_IDENTIFIER_REGEX = "[a-zA-Z_]([a-zA-Z0-9_]|\"{2}+)*";
	public static final Pattern QUOTED_IDENTIFIER_PATTERN = Pattern.compile(QUOTED_IDENTIFIER_REGEX);

	private String name;
	private boolean quoted;

	public CqlIdentifier(String identifier) {
		this(identifier, false);
	}

	/**
	 * Renders the given string as a legal Cassandra identifier.
	 * <ul>
	 * <li>If the given identifier is a legal quoted identifier or forceQuote is true, it is set encased in double quotes.
	 * </li>
	 * <li>If the given identifier is a legal unquoted identifier, it is set unchanged.</li>
	 * <li>If the given identifier is illegal, an {@link IllegalArgumentException} is thrown.</li>
	 * </ul>
	 */
	public CqlIdentifier(String name, boolean forceQuoting) {
		if (isUnquotedIdentifier(name) && forceQuoting == false) {
			this.name = name;
		} else if (isQuotedIdentifier(name)) {
			this.name = name;
			quoted = true;
		} else {
			throw new IllegalArgumentException("[" + name + "] is not a valid CQL quoted or unquoted identifier");
		}
	}

	public String toCql() {
		String id = quoted ? CqlStringUtils.doubleQuote(name) : name;
		return id;
	}

	public StringBuilder toCql(StringBuilder sb) {
		return sb.append(toCql());
	}

	@Override
	public String toString() {
		return toCql();
	}

	public String getName() {
		return name;
	}

	public boolean isQuoted() {
		return quoted;
	}

	public static CqlIdentifier cqlId(String identifier) {
		CqlIdentifier id = new CqlIdentifier(identifier);
		return id;
	}

	public static CqlIdentifier quotedCqlId(String identifier) {
		CqlIdentifier id = new CqlIdentifier(identifier, true);
		return id;
	}

	public static boolean isIdentifier(CharSequence chars) {
		return isUnquotedIdentifier(chars) || isQuotedIdentifier(chars);
	}

	public static boolean isUnquotedIdentifier(CharSequence chars) {
		return UNQUOTED_IDENTIFIER_PATTERN.matcher(chars).matches();
	}

	public static boolean isQuotedIdentifier(CharSequence chars) {
		return QUOTED_IDENTIFIER_PATTERN.matcher(chars).matches();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (quoted ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CqlIdentifier other = (CqlIdentifier) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (quoted != other.quoted)
			return false;
		return true;
	}
}
