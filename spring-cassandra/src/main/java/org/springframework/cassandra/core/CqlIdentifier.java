package org.springframework.cassandra.core;

public class CqlIdentifier {
	private String identifier;
	private boolean quoted;

	public CqlIdentifier(String identifier) {
		this(identifier, false);
	}

	public CqlIdentifier(String identifier, boolean forceQuoting) {
	}

	public String toCql() {
		return identifier;
	}

	public StringBuilder toCql(StringBuilder sb) {
		return sb.append(toCql());
	}

	@Override
	public String toString() {
		return toCql();
	}

	public String getIdentifier() {
		return toCql();
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
}
