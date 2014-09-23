package org.springframework.cassandra.core;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * CQL keywords.
 * 
 * @see <a
 *      href="http://cassandra.apache.org/doc/cql3/CQL.html#appendixA">http://cassandra.apache.org/doc/cql3/CQL.html#appendixA</a>
 * @author Matthew T. Adams
 */
public enum ReservedKeyword {
	ADD,
	ALTER,
	AND,
	ANY,
	APPLY,
	ASC,
	AUTHORIZE,
	BATCH,
	BEGIN,
	BY,
	COLUMNFAMILY,
	CREATE,
	DELETE,
	DESC,
	DROP,
	EACH_QUORUM,
	FROM,
	GRANT,
	IN,
	INDEX,
	INSERT,
	INTO,
	KEYSPACE,
	LIMIT,
	LOCAL_ONE,
	LOCAL_QUORUM,
	MODIFY,
	NORECURSIVE,
	OF,
	ON,
	ONE,
	ORDER,
	PRIMARY,
	QUORUM,
	REVOKE,
	SCHEMA,
	SELECT,
	SET,
	TABLE,
	THREE,
	TOKEN,
	TRUNCATE,
	TWO,
	UPDATE,
	USE,
	USING,
	WHERE,
	WITH;

	/**
	 * @see ReservedKeyword#isReserved(String)
	 */
	public static boolean isReserved(CharSequence candidate) {
		Assert.notNull(candidate);
		return isReserved(candidate.toString());
	}

	/**
	 * Returns whether the given string is a CQL reserved keyword. This comparison is done regardless of case.
	 */
	public static boolean isReserved(String candidate) {

		if (!StringUtils.hasText(candidate)) {
			return false;
		}

		try {
			Enum.valueOf(ReservedKeyword.class, candidate.toUpperCase());
			return true;
		} catch (IllegalArgumentException x) {
			return false;
		}
	}
}
