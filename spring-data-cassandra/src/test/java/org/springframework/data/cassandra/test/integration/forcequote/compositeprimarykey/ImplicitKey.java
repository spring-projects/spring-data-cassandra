package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import java.io.Serializable;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

@PrimaryKeyClass
public class ImplicitKey implements Serializable {

	private static final long serialVersionUID = 4459456944472099332L;

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED, forceQuote = true)
	String keyZero;

	@PrimaryKeyColumn(ordinal = 1, forceQuote = true)
	String keyOne;

	public ImplicitKey(String keyZero, String keyOne) {
		this.keyZero = keyZero;
		this.keyOne = keyOne;
	}

	public String getKeyZero() {
		return keyZero;
	}

	public String getKeyOne() {
		return keyOne;
	}
}
