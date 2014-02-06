package org.springframework.data.cassandra.test.integration.support;

import org.cassandraunit.dataset.commons.AbstractCommonsParserDataSet;
import org.cassandraunit.dataset.commons.ParsedKeyspace;

public class ManualDataSet extends AbstractCommonsParserDataSet {

	private String keyspaceName;
	private ParsedKeyspace pks;

	public ManualDataSet(String keyspaceName) {
		this.keyspaceName = keyspaceName;

		pks = new ParsedKeyspace();
		pks.setName(this.keyspaceName);
	}

	@Override
	protected ParsedKeyspace getParsedKeyspace() {
		return pks;
	}
}
