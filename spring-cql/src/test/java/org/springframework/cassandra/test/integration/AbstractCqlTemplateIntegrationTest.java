package org.springframework.cassandra.test.integration;

import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.CqlTemplate;

public class AbstractCqlTemplateIntegrationTest extends AbstractKeyspaceCreatingIntegrationTest {

	protected CqlOperations t;

	{
		t = new CqlTemplate(SESSION);
	}

	public AbstractCqlTemplateIntegrationTest() {}

	public AbstractCqlTemplateIntegrationTest(String keyspace) {
		super(keyspace);
	}

}
