package org.springframework.cassandra.config.java;

import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.context.annotation.Bean;

public abstract class AbstractCqlTemplateConfiguration extends AbstractSessionConfiguration {

	@Bean
	public CqlTemplate cqlTemplate() throws Exception {
		return new CqlTemplate(session().getObject());
	}
}
