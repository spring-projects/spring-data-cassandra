package org.springframework.data.cassandra.config.xml;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.xml.CassandraClusterParser;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the &lt;cluster&gt; element.
 * 
 * @author Matthew T. Adams
 */
public class CassandraDataClusterParser extends CassandraClusterParser {

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {

		CassandraMappingXmlBeanFactoryPostProcessorRegistrar.ensureRegistration(element, parserContext);

		return super.parseInternal(element, parserContext);
	}
}
