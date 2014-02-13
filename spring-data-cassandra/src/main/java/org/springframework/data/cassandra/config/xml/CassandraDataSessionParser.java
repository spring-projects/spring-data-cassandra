package org.springframework.data.cassandra.config.xml;

import static org.springframework.cassandra.config.xml.ParsingUtils.addOptionalPropertyReference;
import static org.springframework.cassandra.config.xml.ParsingUtils.addOptionalPropertyValue;
import static org.springframework.cassandra.config.xml.ParsingUtils.addRequiredPropertyReference;
import static org.springframework.cassandra.config.xml.ParsingUtils.addRequiredPropertyValue;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.xml.CassandraSessionParser;
import org.springframework.data.cassandra.config.CassandraDataSessionFactoryBean;
import org.springframework.data.cassandra.config.DefaultDataBeanNames;
import org.springframework.data.cassandra.config.SchemaAction;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the &lt;session&gt; element.
 * 
 * @author Matthew T. Adams
 */
public class CassandraDataSessionParser extends CassandraSessionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraDataSessionFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		super.doParse(element, parserContext, builder);

		CassandraMappingXmlBeanFactoryPostProcessorRegistrar.ensureRegistration(element, parserContext);
	}

	@Override
	protected void parseUnhandledSessionElementAttribute(Attr attribute, ParserContext parserContext,
			BeanDefinitionBuilder builder) {

		String name = attribute.getName();

		if ("cassandra-converter-ref".equals(name)) {
			addOptionalPropertyReference(builder, "converter", attribute, DefaultDataBeanNames.CONVERTER);
		} else if ("schema-action".equals(name)) {
			addOptionalPropertyValue(builder, "schemaAction", attribute, SchemaAction.NONE.name());
		} else {
			super.parseUnhandledSessionElementAttribute(attribute, parserContext, builder);
		}
	}

	@Override
	protected void setDefaultProperties(BeanDefinitionBuilder builder) {

		super.setDefaultProperties(builder);

		addRequiredPropertyValue(builder, "schemaAction", SchemaAction.NONE.name());
		addRequiredPropertyReference(builder, "converter", DefaultDataBeanNames.CONVERTER);
	}
}
