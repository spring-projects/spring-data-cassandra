package org.springframework.data.cassandra.config.xml;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.xml.CassandraTemplateParser;
import org.springframework.data.cassandra.config.DefaultDataBeanNames;
import org.springframework.data.cassandra.config.CassandraDataTemplateFactoryBean;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the &lt;template&gt; element.
 * 
 * @author Matthew T. Adams
 */
public class CassandraDataTemplateParser extends CassandraTemplateParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraDataTemplateFactoryBean.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : DefaultDataBeanNames.DATA_TEMPLATE;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		CassandraMappingXmlBeanFactoryPostProcessorRegistrar.ensureRegistration(element, parserContext);

		super.doParse(element, parserContext, builder);

		parseConverterAttribute(element, parserContext, builder);
	}

	protected void parseConverterAttribute(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		String converterRef = element.getAttribute("cassandra-converter-ref");
		if (!StringUtils.hasText(converterRef)) {
			converterRef = DefaultDataBeanNames.CONVERTER;
		}
		builder.addPropertyReference("converter", converterRef);
	}
}
