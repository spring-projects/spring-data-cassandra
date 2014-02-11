package org.springframework.data.cassandra.config.xml;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.cassandra.config.CassandraMappingConverterFactoryBean;
import org.springframework.data.cassandra.config.DefaultDataBeanNames;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the &lt;converter&gt; element.
 * 
 * @author Matthew T. Adams
 */
public class CassandraMappingConverterParser extends AbstractSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraMappingConverterFactoryBean.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : DefaultDataBeanNames.CONVERTER;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		super.doParse(element, parserContext, builder);

		parseMappingContextAttribute(element, parserContext, builder);
	}

	protected void parseMappingContextAttribute(Element element, ParserContext parserContext,
			BeanDefinitionBuilder builder) {

		String mappingContextRef = element.getAttribute("mapping-ref");
		if (!StringUtils.hasText(mappingContextRef)) {
			mappingContextRef = DefaultDataBeanNames.MAPPING_CONTEXT;
		}

		builder.addPropertyReference("mappingContext", mappingContextRef);
	}
}
