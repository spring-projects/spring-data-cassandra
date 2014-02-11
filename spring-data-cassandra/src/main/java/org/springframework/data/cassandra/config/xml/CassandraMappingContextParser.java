package org.springframework.data.cassandra.config.xml;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.cassandra.config.CassandraMappingContextFactoryBean;
import org.springframework.data.cassandra.config.DefaultDataBeanNames;
import org.springframework.data.cassandra.mapping.EntityMapping;
import org.springframework.data.cassandra.mapping.Mapping;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Spring Data Cassandra XML namespace parser for the &lt;mapping&gt; element.
 * 
 * @author Matthew T. Adams
 */
public class CassandraMappingContextParser extends AbstractSimpleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CassandraMappingContextFactoryBean.class;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : DefaultDataBeanNames.MAPPING_CONTEXT;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, parserContext, builder);

		parseBasePackagesAttribute(element, parserContext, builder);
		parseMapping(element, parserContext, builder);
	}

	protected void parseBasePackagesAttribute(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		String basePackages = element.getAttribute("entity-base-packages");
		if (!StringUtils.hasText(basePackages)) {
			return;
		}

		Set<String> basePackageSet = StringUtils.commaDelimitedListToSet(basePackages);
		builder.addPropertyValue("entityBasePackages", basePackageSet);
	}

	protected void parseMapping(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		// TODO: parse <mapping> **attributes** here, if there ever are any

		Set<EntityMapping> mappings = new HashSet<EntityMapping>();

		for (Element child : DomUtils.getChildElementsByTagName(element, "entity")) {

			EntityMapping entityMapping = parseEntity(child);

			if (entityMapping != null) {
				mappings.add(entityMapping);
			}
		}

		Mapping mapping = new Mapping();
		mapping.setEntityMappings(mappings);

		builder.addPropertyValue("mapping", mapping);
	}

	protected EntityMapping parseEntity(Element entity) {

		String className = entity.getAttribute("class");
		if (!StringUtils.hasText(className)) {
			throw new IllegalStateException("class attribute must not be empty");
		}

		Element table = DomUtils.getChildElementByTagName(entity, "table");
		if (table == null) {
			return null;
		}

		String tableName = table.getAttribute("name");
		if (!StringUtils.hasText(tableName)) {
			tableName = null;
		}

		// TODO: parse future entity mappings here, like table options

		return new EntityMapping(className, tableName);
	}
}
