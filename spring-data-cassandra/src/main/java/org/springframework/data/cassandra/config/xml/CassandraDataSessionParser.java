package org.springframework.data.cassandra.config.xml;

import static org.springframework.cassandra.config.xml.ParsingUtils.*;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.cassandra.config.xml.CassandraSessionParser;
import org.springframework.data.cassandra.config.DefaultDataBeanNames;
import org.springframework.data.cassandra.config.CassandraDataSessionFactoryBean;
import org.springframework.data.cassandra.config.EntityMapping;
import org.springframework.data.cassandra.config.Mapping;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
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
	protected void parseUnhandledElement(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		if ("mapping".equals(element.getLocalName())) {
			parseMapping(element, parserContext, builder);
		} else {
			super.parseUnhandledElement(element, parserContext, builder);
		}
	}

	protected void parseMapping(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		// TODO: parse <mapping> attributes here, if there ever are any

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
