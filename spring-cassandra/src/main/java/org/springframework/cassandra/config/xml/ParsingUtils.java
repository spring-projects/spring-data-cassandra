package org.springframework.cassandra.config.xml;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

public class ParsingUtils {

	/**
	 * Configures a property value for the given property name reading the attribute of the given name from the given
	 * {@link Element} if the attribute is configured.
	 * 
	 * @param builder must not be {@literal null}.
	 * @param element must not be {@literal null}.
	 * @param attrName must not be {@literal null} or empty.
	 * @param propertyName must not be {@literal null} or empty.
	 */
	public static void setPropertyValue(BeanDefinitionBuilder builder, Element element, String attrName,
			String propertyName) {

		Assert.notNull(builder, "BeanDefinitionBuilder must not be null!");
		Assert.notNull(element, "Element must not be null!");
		Assert.hasText(attrName, "Attribute name must not be null!");
		Assert.hasText(propertyName, "Property name must not be null!");

		String attr = element.getAttribute(attrName);

		if (StringUtils.hasText(attr)) {
			builder.addPropertyValue(propertyName, attr);
		}
	}
}
