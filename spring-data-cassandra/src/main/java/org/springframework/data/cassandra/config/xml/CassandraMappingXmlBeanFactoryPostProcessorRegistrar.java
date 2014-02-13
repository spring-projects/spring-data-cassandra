package org.springframework.data.cassandra.config.xml;

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.cassandra.config.CassandraMappingBeanFactoryPostProcessor;
import org.springframework.data.config.BeanComponentDefinitionBuilder;
import org.w3c.dom.Element;

/**
 * Ensures that a {@link CassandraMappingBeanFactoryPostProcessor} is registered.
 * 
 * @author Matthew T. Adams
 */
public class CassandraMappingXmlBeanFactoryPostProcessorRegistrar {

	/**
	 * Ensures that a {@link CassandraMappingBeanFactoryPostProcessor} is registered. This method is a no-op if one is
	 * already registered.
	 */
	public static void ensureRegistration(Element element, ParserContext parserContext) {

		BeanDefinitionRegistry registry = parserContext.getRegistry();
		if (!(registry instanceof GenericApplicationContext)) {
			return;
		}
		ConfigurableListableBeanFactory factory = ((GenericApplicationContext) registry).getBeanFactory();

		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(factory,
				CassandraMappingBeanFactoryPostProcessor.class, true, false);
		if (names.length > 0) {
			return;
		}

		BeanComponentDefinitionBuilder componentBuilder = new BeanComponentDefinitionBuilder(element, parserContext);
		BeanDefinitionBuilder definitionBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(CassandraMappingBeanFactoryPostProcessor.class);

		parserContext.registerBeanComponent(componentBuilder.getComponent(definitionBuilder));
	}
}
