package org.springframework.data.cassandra.config;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.util.StringUtils;

public class BeanDefinitionUtils {

	/**
	 * Returns a {@link BeanDefinitionBuilder} iff no {@link BeanDefinition} of the required type is found in the given
	 * {@link ListableBeanFactory}, otherwise returns <code>null</code>, indicating that at least one existed.
	 * 
	 * @param factory The {@link ListableBeanFactory} in which to look for the {@link BeanDefinition}, including
	 *          ancestors.
	 * @param requiredType The {@link BeanDefinition}'s required type.
	 * @param instantiableType The instantiable type for the {@link BeanDefinitionBuilder}.
	 * @param constructorArgs Any {@link BeanDefinitionBuilderArgument}s required by the instantiableType's constructor.
	 * @return A {@link BeanDefinitionBuilder} iff no {@link BeanDefinition} of the required type is found, otherwise
	 *         <code>null</code>.
	 * @see BeanDefinitionUtils#createBeanDefinitionBuilderIfNoBeanDefinitionOfTypeExists(ListableBeanFactory, Class,
	 *      Class, BeanDefinitionBuilderArgument...)
	 * @see BeanDefinitionBuilderArgument#ref(Object)
	 * @see BeanDefinitionBuilderArgument#val(Object)
	 */
	public static BeanDefinitionBuilder createBeanDefinitionBuilderIfNoBeanDefinitionOfTypeExists(
			ListableBeanFactory factory, Class<?> requiredType, Class<?> instantiableType,
			BeanDefinitionBuilderArgument... constructorArgs) {

		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(factory, requiredType, true, false);
		if (names.length > 0) {
			return null;
		}

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(instantiableType);
		if (constructorArgs == null) {
			return builder;
		}

		for (BeanDefinitionBuilderArgument arg : constructorArgs) {
			if (arg.reference) {
				builder.addConstructorArgReference(arg.value.toString());
			} else {
				builder.addConstructorArgValue(arg.value);
			}
		}

		return builder;
	}

	/**
	 * Returns the single {@link BeanDefinitionHolder} with the given type, or null of none were found and
	 * <code>required</code> was <code>false</code>, otherwise throws {@link IllegalArgumentException}.
	 * 
	 * @param registry The {@link BeanDefinitionRegistry}, often the very same instance as the <code>factor</code>
	 *          parameter.
	 * @param factory The {@link ListableBeanFactory}, often the very same instance as the <code>registry</code>
	 *          parameter.
	 * @param type The required {@link BeanDefinition}'s type.
	 * @param includeNonSingletons Whether to include beans with scope other than <code>singleton</code>
	 * @param allowEagerInit Whether to allow eager initialization of beans.
	 * @param required Whether to allow the return of null if none were found.
	 * @return The {@link BeanDefinitionHolder} or null if none found, depending on the value of <code>required</code>.
	 * @throws IllegalArgumentException If multiple were found.
	 * @see BeanFactoryUtils#beanNamesForTypeIncludingAncestors(ListableBeanFactory, Class, boolean, boolean)
	 */
	public static BeanDefinitionHolder getSingleBeanDefinitionOfType(BeanDefinitionRegistry registry,
			ListableBeanFactory factory, Class<?> type, boolean includeNonSingletons, boolean allowEagerInit, boolean required) {

		BeanDefinitionHolder[] definitions = getBeanDefinitionsOfType(registry, factory, type, includeNonSingletons,
				allowEagerInit);

		if (definitions.length == 1) {
			return definitions[0];
		}

		if (definitions.length == 0 && !required) {
			return null;
		}

		String[] names = new String[definitions.length];
		for (int i = 0; i < names.length; i++) {
			names[i] = definitions[i].getBeanName();
		}

		throw new IllegalStateException(String.format("expected one bean definition of type [%s], but found %d: %s",
				type.getName(), definitions.length, StringUtils.arrayToCommaDelimitedString(names)));
	}

	/**
	 * Returns all {@link BeanDefinitionHolder}s with the given type.
	 * 
	 * @param registry The {@link BeanDefinitionRegistry}, often the very same instance as the <code>factor</code>
	 *          parameter.
	 * @param factory The {@link ListableBeanFactory}, often the very same instance as the <code>registry</code>
	 *          parameter.
	 * @param type The required {@link BeanDefinition}'s type.
	 * @param includeNonSingletons Whether to include beans with scope other than <code>singleton</code>
	 * @param allowEagerInit Whether to allow eager initialization of beans.
	 * @param required Whether to allow the return of null if none were found.
	 * @return The {@link BeanDefinitionHolder}s -- never returns null.
	 * @see BeanFactoryUtils#beanNamesForTypeIncludingAncestors(ListableBeanFactory, Class, boolean, boolean)
	 */
	public static BeanDefinitionHolder[] getBeanDefinitionsOfType(BeanDefinitionRegistry registry,
			ListableBeanFactory factory, Class<?> type, boolean includeNonSingletons, boolean allowEagerInit) {

		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(factory, type, includeNonSingletons,
				allowEagerInit);

		if (names.length == 0) {
			return new BeanDefinitionHolder[] {};
		}

		BeanDefinitionHolder[] array = new BeanDefinitionHolder[names.length];

		for (int i = 0; i < names.length; i++) {

			String name = names[i];
			BeanDefinition beanDefinition = null;

			while (beanDefinition == null) {
				try {
					beanDefinition = registry.getBeanDefinition(name);
				} catch (NoSuchBeanDefinitionException x) {
					if (FactoryBean.class.isAssignableFrom(type)) { // try unmangled BeanFactory-prefixed name
						name = name.substring(BeanFactory.FACTORY_BEAN_PREFIX.length());
					} else {
						throw x;
					}
				}
			}

			array[i] = new BeanDefinitionHolder(beanDefinition, name);
		}

		return array;
	}
}
