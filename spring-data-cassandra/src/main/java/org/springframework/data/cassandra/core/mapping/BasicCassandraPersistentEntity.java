/*
 * Copyright 2013-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.lang.annotation.Annotation;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Cassandra specific {@link BasicPersistentEntity} implementation that adds Cassandra specific metadata.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 */
public class BasicCassandraPersistentEntity<T> extends BasicPersistentEntity<T, CassandraPersistentProperty>
		implements CassandraPersistentEntity<T>, ApplicationContextAware {

	static final ValueExpressionParser PARSER = ValueExpressionParser.create(SpelExpressionParser::new);

	private static final CassandraPersistentEntityMetadataVerifier DEFAULT_VERIFIER = new CompositeCassandraPersistentEntityMetadataVerifier();

	private final CqlIdentifierGenerator namingAccessor = new CqlIdentifierGenerator();

	private @Nullable ApplicationContext applicationContext;

	private CassandraPersistentEntityMetadataVerifier verifier = DEFAULT_VERIFIER;

	private @Nullable CqlIdentifier keyspace;

	private @Nullable CqlIdentifier tableName;

	private final Map<Parameter<?, CassandraPersistentProperty>, CassandraPersistentProperty> constructorProperties = new ConcurrentHashMap<>();

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} given {@link TypeInformation}.
	 *
	 * @param typeInformation must not be {@literal null}.
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation) {
		this(typeInformation, DEFAULT_VERIFIER);
	}

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 */
	public BasicCassandraPersistentEntity(TypeInformation<T> typeInformation,
			CassandraPersistentEntityMetadataVerifier verifier) {

		super(typeInformation, CassandraPersistentPropertyComparator.INSTANCE);

		setVerifier(verifier);
	}

	/**
	 * Create a new {@link BasicCassandraPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 * @param comparator must not be {@literal null}.
	 * @since 2.1
	 */
	protected BasicCassandraPersistentEntity(TypeInformation<T> typeInformation,
			CassandraPersistentEntityMetadataVerifier verifier, Comparator<CassandraPersistentProperty> comparator) {

		super(typeInformation, comparator);

		setVerifier(verifier);
	}

	protected CqlIdentifier determineTableName() {
		return determineName(NamingStrategy::getTableName, findAnnotation(Table.class), "value").getRequiredIdentifier();
	}

	protected @Nullable CqlIdentifier determineKeyspace() {
		return determineName(NamingStrategy::getKeyspace, findAnnotation(Table.class), "keyspace").getIdentifier();
	}

	@SuppressWarnings("NullAway")
	CqlIdentifierGenerator.GeneratedName determineName(
			BiFunction<NamingStrategy, CassandraPersistentEntity<?>, @Nullable String> defaultNameGenerator,
			@Nullable Annotation annotation, String annotationAttribute) {

		if (annotation != null) {
			return this.namingAccessor.generate((String) AnnotationUtils.getValue(annotation, annotationAttribute),
					defaultNameGenerator, this, PARSER,
					this::getValueEvaluationContext);
		}

		return this.namingAccessor.generate(null, defaultNameGenerator, this,
				PARSER, this::getValueEvaluationContext);
	}

	@Override
	protected EvaluationContext getEvaluationContext(@Nullable Object rootObject) {
		return postProcess(super.getEvaluationContext(rootObject == null ? applicationContext : rootObject));
	}

	@Override
	protected EvaluationContext getEvaluationContext(@Nullable Object rootObject, ExpressionDependencies dependencies) {
		return postProcess(super.getEvaluationContext(rootObject == null ? applicationContext : rootObject, dependencies));
	}

	private EvaluationContext postProcess(EvaluationContext evaluationContext) {

		if (evaluationContext instanceof StandardEvaluationContext sec) {

			if (sec.getRootObject().getValue() instanceof BeanFactory) {
				sec.addPropertyAccessor(new BeanFactoryAccessor());
			}
		}

		return evaluationContext;
	}

	@Override
	protected ValueEvaluationContext getValueEvaluationContext(@Nullable Object rootObject) {
		return super.getValueEvaluationContext(rootObject);
	}

	@Override
	protected ValueEvaluationContext getValueEvaluationContext(@Nullable Object rootObject,
			ExpressionDependencies dependencies) {
		return super.getValueEvaluationContext(rootObject, dependencies);
	}

	@Override
	public void addAssociation(Association<CassandraPersistentProperty> association) {
		throw new UnsupportedCassandraOperationException("Cassandra does not support associations");
	}

	@Override
	public void doWithAssociations(AssociationHandler<CassandraPersistentProperty> handler) {}

	@Override
	public boolean isCompositePrimaryKey() {
		return isAnnotationPresent(PrimaryKeyClass.class);
	}

	@Override
	public void verify() throws MappingException {

		super.verify();

		this.verifier.verify(this);

		if (this.tableName == null) {
			setTableName(determineTableName());
		}

		if (this.keyspace == null) {
			setKeyspace(determineKeyspace());
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.applicationContext = context;
	}

	@Override
	public @Nullable CqlIdentifier getKeyspace() {
		return Optional.ofNullable(this.keyspace).orElseGet(this::determineKeyspace);
	}

	@Override
	public void setKeyspace(@Nullable CqlIdentifier keyspace) {
		this.keyspace = keyspace;
	}

	@Override
	public CqlIdentifier getTableName() {
		return Optional.ofNullable(this.tableName).orElseGet(this::determineTableName);
	}

	@Override
	public void setTableName(CqlIdentifier tableName) {

		Assert.notNull(tableName, "CqlIdentifier must not be null");

		this.tableName = tableName;
	}

	/**
	 * Set the {@link NamingStrategy} to use.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 * @since 3.0
	 */
	public void setNamingStrategy(NamingStrategy namingStrategy) {
		this.namingAccessor.setNamingStrategy(namingStrategy);
	}

	/**
	 * @param verifier The verifier to set.
	 */
	public void setVerifier(CassandraPersistentEntityMetadataVerifier verifier) {
		this.verifier = verifier;
	}

	/**
	 * @return the verifier.
	 */
	@SuppressWarnings("unused")
	public CassandraPersistentEntityMetadataVerifier getVerifier() {
		return this.verifier;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public boolean isUserDefinedType() {
		return false;
	}

	@Override
	@SuppressWarnings("NullAway")
	public @Nullable CassandraPersistentProperty getProperty(Parameter<?, CassandraPersistentProperty> parameter) {

		if (parameter.getName() == null) {
			return null;
		}

		MergedAnnotations annotations = parameter.getAnnotations();
		if (annotations.isPresent(Column.class) || annotations.isPresent(Element.class)) {

			return constructorProperties.computeIfAbsent(parameter, it -> {

				CassandraPersistentProperty property = getPersistentProperty(it.getName());
				return new AnnotatedCassandraConstructorProperty(
						property == null ? new CassandraConstructorProperty(it, this) : property, it.getAnnotations());
			});
		}

		return getPersistentProperty(parameter.getName());
	}

	@Override
	public String toString() {
		return getName();
	}

}
