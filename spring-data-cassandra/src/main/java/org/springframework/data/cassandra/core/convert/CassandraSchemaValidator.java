package org.springframework.data.cassandra.core.convert;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.data.cassandra.CassandraKeyspaceDoesNotExistsException;
import org.springframework.data.cassandra.CassandraNoActiveKeyspaceSetForCqlSessionException;
import org.springframework.data.cassandra.CassandraSchemaValidationException;
import org.springframework.data.cassandra.config.CassandraSchemaValidationProfile;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Class that is responsible to validate cassandra schema inside {@link CqlSession} keyspace.
 *
 * @author Mikhail Polivakha
 */
public class CassandraSchemaValidator implements SmartInitializingSingleton {

    private static final Log logger = LogFactory.getLog(CassandraSchemaValidator.class);

    private final CqlSession cqlSession;

    private final CassandraMappingContext cassandraMappingContext;

    private final ColumnTypeResolver columnTypeResolver;

    private final boolean strictValidation;

    public CassandraSchemaValidator(
      CqlSession cqlSession,
      CassandraConverter cassandraConverter,
      boolean strictValidation
    ) {
        this.strictValidation = strictValidation;
        this.cqlSession = cqlSession;
        this.cassandraMappingContext = cassandraConverter.getMappingContext();
        this.columnTypeResolver = new DefaultColumnTypeResolver(
          cassandraMappingContext,
          SchemaFactory.ShallowUserTypeResolver.INSTANCE,
          cassandraConverter::getCodecRegistry,
          cassandraConverter::getCustomConversions
        );
    }

    /**
     * Here, we only consider {@link CqlSession#getKeyspace() current session keyspace},
     * because for now there is no way to customize keyspace for {@link CassandraPersistentEntity}.
     * <p>
     * See <a href="https://github.com/spring-projects/spring-data-cassandra/issues/921">related issue</a>
     */
    @Override
    public void afterSingletonsInstantiated() {
        CqlIdentifier activeKeyspace = cqlSession
          .getKeyspace()
          .orElseThrow(CassandraNoActiveKeyspaceSetForCqlSessionException::new);

        KeyspaceMetadata keyspaceMetadata = cqlSession
          .getMetadata()
          .getKeyspace(activeKeyspace)
          .orElseThrow(() -> new CassandraKeyspaceDoesNotExistsException(activeKeyspace.asInternal()));

        Collection<BasicCassandraPersistentEntity<?>> persistentEntities = cassandraMappingContext.getPersistentEntities();

        CassandraSchemaValidationProfile validationProfile = CassandraSchemaValidationProfile.empty();

        for (BasicCassandraPersistentEntity<?> persistentEntity : persistentEntities) {
            validationProfile.addValidationErrors(validatePersistentEntity(keyspaceMetadata, persistentEntity));
        }

        evaluateValidationResult(validationProfile);
    }

    private void evaluateValidationResult(CassandraSchemaValidationProfile validationProfile) {
        if (validationProfile.validationFailed()) {
            if (strictValidation) {
                throw new CassandraSchemaValidationException(validationProfile.renderExceptionMessage());
            } else {
                if (logger.isErrorEnabled()) {
                    logger.error(validationProfile.renderExceptionMessage());
                }
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Cassandra schema validation completed successfully");
            }
        }
    }

    private List<String> validatePersistentEntity(
      KeyspaceMetadata keyspaceMetadata,
      BasicCassandraPersistentEntity<?> entity
    ) {

        if (entity.isTupleType() || entity.isUserDefinedType()) {
            return List.of();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Validating persistent entity '%s'".formatted(keyspaceMetadata.getName()));
        }

        Optional<TableMetadata> table = keyspaceMetadata.getTable(entity.getTableName());

        if (table.isPresent()) {
            return this.validateProperties(table.get(), entity);
        } else {
            return List.of(
              "Unable to locate target table for persistent entity '%s'. Expected table name is '%s', but no such table in keyspace '%s'".formatted(
                entity.getName(),
                entity.getTableName(),
                keyspaceMetadata.getName()
              )
            );
        }
    }

    private List<String> validateProperties(TableMetadata tableMetadata, BasicCassandraPersistentEntity<?> entity) {

        List<String> validationErrors = new LinkedList<>();

        entity.doWithProperties((PropertyHandler<CassandraPersistentProperty>) persistentProperty -> {

            if (persistentProperty.isTransient()) {
                return;
            }

            CqlIdentifier expectedColumnName = persistentProperty.getColumnName();

            Assert.notNull(expectedColumnName, "Column cannot not be null at this point");

            Optional<ColumnMetadata> column = tableMetadata.getColumn(expectedColumnName);

            if (column.isPresent()) {
                ColumnMetadata columnMetadata = column.get();
                DataType dataTypeExpected = columnTypeResolver.resolve(persistentProperty).getDataType();

                if (dataTypeExpected == null) {
                    validationErrors.add(
                      "Unable to deduce cassandra data type for property '%s' inside the persistent entity '%s'".formatted(
                        persistentProperty.getName(),
                        entity.getName()
                      )
                    );
                } else {
                    if (!Objects.equals(dataTypeExpected.getProtocolCode(), columnMetadata.getType().getProtocolCode())) {
                        validationErrors.add(
                          "Expected '%s' data type for '%s' property in the '%s' persistent entity, but actual data type is '%s'".formatted(
                            dataTypeExpected,
                            persistentProperty.getName(),
                            entity.getName(),
                            columnMetadata.getType()
                          )
                        );
                    }
                }
            } else {
                validationErrors.add(
                  "Unable to locate target column for persistent property '%s' in persistent entity '%s'. Expected to see column with name '%s', but there is no such column in table '%s'".formatted(
                    persistentProperty.getName(),
                    entity.getName(),
                    expectedColumnName,
                    entity.getTableName()
                  )
                );
            }
        });

        return validationErrors;
    }

    public boolean isStrictValidation() {
        return strictValidation;
    }
}
