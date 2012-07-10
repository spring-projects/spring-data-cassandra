package org.springframework.data.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EmbeddedCassandra {
    public static final String TEST_KS = "TestKeyspace";
    public static final String TEST_CF = "users";
    private static Logger logger = LoggerFactory.getLogger(EmbeddedCassandra.class);
    private static boolean started = false;

    public EmbeddedCassandra() throws Exception {
        if (!started) {
            CassandraDaemon cassandraService = new CassandraDaemon();
            cassandraService.activate();
            try {
                loadDataSchema(TEST_KS, Arrays.asList(TEST_CF));
            } catch (Throwable t) {
                logger.debug("Received error when bootstrapping data schema, most likely it exists already."
                        + t.getMessage());
            }
            started = true;
        }
    }

    private void loadDataSchema(String keyspaceName, List<String> colFamilyNames) {
        List<KSMetaData> schema = new ArrayList<KSMetaData>();
        Class<? extends AbstractReplicationStrategy> strategyClass = SimpleStrategy.class;
        Map<String, String> strategyOptions = KSMetaData.optsWithRF(1);

        CFMetaData[] cfDefs = new CFMetaData[colFamilyNames.size()];
        for (int i = 0; i < colFamilyNames.size(); i++) {
            CFMetaData cfDef = new CFMetaData(keyspaceName, colFamilyNames.get(i), ColumnFamilyType.Standard,
                    UTF8Type.instance, null);
            cfDefs[i] = cfDef;
        }

        KSMetaData validKsMetadata = KSMetaData.testMetadata(keyspaceName, strategyClass, strategyOptions, cfDefs);
        schema.add(validKsMetadata);

        Schema.instance.load(schema, Schema.instance.getVersion());
        logger.debug("======================= LOADED DATA SCHEMA FOR TESTS ==========================");
    }
}
