package com.github.jcustenborder.kafka.connect.cdc.xstream;

import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class GemdTest {
    @Test
    public void printQuery() {
        System.out.println(Oracle12cTableMetadataProvider.PRIMARY_KEY_SQL);
        System.out.println("==============");
        System.out.println(Oracle12cTableMetadataProvider.UNIQUE_CONSTRAINT_SQL);
        System.out.println("==============");
        System.out.println(Oracle12cTableMetadataProvider.COLUMN_SQL);
    }

    @Test
    public void testBuildSchema() {
        Schema schema = SchemaBuilder.string().parameters(
                ImmutableMap.of(Change.ColumnValue.COLUMN_NAME, "ROW_ID"))
                .optional()
                .doc("Oracle specific ROWID from the incoming RowLCR. https://docs.oracle.com/database/121/SQLRF/pseudocolumns008.htm#SQLRF00254 for more info")
                .build();
        System.out.println(schema.parameters().isEmpty());
    }
}
