package com.github.jcustenborder.kafka.connect.cdc.xstream;

import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Test;
import org.junit.Assert;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.github.jcustenborder.kafka.connect.cdc.xstream.Oracle12cTableMetadataProvider.matches;

public class GemdTest {
    static final Map<String, Schema.Type> TYPE_LOOKUP;
    final static Pattern TIMESTAMP_PATTERN = Pattern.compile("^TIMESTAMP\\(\\d\\)$");
    final static Pattern TIMESTAMP_WITH_LOCAL_TIMEZONE = Pattern.compile("^TIMESTAMP\\(\\d\\) WITH LOCAL TIME ZONE$");
    final static Pattern TIMESTAMP_WITH_TIMEZONE = Pattern.compile("^TIMESTAMP\\(\\d\\) WITH TIME ZONE$");

    static {
        Map<String, Schema.Type> map = new HashMap<>();
        map.put("BINARY_DOUBLE", Schema.Type.FLOAT64);
        map.put("BINARY_FLOAT", Schema.Type.FLOAT32);
        map.put("BLOB", Schema.Type.BYTES);
        map.put("CHAR", Schema.Type.STRING);
        map.put("NCHAR", Schema.Type.STRING);
        map.put("CLOB", Schema.Type.STRING);
        map.put("NCLOB", Schema.Type.STRING);
        map.put("NVARCHAR2", Schema.Type.STRING);
        map.put("VARCHAR2", Schema.Type.STRING);
        map.put("NVARCHAR", Schema.Type.STRING);
        map.put("VARCHAR", Schema.Type.STRING);
        TYPE_LOOKUP = ImmutableMap.copyOf(map);
    }
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

    @Test
    public void testBigDecimal() {
        String number = "290.88252314814815";
        String seq = "5125828185";
        Object bd = BigDecimal.valueOf(Double.valueOf(number));
        Object bd1 = BigDecimal.valueOf(Double.valueOf(seq));
        Object converted = Float.valueOf(((BigDecimal) bd).floatValue());
        Object double1 = Double.valueOf(((BigDecimal) bd).doubleValue());
        Object double2 = Double.valueOf(((BigDecimal) bd1).doubleValue());
        Assert.assertTrue(converted instanceof Float);
    }

    @Test
    public void testFHOPEPSMetadata() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        Statement stmt = null;
        Class.forName("oracle.jdbc.driver.OracleDriver");
        conn = DriverManager
                .getConnection("jdbc:oracle:thin:@//fc8racps1n4:1521/f8modsp1.gfoundries.com", "xstrmadmin", "xtra");

        try (PreparedStatement columnStatement = conn.prepareStatement(Oracle12cTableMetadataProvider.COLUMN_SQL)) {
            columnStatement.setString(1, "MDS_ADMIN");
            columnStatement.setString(2, "T_HISTORY_VIEW_WAFER");

            Map<String, Schema> columnSchemas = new LinkedHashMap<>();

            try (ResultSet resultSet = columnStatement.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString(1);

                    try {
                        Schema columnSchema = generateSchema(resultSet, columnName);
                        columnSchemas.put(columnName, columnSchema);
                    } catch (Exception ex) {
                        throw new DataException("Exception thrown while ", ex);
                    }
                }
            }

            columnSchemas.forEach((k,v) -> System.out.println(String.format("Key: %s; Value: %s", k, v)) );
        }


    }

    Schema generateSchema(ResultSet resultSet, final String columnName) throws SQLException {
        SchemaBuilder builder = null;

        String dataType = resultSet.getString(2);
        String scaleString = resultSet.getString(3);
        int scale = resultSet.getInt(3);
        boolean nullable = "Y".equalsIgnoreCase(resultSet.getString(4));
        String comments = resultSet.getString(5);

        if (TYPE_LOOKUP.containsKey(dataType)) {
            Schema.Type type = TYPE_LOOKUP.get(dataType);
            builder = SchemaBuilder.type(type);
        } else if ("NUMBER".equals(dataType)) {
            builder = scaleString != null ? Decimal.builder(scale) : SchemaBuilder.float64();
        } else if (matches(TIMESTAMP_PATTERN, dataType)) {
            builder = org.apache.kafka.connect.data.Timestamp.builder();
        } else if (matches(TIMESTAMP_WITH_LOCAL_TIMEZONE, dataType)) {
            builder = org.apache.kafka.connect.data.Timestamp.builder();
        } else if (matches(TIMESTAMP_WITH_TIMEZONE, dataType)) {
            builder = org.apache.kafka.connect.data.Timestamp.builder();
        } else if ("DATE".equals(dataType)) {
            builder = Timestamp.builder();
        } else {
            String message = String.format("Could not determine schema type for column %s. dataType = %s", columnName, dataType);
            throw new DataException(message);
        }


        if (nullable) {
            builder.optional();
        }

        if (!Strings.isNullOrEmpty(comments)) {
            builder.doc(comments);
        }

        builder.parameters(
                ImmutableMap.of(Change.ColumnValue.COLUMN_NAME, columnName)
        );

        return builder.build();
    }
}
