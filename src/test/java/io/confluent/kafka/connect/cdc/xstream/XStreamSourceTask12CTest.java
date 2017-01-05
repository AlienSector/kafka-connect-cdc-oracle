package io.confluent.kafka.connect.cdc.xstream;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.JsonTableMetadata;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.xstream.docker.Oracle12cClusterHealthCheck;
import io.confluent.kafka.connect.cdc.xstream.docker.Oracle12cSettings;
import io.confluent.kafka.connect.cdc.xstream.model.JsonRowLCR;
import oracle.jdbc.OracleConnection;
import oracle.streams.ColumnValue;
import oracle.streams.LCR;
import oracle.streams.RowLCR;
import oracle.streams.StreamsException;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@Category(Integration.class)
@DockerCompose(dockerComposePath = Oracle12cTest.DOCKER_COMPOSE_FILE, clusterHealthCheck = Oracle12cClusterHealthCheck.class)
public class XStreamSourceTask12CTest extends Oracle12cTest {
  private static Logger log = LoggerFactory.getLogger(XStreamSourceTask12CTest.class);
  XStreamSourceConnectorConfig config;
  OracleConnection oracleConnection;
  XStreamOutput xStreamOutput;


  @BeforeEach
  public void before(
      @Oracle12cSettings
          Map<String, String> settings
  ) throws StreamsException, InterruptedException {
    this.config = new XStreamSourceConnectorConfig(settings);
    this.oracleConnection = OracleUtils.openUnPooledConnection(this.config);
    this.xStreamOutput = XStreamOutputImpl.attach(this.oracleConnection, this.config, null);
  }

  void assertColumnValue(ColumnValue expected, ColumnValue actual, String message) {
    String prefix = null != message ? message + ": " : "";
    assertNotNull(expected, prefix + "expected cannot be null.");
    assertNotNull(actual, prefix + "actual cannot be null.");

    assertEquals(expected.getCharsetId(), actual.getCharsetId(), prefix + "getCharsetId does not match.");
    assertEquals(expected.getColumnData(), actual.getColumnData(), prefix + "getColumnData does not match.");
    assertEquals(expected.getColumnDataType(), actual.getColumnDataType(), prefix + "getColumnDataType does not match.");
    assertEquals(expected.getTDEFlag(), actual.getTDEFlag(), prefix + "getTDEFlag does not match.");
  }

  void assertColumnValues(ColumnValue[] expected, ColumnValue[] actual, String message) {
    String prefix = null != message ? message : "";
    if (null == expected) {
      assertNull(actual, prefix + "actual should be null.");
      return;
    }
    assertNotNull(actual, prefix + "actual should not be null.");
    assertEquals(expected.length, actual.length, prefix + "number of elements in do not match.");

    for (int i = 0; i < expected.length; i++) {
      ColumnValue expectedColumn = expected[i];
      ColumnValue actualColumn = actual[i];
      assertColumnValue(expectedColumn, actualColumn, String.format("Index %d", i));
    }


  }

  void assertRowLCR(JsonRowLCR expected, JsonRowLCR actual) {
    assertNotNull(expected, "expected cannot be null.");
    assertNotNull(actual, "actual cannot be null.");

    assertEquals(expected.getCommandType(), actual.getCommandType(), "getCommandType() does not match.");
    assertEquals(expected.getObjectName(), actual.getObjectName(), "getObjectName() does not match.");
    assertEquals(expected.getObjectOwner(), actual.getObjectOwner(), "getObjectOwner() does not match.");
    assertArrayEquals(expected.getPosition(), actual.getPosition(), "getPosition() does not match.");
    assertEquals(expected.getSourceTime(), actual.getSourceTime(), "getSourceTime() does not match.");
    assertArrayEquals(expected.getTag(), actual.getTag(), "getTag() does not match.");
    assertEquals(expected.getTransactionId(), actual.getTransactionId(), "getTag() does not match.");

    assertColumnValues(expected.getOldValues(), actual.getOldValues(), "getOldValues() do not match");
    assertColumnValues(expected.getNewValues(), actual.getNewValues(), "getNewValues() do not match");
  }

  void test(TestCase testCase) throws IOException {
    log.debug("writing lcr for {} to {}", testCase.changeKey, testCase.lcrPath);
    JsonRowLCR.write(testCase.lcrPath.toFile(), testCase.expectedJsonLCR);
    JsonRowLCR actualJsonLCR = JsonRowLCR.read(testCase.lcrPath.toFile());
    assertRowLCR(testCase.expectedJsonLCR, actualJsonLCR);
  }

  @Disabled
  @TestFactory
  public Stream<DynamicTest> foo() throws StreamsException, SQLException, InterruptedException, IOException {
    Path parentPath = Paths.get("/Users/jeremy/source/confluent/kafka-connect/public/kafka-connect-cdc/kafka-connect-cdc-xstream/src/test/resources/io/confluent/kafka/connect/cdc/xstream/lcrs");


    int nullCount = 1;

    Multimap<ChangeKey, JsonRowLCR> rowLCRs = ArrayListMultimap.create();

    while (nullCount <= 5) {
      LCR lcr = this.xStreamOutput.receiveLCR();

      if (null == lcr) {
        log.debug("{} null lcr(s) returned.", nullCount);
        nullCount++;
        continue;
      }

      nullCount = 0;
      log.debug("LCR = {}", lcr);

      if (lcr instanceof RowLCR) {
        RowLCR rowLCR = (RowLCR) lcr;

        if (RowLCR.COMMIT.equals(lcr.getCommandType())) {
          continue;
        }

        if (!"DATATYPE_TESTING".equalsIgnoreCase(rowLCR.getObjectOwner())) {
          continue;
        }

        JsonRowLCR jsonRowLCR = JsonRowLCR.build(xStreamOutput, rowLCR);
        ChangeKey changeKey = new ChangeKey(rowLCR.getSourceDatabaseName(), rowLCR.getObjectOwner(), rowLCR.getObjectName());
        rowLCRs.put(changeKey, jsonRowLCR);
      }
    }

    List<TestCase> testCases = new ArrayList<>(rowLCRs.size());
    List<OracleChangeTest.ChangeTestCase> changeTestCases = new ArrayList<>();


    for (ChangeKey changeKey : rowLCRs.keySet()) {
      Path tablePath = parentPath.resolve(changeKey.tableName);
      if (!tablePath.toFile().exists()) {
        tablePath.toFile().mkdirs();
      }

      Map<String, Integer> commandTypeCount = new HashMap<>();
      for (JsonRowLCR expectedJsonLCR : rowLCRs.get(changeKey)) {
        Integer count = commandTypeCount.getOrDefault(expectedJsonLCR.getCommandType(), Integer.valueOf(0));
        count++;
        commandTypeCount.put(expectedJsonLCR.getCommandType(), count);
        String filename = String.format("%s%03d.json", expectedJsonLCR.getCommandType(), count);

        TestCase testCase = new TestCase();
        testCase.lcrPath = tablePath.resolve(filename);
        testCase.changeKey = changeKey;
        testCase.expectedJsonLCR = expectedJsonLCR;
        testCase.name = parentPath.relativize(testCase.lcrPath).toString();

        OracleChangeTest.ChangeTestCase changeTestCase = new OracleChangeTest.ChangeTestCase();
        changeTestCase.inputRowLCR = expectedJsonLCR;
        changeTestCase.name = parentPath.relativize(testCase.lcrPath).toString();
        changeTestCase.inputTableMetadata = new JsonTableMetadata();


        testCases.add(testCase);
      }
    }

    return testCases.stream().map(data -> dynamicTest(data.name, () -> test(data)));
  }

  @AfterEach
  public void stop() throws StreamsException, SQLException {
    this.xStreamOutput.detach();
    this.oracleConnection.close();
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public class TestCase {
    String name;
    JsonRowLCR expectedJsonLCR;
    ChangeKey changeKey;
    Path lcrPath;
  }
}