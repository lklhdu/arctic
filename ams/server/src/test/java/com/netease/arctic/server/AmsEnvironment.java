package com.netease.arctic.server;

import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.Environments;
import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.ams.api.properties.CatalogMetaProperties;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.catalog.CatalogTestHelpers;
import com.netease.arctic.hive.HMSMockServer;
import com.netease.arctic.optimizer.local.LocalOptimizer;
import com.netease.arctic.server.resource.ResourceContainers;
import com.netease.arctic.server.table.DefaultTableService;
import com.netease.arctic.server.utils.Configurations;
import com.netease.arctic.table.TableIdentifier;
import org.apache.commons.io.FileUtils;
import org.apache.curator.shaded.com.google.common.io.MoreFiles;
import org.apache.curator.shaded.com.google.common.io.RecursiveDeleteOption;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;
import org.junit.rules.TemporaryFolder;
import org.kohsuke.args4j.CmdLineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class AmsEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(AmsEnvironment.class);
  private final String rootPath;
  private static final String DEFAULT_ROOT_PATH = "/tmp/arctic_integration";
  private static final String OPTIMIZE_GROUP = "default";
  private final ArcticServiceContainer arcticService;
  private Configurations serviceConfig;
  private DefaultTableService tableService;
  private final AtomicBoolean amsExit;
  private int thriftBindPort;
  private final HMSMockServer testHMS;
  private final Map<String, ArcticCatalog> catalogs = new HashMap<>();
  public static final String ICEBERG_CATALOG = "iceberg_catalog";
  public static String ICEBERG_CATALOG_DIR = "/iceberg/warehouse";
  public static final String MIXED_ICEBERG_CATALOG = "mixed_iceberg_catalog";
  public static String MIXED_ICEBERG_CATALOG_DIR = "/mixed_iceberg/warehouse";
  public static final String MIXED_HIVE_CATALOG = "mixed_hive_catalog";

  public static void main(String[] args) throws Exception {
    AmsEnvironment amsEnvironment = new AmsEnvironment();
    amsEnvironment.start();
    amsEnvironment.startOptimizer();
  }

  public AmsEnvironment() throws Exception {
    this(DEFAULT_ROOT_PATH);
  }

  public AmsEnvironment(String rootPath) throws Exception {
    this.rootPath = rootPath;
    LOG.info("ams environment root path: " + rootPath);
    String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource("")).getPath();
    FileUtils.writeStringToFile(new File(rootPath + "/conf/config.yaml"), getAmsConfig());
    System.setProperty(Environments.SYSTEM_ARCTIC_HOME, rootPath);
    System.setProperty("derby.init.sql.dir", path + "../classes/sql/derby/");
    amsExit = new AtomicBoolean(false);
    arcticService = new ArcticServiceContainer();
    TemporaryFolder hiveDir = new TemporaryFolder();
    hiveDir.create();
    testHMS = new HMSMockServer(hiveDir.newFile());
    testHMS.start();
  }

  public void start() throws Exception {
    startAms();
    DynFields.UnboundField<DefaultTableService> amsTableServiceField =
        DynFields.builder().hiddenImpl(ArcticServiceContainer.class, "tableService").build();
    tableService = amsTableServiceField.bind(arcticService).get();
    DynFields.UnboundField<CompletableFuture<Boolean>> tableServiceField =
        DynFields.builder().hiddenImpl(DefaultTableService.class, "initialized").build();
    boolean tableServiceIsStart = false;
    long startTime = System.currentTimeMillis();
    while (!tableServiceIsStart) {
      if (System.currentTimeMillis() - startTime > 10000) {
        throw new RuntimeException("table service not start yet after 10s");
      }
      try {
        tableServiceField.bind(tableService).get().get();
        tableServiceIsStart = true;
      } catch (RuntimeException e) {
        LOG.info("table service not start yet");
      }
      Thread.sleep(1000);
    }

    initCatalog();
  }

  public void stop() throws IOException {
    stopOptimizer();
    if (this.arcticService != null) {
      this.arcticService.dispose();
    }
    testHMS.stop();
    MoreFiles.deleteRecursively(Paths.get(rootPath), RecursiveDeleteOption.ALLOW_INSECURE);
  }

  public ArcticCatalog catalog(String name) {
    return catalogs.get(name);
  }

  public boolean tableExist(TableIdentifier tableIdentifier) {
    return tableService.tableExist(tableIdentifier.buildTableIdentifier());
  }

  public HMSMockServer getTestHMS() {
    return testHMS;
  }

  private void initCatalog() {
    createIcebergCatalog();
    createMixIcebergCatalog();
    createMixHiveCatalog();
  }

  private void createIcebergCatalog() {
    String warehouseDir = rootPath + ICEBERG_CATALOG_DIR;
    Map<String, String> properties = Maps.newHashMap();
    createDirIfNotExist(warehouseDir);
    properties.put(CatalogMetaProperties.KEY_WAREHOUSE, warehouseDir);
    CatalogMeta catalogMeta = CatalogTestHelpers.buildCatalogMeta(ICEBERG_CATALOG,
        CatalogMetaProperties.CATALOG_TYPE_HADOOP, properties, TableFormat.ICEBERG);
    tableService.createCatalog(catalogMeta);
    catalogs.put(ICEBERG_CATALOG, CatalogLoader.load(getAmsUrl() + "/" + ICEBERG_CATALOG));
  }

  private void createMixIcebergCatalog() {
    String warehouseDir = rootPath + MIXED_ICEBERG_CATALOG_DIR;
    Map<String, String> properties = Maps.newHashMap();
    createDirIfNotExist(warehouseDir);
    properties.put(CatalogMetaProperties.KEY_WAREHOUSE, warehouseDir);
    CatalogMeta catalogMeta = CatalogTestHelpers.buildCatalogMeta(MIXED_ICEBERG_CATALOG,
        CatalogMetaProperties.CATALOG_TYPE_AMS, properties, TableFormat.MIXED_ICEBERG);
    tableService.createCatalog(catalogMeta);
    catalogs.put(MIXED_ICEBERG_CATALOG, CatalogLoader.load(getAmsUrl() + "/" + MIXED_ICEBERG_CATALOG));
  }

  private void createMixHiveCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    CatalogMeta catalogMeta = CatalogTestHelpers.buildHiveCatalogMeta(MIXED_HIVE_CATALOG,
        properties, testHMS.hiveConf(), TableFormat.MIXED_HIVE);
    tableService.createCatalog(catalogMeta);
    catalogs.put(MIXED_HIVE_CATALOG, CatalogLoader.load(getAmsUrl() + "/" + MIXED_HIVE_CATALOG));
  }

  private void createDirIfNotExist(String warehouseDir) {
    try {
      Files.createDirectories(Paths.get(warehouseDir));
    } catch (IOException e) {
      LOG.error("failed to create iceberg warehouse dir {}", warehouseDir, e);
      throw new RuntimeException(e);
    }
  }

  public void startOptimizer() {
    new Thread(() -> {
      String[] startArgs = {"-m", "1024", "-a", getAmsUrl(), "-p", "1", "-g", "default"};
      try {
        LocalOptimizer.main(startArgs);
      } catch (CmdLineException e) {
        throw new RuntimeException(e);
      }
    }).start();
  }

  public void stopOptimizer() {
    DynFields.UnboundField<DefaultOptimizingService> field =
        DynFields.builder().hiddenImpl(ArcticServiceContainer.class, "optimizingService").build();
    field.bind(arcticService).get().listOptimizers()
        .forEach(resource -> {
          ResourceContainers.get(resource.getContainerName()).releaseOptimizer(resource);
        });
  }

  public String getAmsUrl() {
    return "thrift://127.0.0.1:" + thriftBindPort;
  }

  private void startAms() throws Exception {
    Thread amsRunner = new Thread(() -> {
      int retry = 10;
      try {
        while (true) {
          try {
            LOG.info("start ams");
            genThriftBindPort();
            DynFields.UnboundField<Configurations> field =
                DynFields.builder().hiddenImpl(ArcticServiceContainer.class, "serviceConfig").build();
            serviceConfig = field.bind(arcticService).get();
            serviceConfig.set(ArcticManagementConf.THRIFT_BIND_PORT, thriftBindPort);
            serviceConfig.set(ArcticManagementConf.REFRESH_EXTERNAL_CATALOGS_INTERVAL, 1000L);
            // when AMS is successfully running, this thread will wait here
            arcticService.startService();
            break;
          } catch (TTransportException e) {
            if (e.getCause() instanceof BindException) {
              LOG.error("start ams failed", e);
              if (retry-- < 0) {
                throw e;
              } else {
                Thread.sleep(1000);
              }
            } else {
              throw e;
            }
          } catch (Throwable e) {
            throw e;
          }
        }
      } catch (Throwable t) {
        LOG.error("start ams failed", t);
      } finally {
        amsExit.set(true);
      }
    }, "ams-runner");
    amsRunner.start();

    DynFields.UnboundField<TServer> amsServerField =
        DynFields.builder().hiddenImpl(ArcticServiceContainer.class, "server").build();
    while (true) {
      if (amsExit.get()) {
        LOG.error("ams exit");
        break;
      }
      TServer thriftServer = amsServerField.bind(arcticService).get();
      if (thriftServer != null && thriftServer.isServing()) {
        LOG.info("ams start");
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.warn("interrupt ams");
        amsRunner.interrupt();
        break;
      }
    }
  }

  private void genThriftBindPort() {
    // create a random port between 14000 - 18000
    int port = new Random().nextInt(4000);
    this.thriftBindPort = port + 14000;
  }

  private String getAmsConfig() {
    return "ams:\n" +
        "  admin-username: \"admin\"\n" +
        "  admin-passowrd: \"admin\"\n" +
        "  server-bind-host: \"0.0.0.0\"\n" +
        "  server-expose-host: \"127.0.0.1\"\n" +
        "  refresh-external-catalog-interval: 180000 # 3min\n" +
        "  refresh-table-thread-count: 10\n" +
        "  refresh-table-interval: 60000 #1min\n" +
        "  expire-table-thread-count: 10\n" +
        "  clean-orphan-file-thread-count: 10\n" +
        "  sync-hive-tables-thread-count: 10\n" +
        "\n" +
        "  thrift-server:\n" +
        "    bind-port: 1260\n" +
        "    max-message-size: 104857600 # 100MB\n" +
        "    worker-thread-count: 20\n" +
        "    selector-thread-count: 2\n" +
        "    selector-queue-size: 4\n" +
        "\n" +
        "  http-server:\n" +
        "    bind-port: 1630\n" +
        "\n" +
        "  self-optimizing:\n" +
        "    commit-thread-count: 10\n" +
        "\n" +
        "  database:\n" +
        "    type: \"derby\"\n" +
        "    jdbc-driver-class: \"org.apache.derby.jdbc.EmbeddedDriver\"\n" +
        "    url: \"jdbc:derby:" + rootPath.replace("\\", "\\\\") + "/derby;create=true\"\n" +
        "\n" +
        "  terminal:\n" +
        "    backend: local\n" +
        "    local.spark.sql.session.timeZone: UTC\n" +
        "    local.spark.sql.iceberg.handle-timestamp-without-timezone: false\n" +
        "\n" +
        "containers:\n" +
        "  - name: localContainer\n" +
        "    container-impl: com.netease.arctic.optimizer.LocalOptimizerContainer\n" +
        "    properties:\n" +
        "      memory: \"1024\"\n" +
        "      hadoop_home: /opt/hadoop\n" +
        "      # java_home: /opt/java\n" +
        "\n" +
        "optimizer_groups:\n" +
        "  - name: " + OPTIMIZE_GROUP + "\n" +
        "    container: localContainer\n" +
        "    properties:\n" +
        "      memory: 1024 # MB\n";
  }
}
