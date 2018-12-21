package org.apache.impala.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.analysis.AlterTableSetColumnStats;
import org.apache.impala.analysis.AlterTableSetTblProperties;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CreateDbStmt;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.CreateViewStmt;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.PartitionSet;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.View;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.serialize.ArraySerializer;
import org.apache.impala.common.serialize.JacksonTreeSerializer;
import org.apache.impala.common.serialize.ToJsonOptions;
import org.apache.impala.service.CatalogOpExecutor.CatalogMutator;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TCreateOrAlterViewParams;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TQueryCtx;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class MockPlanner extends FrontendTestBase {

  public List<String> parseDump(File file) throws IOException {
    List<String> stmts = new ArrayList<>();
    try (BufferedReader in = new BufferedReader(new FileReader(file))) {
      while (skipToResults(in)) {
        stmts.add(parseStmt(in));
      }
    }
    return stmts;
  }

	private boolean skipToResults(BufferedReader in) throws IOException {
	  String line;
    while ((line = in.readLine()) != null) {
      if (!line.startsWith("+---")) continue;
      line = in.readLine();
      if (line == null) break;
      if (!line.startsWith("| result ")) continue;
      line = in.readLine();
      if (line == null) break;
      if (!line.startsWith("+---")) continue;
      return true;
    }
    return false;
  }

  private String parseStmt(BufferedReader in) throws IOException {
    StringBuilder buf = new StringBuilder();
    String line;
    while ((line = in.readLine()) != null) {
      if (line.startsWith("+---")) break;
      line = line.substring(1, line.length()-2).trim();
      if (line.isEmpty()) continue;
      buf.append(line).append("\n");
    }
    return buf.toString();
  }

  private List<String> parseSqlFile(File file) throws IOException {
    List<String> stmts = new ArrayList<>();
    try (BufferedReader in = new BufferedReader(new FileReader(file))) {
      for (;;) {
        String stmt = parseSqlStmt(in);
        if (stmt == null) break;
        stmts.add(stmt);
      }
    }
    return stmts;
  }

  private String parseSqlStmt(BufferedReader in) throws IOException {
    String line;
    while ((line = in.readLine()) != null) {
      line = line.trim();
      if (! line.isEmpty()) break;
    }
    if (line == null) return null;
    StringBuilder stmt = new StringBuilder();
    do {
      line = line.trim();
      if (line.isEmpty()) continue;
      if (line.equals("NULL")) continue;
      if (line.endsWith(";")) {
        line = line.substring(0, line.length() - 1);
        stmt.append(line).append("\n");
        break;
      }
      stmt.append(line).append("\n");
    } while ((line = in.readLine()) != null);
    return stmt.toString();
  }

  @Test
	public void testDump() throws IOException {
    List<String> dbNames = Lists.newArrayList(
        "retrdmp01_business_users_enc",
        "retrdmp01_data_lake_enc");
    for (String dbName : dbNames) {
      System.out.println("CREATE DATABASE " + dbName);
  		addTestDb(dbName, null);
    }
		File schemaFile = new File("/home/progers/data/Prudential/export-ddl.txt");
		List<String> stmts = parseDump(schemaFile);
		for (String stmt : stmts) {
		  System.out.println(stmt);
		  addTestTable(stmt);
		}
	}

  List<String> dbs_ = new ArrayList<>();

  @Test
  public void testSql() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/Prudential");
    parseSql(new File(dir, "pru-ddl.sql"));
    parseSql(new File(dir, "export-stats.ddl"));
    parseSql(new File(dir, "import-stats.sql"));

    dumpCatalog(new File(dir, "schema.json"));

    testQuery(new File(dir, "old_profile1.txt"));
    testQuery(new File(dir, "old_profile2.txt"));
  }

  public void dumpCatalog(File destFile) throws IOException {
    try (FileWriter out = new FileWriter(destFile);
        JacksonTreeSerializer ts = new JacksonTreeSerializer(out, ToJsonOptions.full())) {
      ArraySerializer as = ts.root().array("dbs");
      for (String db : dbs_) {
        catalog_.getDb(db).serialize(as.object());
      }
    }
  }

  public void testQuery(File file) throws FileNotFoundException, IOException, ImpalaException {
    File dir = file.getParentFile();
    ProfileParser pp = new ProfileParser(file);
    String query = pp.query();
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        "default", System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);
    PlanCtx planCtx = new PlanCtx(queryCtx);
    TExecRequest execRequest = frontend_.createExecRequest(planCtx);
    String explainStr = planCtx.getExplainString();

    String baseName = FilenameUtils.getBaseName(file.getName());
    File destFile = new File(dir, baseName + "-plan-I31.txt");
    writeFile(destFile, explainStr);

    destFile = new File(dir, baseName + "-plan.txt");
    writeFile(destFile, pp.plan());

    destFile = new File(dir, baseName + "-outline-I31.txt");
    writeFile(destFile, reduce(explainStr));

    destFile = new File(dir, baseName + "-outline.txt");
    writeFile(destFile, reduce(pp.plan()));
  }

  public void writeFile(File destFile, String str) throws IOException {
    try (PrintWriter out = new PrintWriter(new FileWriter(destFile))) {
      out.println(str);
    }
  }

  private void parseSql(File file) throws IOException, ImpalaException {
    List<String> stmts = parseSqlFile(file);
    for (String stmt : stmts) {
      stmt = sanitize(stmt);
      //System.out.println(stmt);
      runStmt(stmt);
    }
  }

  private String sanitize(String stmt) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < stmt.length(); i++) {
      char c = stmt.charAt(i);
      if (c < ' ' && c != '\n') {
        String octal = "000" + Integer.toOctalString(c);
        buf.append("\\").append(octal.substring(octal.length() - 3, octal.length()));
      } else buf.append(c);
    }
    return buf.toString();
  }

  private void runStmt(String sql) throws ImpalaException {
//    // Horrible hack since Impala does not support Hive alter table syntax
//    if (sql.toLowerCase().startsWith("alter table")) {
//      alterTable(sql);
//      return;
//    }
    sql = sql.replaceAll("\n--", "\n");
    sql = sql.replaceAll("'(\\d+),", "'$1',");
    AnalysisContext ctx = createAnalysisCtx();
    StatementBase parsedStmt = Parser.parse(sql, ctx.getQueryOptions());
    if (parsedStmt instanceof AlterTableSetTblProperties) {
      preAnalysisCheck((AlterTableSetTblProperties) parsedStmt);
    }
    else if (parsedStmt instanceof AlterTableSetColumnStats) {
      ((AlterTableSetColumnStats) parsedStmt).setPermissiveMode();
    }
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(frontend_, ctx.getQueryCtx().session.database, null);
    StmtTableCache stmtTableCache = mdLoader.loadTables(parsedStmt);
    AnalysisResult analysisResult = ctx.analyzeAndAuthorize(parsedStmt, stmtTableCache, frontend_.getAuthzChecker());
    StatementBase stmt = (StatementBase) analysisResult.getStmt();;
    if (stmt instanceof CreateDbStmt) {
      createDb((CreateDbStmt) stmt);
    } else if (stmt instanceof CreateTableStmt) {
      createTable(sql, (CreateTableStmt) stmt);
    } else if (stmt instanceof AlterTableSetTblProperties) {
      setTableProperties((AlterTableSetTblProperties) stmt);
    } else if (stmt instanceof AlterTableSetColumnStats) {
      setColumnStats((AlterTableSetColumnStats) stmt);
    } else if (stmt instanceof CreateViewStmt) {
      createView((CreateViewStmt) stmt);
    } else {
      throw new UnsupportedOperationException(
          "Statement not supported: " + stmt.getClass().getSimpleName());
    }
  }

  private void preAnalysisCheck(AlterTableSetTblProperties stmt) throws CatalogException {
    String dbName = stmt.getTableName().getDb();
    if (dbName == null || dbName.isEmpty()) return;
    Db db = catalog_.getDb(dbName);
    String tableName = stmt.getTableName().getTbl();
    if (tableName == null || tableName.isEmpty()) return;
    Table table = db.getTable(tableName);
    //System.out.println(table.toString());
    if (!(table instanceof HdfsTable)) return;
    HdfsTable fsTable = (HdfsTable) table;
    PartitionSet partSet = stmt.getPartitionSet();
    if (partSet == null) return;
    for (Expr expr : partSet.getPartitionExprs()) {
      // Just let analysis fail if the expression is wrong
      if (!(expr instanceof BinaryPredicate)) return;
      Expr left = expr.getChild(0);
      if (!(left instanceof SlotRef)) return;
      Expr right = expr.getChild(1);
      if (!(right instanceof LiteralExpr)) return;
      LiteralExpr partKey = (LiteralExpr) right;
      SlotRef partName = (SlotRef) left;
      List<String> path = partName.getRawPath();
      if (path.size() != 1) return;
      String name = path.get(0);
      //System.out.println("Create partition: " + name + "=" + partKey.toSql());
      HdfsPartition proto = fsTable.getPrototypePartition();
      //System.out.println("Prototype: " + proto.toString());
      org.apache.hadoop.hive.metastore.api.Partition msPartition = new org.apache.hadoop.hive.metastore.api.Partition();
      msPartition.setDbName(dbName);
      msPartition.setTableName(tableName);
      StorageDescriptor sd = fsTable.getMetaStoreTable().getSd();
      StorageDescriptor partSd = sd.deepCopy();
      partSd.setBucketCols(new ArrayList<>());
      partSd.setSortCols(new ArrayList<>());
      partSd.setParameters(new HashMap<>());
      partSd.setLocation(sd.getLocation() + "/" + name + "=" + partKey.toSql());
      msPartition.setSd(partSd);
      HdfsPartition partition = new HdfsPartition(fsTable, msPartition,
          Lists.newArrayList(partKey), proto.getInputFormatDescriptor(), new ArrayList<>(), fsTable.getAccessLevel());
      //System.out.println(partition);
      fsTable.addPartition(partition);
    }
  }

  private void createDb(CreateDbStmt stmt) {
    dbs_.add(stmt.getDb());
    addTestDb(stmt.getDb(), stmt.getComment());
  }

  private void createTable(String sql, CreateTableStmt stmt) {
    addTestTable(sql, stmt);
  }

  private void setTableProperties(AlterTableSetTblProperties stmt) throws ImpalaException {
    // Resolution points to the catalog table, not a copy, so OK to update
    // that table directly.
    Table table = (Table) stmt.getTargetTable();
    TAlterTableParams params = stmt.toThrift();
    // Put the metadata as properties, then tell the table to parse out stats
    table.getMetaStoreTable().getParameters().putAll(params.getSet_tbl_properties_params().getProperties());
    table.setTableStats(stmt.getTargetTable().getMetaStoreTable());
  }

  private void setColumnStats(AlterTableSetColumnStats stmt) {
    TAlterTableParams params = stmt.toThrift();
    Table table = (Table) stmt.getTargetTable();
    ColumnStatistics colStats = CatalogMutator.createHiveColStats(params.getUpdate_stats_params(), table);
    FeCatalogUtils.injectColumnStats(colStats.getStatsObj(), table);
  }

  private void createView(CreateViewStmt stmt) throws TableLoadingException {
    TCreateOrAlterViewParams params = stmt.toThrift();
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.createView");
    if (catalog_.containsTable(tableName.getDb(), tableName.getTbl())) {
      System.out.println(String.format("Skipping view creation because %s already exists and " +
          "ifNotExists is true.", tableName));
      return;
    }

    // Create new view.
    org.apache.hadoop.hive.metastore.api.Table view =
        new org.apache.hadoop.hive.metastore.api.Table();
    CatalogMutator.setCreateViewAttributes(params, view);
    Db db = catalog_.getDb(tableName.getDb());
    if (db == null)
      throw new IllegalArgumentException("DB not found: " + db);
    Table dummyTable = Table.fromMetastoreTable(db, view);
    ((View) dummyTable).localLoad();
    db.addTable(dummyTable);
  }

  public static class ProfileParser {
    private final File file_;
    private final String query_;
    private final String plan_;
    private final String execSummary_;

    public ProfileParser(File file) throws FileNotFoundException, IOException {
      file_ = file;
      try (BufferedReader in = new BufferedReader(new FileReader(file_))) {
        query_ = parseQuery(in);
        plan_ = parsePlan(in);
        execSummary_ = parseTable(in);
      }
    }

    public String query() { return query_; }
    public String plan() { return plan_; }
    public String summary() { return execSummary_; }

    private static final String QUERY_PREFIX = "    Sql Statement: ";
    private static final String END_QUERY_MARKER = "    Coordinator: ";

    private String parseQuery(BufferedReader in) throws IOException {
      String line;
      while ((line = in.readLine()) != null) {
        if (line.startsWith(QUERY_PREFIX)) break;
      }
      if (line == null) {
        throw new IllegalArgumentException("Query not found in profile");
      }
      StringBuilder buf = new StringBuilder();
      buf.append(line.substring(QUERY_PREFIX.length())).append("\n");
      while ((line = in.readLine()) != null) {
        if (line.startsWith(END_QUERY_MARKER)) break;
        buf.append(line).append("\n");
      }
      if (line == null) {
        throw new IllegalArgumentException("EOF while reading query");
      }
      // Input positioned just after Coordinator: line
      return buf.toString();
    }

    private static final String PLAN_MARKER = "    Plan:";
    private static final String END_PLAN_MARKER = "----------------";

    private String parsePlan(BufferedReader in) throws IOException {
      String line;
      while ((line = in.readLine()) != null) {
        if (line.startsWith(PLAN_MARKER)) break;
      }
      if (line == null) {
        throw new IllegalArgumentException("Plan not found in profile");
      }
      // Skip ruling and resource lines
      while ((line = in.readLine()) != null) {
        if (line.isEmpty()) break;
      }
      StringBuilder buf = new StringBuilder();
      buf.append(line).append("\n");
      while ((line = in.readLine()) != null) {
        if (line.startsWith(END_PLAN_MARKER)) break;
        buf.append(line).append("\n");
      }
      if (line == null) {
        throw new IllegalArgumentException("EOF while reading plan");
      }
      // Input positioned just after the end ruling
      return buf.toString();
    }

    private static final String TABLE_MARKER = "    ExecSummary:";

    private String parseTable(BufferedReader in) throws IOException {
      String line;
      while ((line = in.readLine()) != null) {
        if (line.startsWith(TABLE_MARKER)) break;
      }
      if (line == null) {
        throw new IllegalArgumentException("Exec summaray not found in profile");
      }
      StringBuilder buf = new StringBuilder();
      buf.append(line).append("\n");
      while ((line = in.readLine()) != null) {
        if (line.startsWith("  ")) break;
        buf.append(line).append("\n");
      }
      if (line == null) {
        throw new IllegalArgumentException("EOF while reading exec summary");
      }
      // Input positioned just after the end ruling
      return buf.toString();
    }
  }

  @Test
  public void testProfileParser() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/Prudential");
    File file = new File(dir, "old_profile1.txt");
    ProfileParser pp = new ProfileParser(file);
    System.out.println(file.getAbsolutePath());
    System.out.println("---- Query -----");
    System.out.print(pp.query());
    System.out.println("---- Plan -----");
    System.out.print(pp.plan());
    System.out.println("---- Summary -----");
    System.out.print(pp.summary());
    System.out.println("----");
  }

  public static final String ROOT = "PLAN-ROOT SINK";
  public static final String WARNING = "WARNING: ";

  public String reduce(String fullPlan) {
    StringBuilder buf = new StringBuilder();
    Pattern p = Pattern.compile("([-| ]*)((F?\\d+:)?(.*))?");
    try (BufferedReader in = new BufferedReader(new StringReader(fullPlan))) {
      String line;
      while ((line = in.readLine()) != null) {
        if (line.equals(ROOT)) {
          buf.append(line).append("\n");
          break;
        }
        if (line.startsWith(WARNING)) {
          break;
        }
      }
      if (line.startsWith(WARNING)) {
        buf.append(line).append("\n");
        while ((line = in.readLine()) != null) {
          buf.append(line).append("\n");
          if (line.equals(ROOT)) {
            break;
          }
        }
      }
      String prefix = null;
      boolean skip = false;
      while ((line = in.readLine()) != null) {
        Matcher m = p.matcher(line);
        if (! m.matches()) continue;
        String lead = m.group(1);
        String tail = m.group(2);
        String id = m.group(3);
        String op = m.group(4);
        if (tail == null ||tail.isEmpty()) {
          if (! skip) {
            buf.append(line).append("\n");
            skip = true;
          }
          continue;
        }
        if (id == null || id.isEmpty()) {
          if (op.startsWith("Per-Host")) continue;
          if (op.contains("stats:")) continue;
          if (op.startsWith("stats")) continue;
          if (op.startsWith("tuple-ids")) continue;
          if (op.startsWith("mem-est")) continue;
          buf.append(lead).append(op).append("\n");
          continue;
        }
        if (id.startsWith("F")) continue;
        if (prefix == null) prefix = lead;
        if (lead.length() != prefix.length()) prefix = lead;
        if (!lead.endsWith(" ")) prefix = lead;
        if (op.startsWith("EXCHANGE")) continue;
        buf.append(prefix).append(op).append("\n");
        skip = false;
      }
    } catch (IOException e) {
      // Should never occur
    }
    return buf.toString();
  }

  @Test
  public void testReduce() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/Prudential");
    File file = new File(dir, "old_profile1.txt");
    ProfileParser pp = new ProfileParser(file);
    System.out.println(reduce(pp.plan()));
  }
}
