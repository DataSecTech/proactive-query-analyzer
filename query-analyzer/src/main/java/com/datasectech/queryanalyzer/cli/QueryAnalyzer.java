package com.datasectech.queryanalyzer.cli;

import com.datasectech.queryanalyzer.core.db.cache.AnalyzerDbCacheSchemaFactory;
import com.datasectech.queryanalyzer.core.db.cache.mapper.IndividualCacheMapper;
import com.datasectech.queryanalyzer.core.db.disk.AnalyzerDbDiskSchemaFactory;
import com.datasectech.queryanalyzer.core.query.DataMapper;
import com.datasectech.queryanalyzer.core.query.OutputPrinter;
import com.datasectech.queryanalyzer.core.query.dto.*;
import com.datasectech.queryanalyzer.core.query.modelbuilders.InlineModelGenerator;
import com.datasectech.queryanalyzer.core.query.sensitivity.SensitivityAnalyzer;
import com.datasectech.queryanalyzer.core.query.sensitivity.SensitivityAnalyzerBuilder;
import com.datasectech.queryanalyzer.core.query.sensitivity.metadata.JdbcMetadataReader;
import com.datasectech.queryanalyzer.core.query.writer.SDLPlanWriterUtils;
import com.fiftyonred.mock_jedis.MockJedis;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.Closeable;
import java.io.File;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class QueryAnalyzer implements Closeable {

    private final static Logger logger = LogManager.getLogger(QueryAnalyzer.class);

    protected DataMapper dataMapper;
    protected Input input;

    // Can be path to a model file or inline model string
    protected String model;
    protected CalciteModel calciteModel;
    protected boolean isAnalyzerDBSchema;

    protected Connection connection;
    protected CalciteConnection calciteConnection;
    protected FrameworkConfig config;
    protected String schemaName;
    protected OutputPrinter outputPrinter;
    protected boolean isEnabledSavePlan;

    protected SensitivityAnalyzer sensitivityAnalyzer;

    public QueryAnalyzer(File inputFile) {
        if (!inputFile.exists()) {
            throw new RuntimeException("Queries file does not exist");
        }

        logger.info("Reading input queries file {}", inputFile);

        dataMapper = new DataMapper();
        readAndValidateInput(inputFile);
        readModel(inputFile);
        validateModel();
    }

    private void readAndValidateInput(File inputFile) {
        input = dataMapper.readInput(inputFile);
        if (input.db == null) {
            throw new RuntimeException("Input file must have database configuration");
        }

        if (input.db.planDir != null && !input.db.planDir.isEmpty()) {
            outputPrinter = new OutputPrinter(getAbsoluteFileRelToInput(inputFile, input.db.planDir));
            isEnabledSavePlan = true;
        }
    }

    public File getAbsoluteFileRelToInput(File inputFile, String path) {
        String pathTrimmed = path.trim();

        if (".".equals(pathTrimmed)) {
            return inputFile.getParentFile().getAbsoluteFile();
        }

        File inputPath = new File(pathTrimmed);

        if (inputPath.isAbsolute()) {
            return inputPath;
        }

        return new File(inputFile.getParent(), pathTrimmed);
    }

    private void readModel(File inputFile) {

        if (input.db.calciteModel.startsWith("sdl_csv:")) {

            File csvDir = getAbsoluteFileRelToInput(inputFile, input.db.calciteModel.substring(8));
            model = InlineModelGenerator.generateInlineCSVModel(input.db.schema, csvDir.toString());
            calciteModel = dataMapper.readModel(model.substring(7));
            return;
        }

        if (input.db.calciteModel.startsWith("inline:")) {
            model = input.db.calciteModel;
            calciteModel = dataMapper.readModel(model.substring(7));
            return;
        }

        File modelFile = getAbsoluteFileRelToInput(inputFile, input.db.calciteModel);

        if (!modelFile.exists()) {
            throw new RuntimeException("Calcite model file not found at " + input.db.calciteModel);
        }

        model = modelFile.toString();
        calciteModel = dataMapper.readModel(modelFile);
    }

    private void validateModel() {

        if (calciteModel.schemas.size() != 1) {
            throw new RuntimeException("Expecting single schema in model file.");
        }

        CalciteModelSchema schema = calciteModel.schemas.get(0);

        isAnalyzerDBSchema = AnalyzerDbDiskSchemaFactory.class.getCanonicalName().equals(schema.factory)
                || AnalyzerDbCacheSchemaFactory.class.getCanonicalName().equals(schema.factory);
    }

    public void listQueries() {

        for (String queryId : input.queries.keySet()) {
            System.out.println(queryId);
            System.out.print("    ");
            System.out.println(input.queries.get(queryId));
        }
    }

    public void initiateCalciteConnection() {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");

            logger.info("Connecting to calcite with model: {}", model);

            Properties info = new Properties();
            info.setProperty("model", model);

            connection = DriverManager.getConnection("jdbc:calcite:", info);
            calciteConnection = connection.unwrap(CalciteConnection.class);

            SchemaPlus schema = calciteConnection.getRootSchema()
                    .getSubSchema(input.db.schema);

            if (schema == null) {
                throw new RuntimeException("Can not find schema named: " + input.db.schema);
            }

            schemaName = schema.getName();

            config = Frameworks
                    .newConfigBuilder()
                    .defaultSchema(schema)
                    .parserConfig(SqlParser.configBuilder().setCaseSensitive(false).build())
                    .build();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Can't find calcite driver; make sure the classpath is correct", e);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create connection", e);
        }
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }

        } catch (SQLException e) {
            logger.error("Failed to close connection");
            throw new RuntimeException(e);
        }
    }

    public void execute(String queryId) {

        if (isAnalyzerDBSchema) {
            throw new RuntimeException("Can not execute query on analyzer schema");
        }

        if (queryId != null && !queryId.isEmpty()) {
            executeAndPrintQuery(input.getQuery(queryId));
            return;
        }

        for (String q : input.queries.keySet()) {
            String query = input.queries.get(q);
            executeAndPrintQuery(query);
        }
    }

    protected void executeAndPrintQuery(String query) {
        logger.info("Running query {}", query);
        try (
                Statement statement = calciteConnection.createStatement();
                ResultSet rs = statement.executeQuery(query)
        ) {
            OutputPrinter.printResultSet(rs);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to run query: " + query, e);
        }
    }

    public void initiateSensitivityAnalyzer() {

        if (!isAnalyzerDBSchema) {
            sensitivityAnalyzer = new SensitivityAnalyzerBuilder()
                    .jdbcMetadataReader(schemaName, calciteConnection, input.db.sensitiveColumns)
                    .build();
            return;
        }

        String schemaDirectoryName = calciteModel.schemas.get(0).operand.get("directory");
        File schemaDirectory = new File(schemaDirectoryName);

        if (!schemaDirectory.isAbsolute()) {

            if (this.model.startsWith("inline:")) {
                throw new RuntimeException("For inline model the directory parameter must be an absolute path");
            }

            schemaDirectory = new File(new File(model).getParent(), schemaDirectoryName);
        }

        Map<String, TableStatistics> schemaStatistics = dataMapper.readSchemaStatistics(schemaDirectory);

        sensitivityAnalyzer = new SensitivityAnalyzerBuilder()
                .fixedMetadataReader(schemaStatistics, input.db.sensitiveColumns)
                .build();
    }

    public void writeStatistics(File outputDirectory, boolean overwrite) {
        final Map<String, TableStatistics> schemaStats = new HashMap<>();

        JdbcMetadataReader jdbcMetadataReader = new JdbcMetadataReader(schemaName, calciteConnection, schemaStats);
        jdbcMetadataReader.calculateTableStatistics();
        jdbcMetadataReader.addAdditionalSensitiveColumns(input.db.sensitiveColumns);

        dataMapper.writeSchemaStatistics(outputDirectory, jdbcMetadataReader.schemaStats, overwrite);
    }

    public void printRedisCommand() {
        final Map<String, TableStatistics> schemaStats = new HashMap<>();

        JdbcMetadataReader jdbcMetadataReader = new JdbcMetadataReader(schemaName, calciteConnection, schemaStats);
        jdbcMetadataReader.calculateTableStatistics();
        jdbcMetadataReader.addAdditionalSensitiveColumns(input.db.sensitiveColumns);

        Jedis redisClient = new MockJedis("test-redis");
        String keyPrefix = AnalyzerDbCacheSchemaFactory.DEFAULT_PREFIX;

        IndividualCacheMapper cacheMapper = new IndividualCacheMapper(redisClient, keyPrefix);
        cacheMapper.writeSchemaStats(schemaName, schemaStats);

        Set<String> keys = redisClient.keys(keyPrefix + "*");

        logger.info("Redis commands: ");
        for (String key : keys) {
            System.out.printf("set %s '%s'\n", key, redisClient.get(key));
        }
    }

    public void analyze(String queryId, boolean overwrite, boolean disablePlanner) {

        if (queryId != null && !queryId.isEmpty()) {
            generateAndPrintPlan(queryId, input.getQuery(queryId), overwrite, disablePlanner);
            return;
        }

        for (String q : input.queries.keySet()) {

            String query = input.queries.get(q);
            generateAndPrintPlan(q, query, overwrite, disablePlanner);
        }
    }

    private void generateAndPrintPlan(String queryId, String query, boolean overwrite, boolean disablePlanner) {

        logger.info("Analyzing query: {} -> {}", queryId, query.trim());

        try (Planner planner = Frameworks.getPlanner(config)) {
            SqlNode parsed = planner.parse(query);

            if (isEnabledSavePlan) {
                outputPrinter.saveContent(OutputPrinter.OutputType.PARSED_SQL, queryId, parsed.toString(), overwrite);
            }

            SqlNode validate = planner.validate(parsed);
            RelRoot relRoot = planner.rel(validate);

            RelNode analyzeRelNode = getAnalysisRootNode(disablePlanner, relRoot);

            if (isEnabledSavePlan) {
                String outputText = RelOptUtil.toString(analyzeRelNode);
                String outputXml = SDLPlanWriterUtils.toXML(analyzeRelNode);

                outputPrinter.saveContent(OutputPrinter.OutputType.PLAN_TEXT, queryId, outputText, overwrite);
                outputPrinter.saveContent(OutputPrinter.OutputType.PLAN_XML, queryId, outputXml, overwrite);

                String outputPaths = SDLPlanWriterUtils.toPaths(analyzeRelNode);
                outputPrinter.saveContent(OutputPrinter.OutputType.PATH_TEXT, queryId, outputPaths, overwrite);
            }

            SensitivityStatistics sensitivityStat = analyzeSensitivity(analyzeRelNode);
            logger.info(generateResultString(sensitivityStat));

        } catch (SqlParseException | ValidationException | RelConversionException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateResultString(SensitivityStatistics sensitivityStat) {
        StringBuilder sb = new StringBuilder("Estimated sensitive output rows: ");
        sb.append(sensitivityStat.estimatedRows);

        if (sensitivityStat.sensitiveColumns == null || sensitivityStat.sensitiveColumns.isEmpty()) {
            return sb.append("; No sensitive column in result.").toString();
        }

        sb.append("; Sensitive column and types - ");
        for (String columnName : sensitivityStat.sensitiveColumns.keySet()) {
            sb.append("[")
                    .append(columnName)
                    .append(": ")
                    .append(sensitivityStat.sensitiveColumns.get(columnName))
                    .append("], ");
        }

        sb.delete(sb.length() - 2, sb.length());
        return sb.toString();
    }

    private RelNode getAnalysisRootNode(boolean disablePlanner, RelRoot relRoot) {

        if (disablePlanner) {
            return relRoot.rel;
        }

        HepProgram program = HepProgram.builder().addRuleInstance(FilterJoinRule.FILTER_ON_JOIN).build();
        HepPlanner hepPlanner = new HepPlanner(program);

        hepPlanner.setRoot(relRoot.rel);
        return hepPlanner.findBestExp();
    }

    public SensitivityStatistics analyzeSensitivity(final RelNode rel) {
        if (rel == null) {
            return null;
        }

        return sensitivityAnalyzer.analyze(rel);
    }
}
