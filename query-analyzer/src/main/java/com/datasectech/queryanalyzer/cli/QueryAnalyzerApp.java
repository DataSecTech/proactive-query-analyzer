package com.datasectech.queryanalyzer.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;

@Command(
        name = "QueryAnalyzer",
        mixinStandardHelpOptions = true,
        description = "Analyze and save statistics of calcite databases"
)
public class QueryAnalyzerApp {

    @Command(name = "list", description = "List queries from input file")
    int list(
            @Option(names = {"-i", "--in-file"}, description = "Input file with Calcite configuration and query", required = true) File inputFile
    ) {
        new QueryAnalyzer(inputFile).listQueries();
        return 0;
    }

    @Command(name = "analyze", description = "Perform sensitivity analysis on input queries")
    int analyze(
            @Option(names = {"-i", "--in-file"}, description = "Input file with Calcite configuration and query", required = true) File inputFile,
            @Option(names = {"-q", "--query-id"}, description = "Specific query to run, if not provided all queries are selected") String queryId,
            @Option(names = {"-w", "--overwrite"}, description = "Overwrite existing generated files.") boolean overwrite,
            @Option(names = {"-d", "--disable-planner"}, description = "Disable planner.") boolean disablePlanner
    ) {
        try (QueryAnalyzer queryAnalyzer = new QueryAnalyzer(inputFile)) {

            queryAnalyzer.initiateCalciteConnection();
            queryAnalyzer.initiateSensitivityAnalyzer();
            queryAnalyzer.analyze(queryId, overwrite, disablePlanner);
        }

        return 0;
    }

    @Command(name = "exec", description = "Perform sensitivity analysis on input queries")
    int exec(
            @Option(names = {"-i", "--in-file"}, description = "Input file with Calcite configuration and query", required = true) File inputFile,
            @Option(names = {"-q", "--query-id"}, description = "Specific query to run, if not provided all queries are selected") String queryId
    ) {
        try (QueryAnalyzer queryAnalyzer = new QueryAnalyzer(inputFile)) {

            queryAnalyzer.initiateCalciteConnection();
            queryAnalyzer.execute(queryId);
        }

        return 0;
    }

    @Command(name = "save-stats", description = "Save analyzer statistics of current db")
    int saveStatistics(
            @Option(names = {"-i", "--in-file"}, description = "Input file with Calcite configuration and query", required = true) File inputFile,
            @Option(names = {"-r", "--redis-command"}, description = "Print redis command to save into redis") boolean redis,
            @Option(names = {"-o", "--out-dir"}, description = "Output directory to save statistics json files") File outDir,
            @Option(names = {"-w", "--overwrite"}, description = "Overwrite existing generated files.") boolean overwrite
    ) {
        try (QueryAnalyzer queryAnalyzer = new QueryAnalyzer(inputFile)) {

            queryAnalyzer.initiateCalciteConnection();

            if (!redis && outDir == null) {
                throw new RuntimeException("Must provide `--redis-command` or `--out-dir` options");
            }

            if (redis) {
                queryAnalyzer.printRedisCommand();
            }

            if (outDir != null) {
                queryAnalyzer.writeStatistics(outDir, overwrite);
            }
        }

        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new QueryAnalyzerApp()).execute(args);
        System.exit(exitCode);
    }
}
