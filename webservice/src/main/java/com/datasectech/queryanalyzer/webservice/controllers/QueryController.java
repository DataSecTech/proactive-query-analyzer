package com.datasectech.queryanalyzer.webservice.controllers;

import com.datasectech.queryanalyzer.webservice.dto.QueryList;
import com.datasectech.queryanalyzer.webservice.dto.ResultList;
import com.datasectech.queryanalyzer.webservice.services.InMemoryStatisticsService;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;

@RestController
public class QueryController {
    protected InMemoryStatisticsService inMemoryStatisticsService;

    public QueryController(InMemoryStatisticsService inMemoryStatisticsService) {
        this.inMemoryStatisticsService = inMemoryStatisticsService;
    }

    @PostMapping("/query/batch/{schemaName}")
    public ResultList batchQuery(@PathVariable String schemaName, @RequestBody QueryList queryList) {
        return inMemoryStatisticsService.analyzeQueries(schemaName, queryList);
    }

    @GetMapping("/query/{schemaName}")
    public ResultList query(@PathVariable String schemaName, @RequestParam String query) {

        QueryList queryList = new QueryList();
        queryList.queries = Collections.singletonMap("query", query);

        return inMemoryStatisticsService.analyzeQueries(schemaName, queryList);
    }
}
