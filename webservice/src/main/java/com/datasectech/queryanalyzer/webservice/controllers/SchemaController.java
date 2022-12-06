package com.datasectech.queryanalyzer.webservice.controllers;

import com.datasectech.queryanalyzer.core.query.dto.TableStatistics;
import com.datasectech.queryanalyzer.webservice.entities.SchemaEntity;
import com.datasectech.queryanalyzer.webservice.entities.TableEntity;
import com.datasectech.queryanalyzer.webservice.repositories.TableRepository;
import com.datasectech.queryanalyzer.webservice.services.InMemoryStatisticsService;
import com.datasectech.queryanalyzer.webservice.services.SchemaService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.rest.webmvc.RepositoryRestController;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RepositoryRestController
public class SchemaController {
    protected TableRepository tableRepository;
    protected SchemaService schemaService;
    protected InMemoryStatisticsService inMemoryStatisticsService;
    protected ObjectMapper objectMapper;

    public SchemaController(
            TableRepository tableRepository, SchemaService schemaService,
            InMemoryStatisticsService inMemoryStatisticsService, ObjectMapper objectMapper
    ) {
        this.tableRepository = tableRepository;
        this.schemaService = schemaService;
        this.inMemoryStatisticsService = inMemoryStatisticsService;
        this.objectMapper = objectMapper;
    }

    @DeleteMapping("/schema/{schemaName}/table/{tableName}")
    public @ResponseBody ResponseEntity<?> deleteTable(
            @PathVariable String schemaName,
            @PathVariable String tableName
    ) {
        SchemaEntity schema = schemaService.findByNameOrFail(schemaName);
        TableEntity table = tableRepository.findByName(tableName, schema);

        if (table == null) {
            return ResponseHelper.buildErrorMessage(HttpStatus.NOT_FOUND, "Invalid request");
        }

        tableRepository.delete(table);

        return ResponseHelper.buildResponseEntity(HttpStatus.OK, "Deleted " + schemaName + "." + tableName);
    }

    @PutMapping("/schema/{schemaName}/table/{tableName}")
    public @ResponseBody ResponseEntity<?> updateTable(
            @PathVariable String schemaName,
            @PathVariable String tableName,
            @RequestBody TableStatistics tableStatistics
    ) throws JsonProcessingException {

        SchemaEntity schemaEntity = schemaService.findByNameOrFail(schemaName);

        if (tableStatistics.name == null || !tableStatistics.name.equals(tableName)) {
            return ResponseHelper.buildErrorMessage(HttpStatus.NOT_FOUND, "Table name doesn't match");
        }

        TableEntity tableEntity = tableRepository.findByName(tableName, schemaEntity);
        String tableStatisticsStr = objectMapper.writeValueAsString(tableStatistics);

        if (tableEntity == null) {
            tableEntity = new TableEntity();
            tableEntity.setName(tableName);
            tableEntity.setSchema(schemaEntity);
            tableEntity.setStatistics(tableStatisticsStr);
        } else {
            tableEntity.setStatistics(tableStatisticsStr);
        }

        tableEntity = tableRepository.save(tableEntity);

        inMemoryStatisticsService.checkOrUpdate(schemaEntity);

        return ResponseEntity.ok(tableEntity);
    }
}
