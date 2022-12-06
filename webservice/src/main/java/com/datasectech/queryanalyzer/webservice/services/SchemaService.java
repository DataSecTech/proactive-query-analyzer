package com.datasectech.queryanalyzer.webservice.services;

import com.datasectech.queryanalyzer.webservice.controllers.NotFoundException;
import com.datasectech.queryanalyzer.webservice.entities.SchemaEntity;
import com.datasectech.queryanalyzer.webservice.repositories.SchemaRepository;
import org.springframework.stereotype.Service;

@Service
public class SchemaService {

    protected SchemaRepository schemaRepository;

    public SchemaService(SchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

    public SchemaEntity findByNameOrFail(String schemaName) {
        SchemaEntity schema = schemaRepository.findByName(schemaName);

        if (schema == null) {
            throw new NotFoundException("Schema not found");
        }

        return schema;
    }
}
