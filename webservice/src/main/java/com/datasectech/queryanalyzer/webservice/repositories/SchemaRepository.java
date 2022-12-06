package com.datasectech.queryanalyzer.webservice.repositories;

import com.datasectech.queryanalyzer.webservice.entities.SchemaEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource(
        path = "schema",
        itemResourceRel = "schema",
        collectionResourceRel = "schemas"
)
public interface SchemaRepository extends JpaRepository<SchemaEntity, Long> {

    @Query(value = "SELECT s FROM SchemaEntity s WHERE s.name = :name")
    SchemaEntity findByName(String name);
}
