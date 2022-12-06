package com.datasectech.queryanalyzer.webservice.repositories;

import com.datasectech.queryanalyzer.webservice.entities.SchemaEntity;
import com.datasectech.queryanalyzer.webservice.entities.TableEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.data.rest.core.annotation.RestResource;

@RepositoryRestResource(
        path = "table",
        itemResourceRel = "table",
        collectionResourceRel = "tables"
)
public interface TableRepository extends JpaRepository<TableEntity, Long> {

    @Query(value = "SELECT t FROM TableEntity t WHERE t.name = :name and t.schema = :schema")
    TableEntity findByName(String name, SchemaEntity schema);

    @Override
    @RestResource(exported = false)
    <S extends TableEntity> S save(S entity);

    @Override
    @RestResource(exported = false)
    void delete(TableEntity entity);
}
