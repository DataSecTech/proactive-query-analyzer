package com.datasectech.queryanalyzer.webservice.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.persistence.*;

@Entity
@javax.persistence.Table(name = "sdl_table", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"name", "schema_id"}, name = "uk_schema_id_name")
}
)
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class TableEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @ManyToOne
    @JoinColumn(
            name = "schema_id",
            referencedColumnName = "id",
            nullable = false,
            foreignKey = @ForeignKey(name = "fk_schema_to_table")
    )
    @JsonIgnore
    private SchemaEntity schema;

    @Column(name = "statistics", columnDefinition = "TEXT")
    private String statistics;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SchemaEntity getSchema() {
        return schema;
    }

    public void setSchema(SchemaEntity schema) {
        this.schema = schema;
    }

    public String getStatistics() {
        return statistics;
    }

    public void setStatistics(String statistics) {
        this.statistics = statistics;
    }
}
