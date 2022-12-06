package com.datasectech.queryanalyzer.core.query.dto;

import java.util.ArrayList;
import java.util.List;

public class CalciteModel {
    public double version;
    public String defaultSchema;
    public List<CalciteModelSchema> schemas = new ArrayList<>();
}
