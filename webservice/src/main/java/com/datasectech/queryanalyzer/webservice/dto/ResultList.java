package com.datasectech.queryanalyzer.webservice.dto;

import com.datasectech.queryanalyzer.core.query.dto.SensitivityStatistics;

import java.util.HashMap;
import java.util.Map;

public class ResultList {
    public ResultSummary summary;
    public Map<String, SensitivityStatistics> success;
    public Map<String, String> errors;

    public ResultList() {
        summary = new ResultSummary();
        success = new HashMap<>();
        errors = new HashMap<>();
    }
}
