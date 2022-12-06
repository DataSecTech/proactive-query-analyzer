package com.datasectech.queryanalyzer.webservice.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

public class ResponseHelper {

    public static ResponseEntity<?> buildErrorMessage(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(genMessageResponseMap(message, true));
    }

    public static ResponseEntity<?> buildResponseEntity(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(genMessageResponseMap(message, false));
    }

    protected static Map<String, String> genMessageResponseMap(String message, boolean error) {

        Map<String, String> messageMap = new HashMap<>();
        messageMap.put(error ? "errorMessage" : "message", message);

        return messageMap;
    }
}
