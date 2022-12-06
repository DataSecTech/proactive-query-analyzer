package com.datasectech.queryanalyzer.webservice.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class ExceptionHandler extends ResponseEntityExceptionHandler {

    @org.springframework.web.bind.annotation.ExceptionHandler(value = {NotFoundException.class})
    protected ResponseEntity<?> handleConflict(NotFoundException ex) {
        return ResponseHelper.buildErrorMessage(HttpStatus.NOT_FOUND, ex.getMessage());
    }
}
