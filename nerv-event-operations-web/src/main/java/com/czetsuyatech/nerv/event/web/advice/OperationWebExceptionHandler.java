package com.czetsuyatech.nerv.event.web.advice;

import com.czetsuyatech.nerv.event.exception.OperationRecordNotFoundException;
import com.czetsuyatech.nerv.event.exception.ManualRetryRejectedException;
import com.czetsuyatech.nerv.event.web.controller.InboxOperationController;
import com.czetsuyatech.nerv.event.web.controller.OutboxOperationController;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Instant;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Stable, payload-free error responses scoped to the optional operations controllers.
 */
@RestControllerAdvice(assignableTypes = {OutboxOperationController.class, InboxOperationController.class})
public class OperationWebExceptionHandler {

  @ExceptionHandler(OperationRecordNotFoundException.class)
  ResponseEntity<OperationsErrorResponse> notFound(
      OperationRecordNotFoundException exception,
      HttpServletRequest request
  ) {
    return error(
        HttpStatus.NOT_FOUND,
        "NERV_EVENT_NOT_FOUND",
        exception.getMessage(),
        request
    );
  }

  @ExceptionHandler(ManualRetryRejectedException.class)
  ResponseEntity<OperationsErrorResponse> retryConflict(
      ManualRetryRejectedException exception,
      HttpServletRequest request
  ) {
    return error(
        HttpStatus.CONFLICT,
        "NERV_EVENT_RETRY_CONFLICT",
        exception.getMessage(),
        request
    );
  }

  @ExceptionHandler({IllegalArgumentException.class, MethodArgumentTypeMismatchException.class})
  ResponseEntity<OperationsErrorResponse> invalidQuery(
      Exception exception,
      HttpServletRequest request
  ) {
    return error(
        HttpStatus.BAD_REQUEST,
        "NERV_EVENT_INVALID_QUERY",
        exception.getMessage(),
        request
    );
  }

  @ExceptionHandler(Exception.class)
  ResponseEntity<OperationsErrorResponse> internalError(
      Exception exception,
      HttpServletRequest request
  ) {
    return error(
        HttpStatus.INTERNAL_SERVER_ERROR,
        "NERV_EVENT_INTERNAL_ERROR",
        "Unexpected operations request failure",
        request
    );
  }

  private static ResponseEntity<OperationsErrorResponse> error(
      HttpStatus status,
      String code,
      String message,
      HttpServletRequest request
  ) {
    return ResponseEntity.status(status)
        .body(
            new OperationsErrorResponse(
                code,
                message,
                Instant.now(),
                request.getRequestURI()
            )
        );
  }

  public record OperationsErrorResponse(
      String code,
      String message,
      Instant timestamp,
      String path
  )
  {
  }
}
