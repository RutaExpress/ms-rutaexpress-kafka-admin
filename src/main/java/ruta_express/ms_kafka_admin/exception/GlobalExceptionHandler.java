package ruta_express.ms_kafka_admin.exception;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(TopicExistsException.class)
    public ProblemDetail handleTopicExists(TopicExistsException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(409, ex.getMessage());
        problem.setTitle("Conflict");
        return problem;
    }

    @ExceptionHandler(UnknownTopicOrPartitionException.class)
    public ProblemDetail handleTopicNotFound(UnknownTopicOrPartitionException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(404, ex.getMessage());
        problem.setTitle("Not Found");
        return problem;
    }

    @ExceptionHandler(InvalidRequestException.class)
    public ProblemDetail handleBadRequest(InvalidRequestException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(400, ex.getMessage());
        problem.setTitle("Bad Request");
        return problem;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneric(Exception ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(500, "Error interno: " + ex.getMessage());
        problem.setTitle("Internal Server Error");
        return problem;
    }
}