package ruta_express.ms_kafka_admin.exception;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

// errores HTTP de kafka-admin con ProblemDetail (contrato 5)
@RestControllerAdvice
public class GlobalExceptionHandler {

    // 409: el tópico ya existe
    @ExceptionHandler(TopicExistsException.class)
    public ProblemDetail handleTopicExists(TopicExistsException ex) {
        return problema(HttpStatus.CONFLICT, "Conflict", ex.getMessage());
    }

    // 404: el tópico no existe
    @ExceptionHandler(UnknownTopicOrPartitionException.class)
    public ProblemDetail handleTopicNotFound(UnknownTopicOrPartitionException ex) {
        return problema(HttpStatus.NOT_FOUND, "Not Found", ex.getMessage());
    }

    // 400: Kafka rechazó lo pedido (nombre, particiones, réplicas o configuración inválidos)
    @ExceptionHandler({ InvalidRequestException.class, InvalidTopicException.class, InvalidPartitionsException.class,
            InvalidReplicationFactorException.class, InvalidConfigurationException.class })
    public ProblemDetail handleKafkaBadRequest(ApiException ex) {
        return problema(HttpStatus.BAD_REQUEST, "Bad Request", ex.getMessage());
    }

    // 400: falla un @NotBlank / @Min / @NotEmpty del DTO
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        String detalle = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> e.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("Datos inválidos");
        return problema(HttpStatus.BAD_REQUEST, "Bad Request", detalle);
    }

    // 400: el cuerpo no es un JSON válido
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail handleUnreadable(HttpMessageNotReadableException ex) {
        return problema(HttpStatus.BAD_REQUEST, "Bad Request", "El cuerpo no es un JSON válido");
    }

    // 503: Kafka no respondió a tiempo (clúster caído o inalcanzable)
    @ExceptionHandler(TimeoutException.class)
    public ProblemDetail handleKafkaDown(TimeoutException ex) {
        return problema(HttpStatus.SERVICE_UNAVAILABLE, "Service Unavailable", "Kafka no disponible: " + ex.getMessage());
    }

    // 500: cualquier otro error
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneric(Exception ex) {
        return problema(HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error", "Error interno: " + ex.getMessage());
    }

    // problema: arma el ProblemDetail (Spring Framework 7 pide HttpStatusCode, no un int)
    private ProblemDetail problema(HttpStatus estado, String titulo, String detalle) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(estado, detalle);
        problem.setTitle(titulo);
        return problem;
    }
}
