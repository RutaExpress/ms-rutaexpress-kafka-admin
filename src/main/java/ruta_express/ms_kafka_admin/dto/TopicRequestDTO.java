package ruta_express.ms_kafka_admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

// datos para crear un tópico; con @Valid en el controller, un dato inválido responde 400 sin llamar a Kafka
public record TopicRequestDTO(
        @NotBlank(message = "name es obligatorio") String name,
        @Min(value = 1, message = "partitions debe ser al menos 1") int partitions,
        @Min(value = 1, message = "replicationFactor debe ser al menos 1") short replicationFactor
) {}
