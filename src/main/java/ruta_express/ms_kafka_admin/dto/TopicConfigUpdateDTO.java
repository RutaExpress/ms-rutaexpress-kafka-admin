package ruta_express.ms_kafka_admin.dto;

import java.util.Map;

import jakarta.validation.constraints.NotEmpty;

// configuraciones a cambiar en un tópico, por ejemplo {"retention.ms": "604800000", "cleanup.policy": "delete"}
public record TopicConfigUpdateDTO(
        @NotEmpty(message = "configs debe traer al menos una clave") Map<String, String> configs
) {}
