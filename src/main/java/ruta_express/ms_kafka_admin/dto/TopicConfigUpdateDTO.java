package ruta_express.ms_kafka_admin.dto;

import java.util.Map;

public record TopicConfigUpdateDTO(
    Map<String, String> configs
) {}