package ruta_express.ms_kafka_admin.dto;

public record TopicRequestDTO(
    String name,
    int partitions,
    short replicationFactor
) {}