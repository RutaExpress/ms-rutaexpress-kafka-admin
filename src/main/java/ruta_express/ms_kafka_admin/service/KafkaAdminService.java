package ruta_express.ms_kafka_admin.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;
import ruta_express.ms_kafka_admin.dto.TopicRequestDTO;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaAdminService {

    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    // 1. Crear un tópico
    public void createTopic(TopicRequestDTO request) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(request.name(), request.partitions(), request.replicationFactor());
        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                throw new TopicExistsException("El tópico ya existe", e.getCause());
            }
            throw e;
        }
    }

    // 2. Listar tópicos
    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        return adminClient.listTopics().names().get();
    }

    // 3. Describir un tópico (ver sus particiones)
    public Map<String, Object> describeTopic(String name) throws ExecutionException, InterruptedException {
        TopicDescription description = adminClient.describeTopics(Collections.singletonList(name))
                .allTopicNames().get().get(name);
        
        if (description == null) {
            throw new UnknownTopicOrPartitionException("Tópico no encontrado");
        }

        return Map.of(
            "name", description.name(),
            "partitions", description.partitions().size(),
            "internal", description.isInternal()
        );
    }

    // 4. Eliminar un tópico
    public void deleteTopic(String name) throws ExecutionException, InterruptedException {
        try {
            adminClient.deleteTopics(Collections.singletonList(name)).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new UnknownTopicOrPartitionException("Tópico no encontrado");
            }
            throw e;
        }
    }
}