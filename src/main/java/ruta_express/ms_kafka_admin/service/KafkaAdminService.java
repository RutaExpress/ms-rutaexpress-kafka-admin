package ruta_express.ms_kafka_admin.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;
import ruta_express.ms_kafka_admin.dto.TopicConfigUpdateDTO;
import ruta_express.ms_kafka_admin.dto.TopicRequestDTO;

import java.util.*;
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

    // 3. Describir un tópico
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

    // 5. Modificar configuración (retention.ms, cleanup.policy)
    public void updateTopicConfig(String topicName, TopicConfigUpdateDTO request) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        
        List<AlterConfigOp> ops = request.configs().entrySet().stream()
                .map(entry -> new AlterConfigOp(
                        new ConfigEntry(entry.getKey(), entry.getValue()), 
                        AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());

        Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(resource, ops);
        
        try {
            adminClient.incrementalAlterConfigs(configs).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new UnknownTopicOrPartitionException("Tópico no encontrado");
            }
            throw e;
        }
    }

    // 6. Listar Consumer Groups con sus offsets y lag
    public List<Map<String, Object>> listConsumerGroupsWithLag() throws ExecutionException, InterruptedException {
        List<Map<String, Object>> result = new ArrayList<>();
        
        // Obtenemos todos los grupos
        Collection<ConsumerGroupListing> groups = adminClient.listConsumerGroups().all().get();
        
        for (ConsumerGroupListing group : groups) {
            String groupId = group.groupId();
            
            // Obtenemos los offsets del grupo
            Map<TopicPartition, OffsetAndMetadata> groupOffsets = adminClient.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();
            
            if (groupOffsets.isEmpty()) continue;

            // Calculamos el end offset (el último mensaje en el tópico)
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            groupOffsets.keySet().forEach(tp -> request.put(tp, OffsetSpec.latest()));
            
            ListOffsetsResult endOffsetsResult = adminClient.listOffsets(request);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = endOffsetsResult.all().get();

            // Calculamos el lag por partición
            long totalLag = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long currentOffset = entry.getValue().offset();
                long endOffset = endOffsets.get(tp).offset();
                totalLag += Math.max(0, endOffset - currentOffset);
            }

            result.add(Map.of(
                "groupId", groupId,
                "state", group.state().toString(),
                "totalLag", totalLag
            ));
        }
        return result;
    }
}