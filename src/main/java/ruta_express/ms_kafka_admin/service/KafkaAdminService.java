package ruta_express.ms_kafka_admin.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ruta_express.ms_kafka_admin.dto.TopicConfigUpdateDTO;
import ruta_express.ms_kafka_admin.dto.TopicRequestDTO;

// administración del clúster de Kafka: tópicos, configuraciones y consumer groups con su lag
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaAdminService {

    // dependencia: el cliente de administración que crea KafkaAdminConfig
    private final AdminClient adminClient;

    // createTopic (crear tópico)
    public void createTopic(TopicRequestDTO request) {
        NewTopic topico = new NewTopic(request.name(), request.partitions(), request.replicationFactor());
        esperar(adminClient.createTopics(List.of(topico)).all());
        log.info("[TOPICO-CREADO] {} particiones={} replicas={}", request.name(), request.partitions(), request.replicationFactor());
    }

    // listTopics (listar tópicos): nombres, sin los internos de Kafka
    public Collection<String> listTopics() {
        return esperar(adminClient.listTopics().names());
    }

    // describeTopic (describir tópico): particiones y factor de réplica
    public Map<String, Object> describeTopic(String name) {
        TopicDescription d = esperar(adminClient.describeTopics(List.of(name)).allTopicNames()).get(name);
        int replicas = d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
        return Map.of(
                "name", d.name(),
                "partitions", d.partitions().size(),
                "replicationFactor", replicas,
                "internal", d.isInternal());
    }

    // deleteTopic (eliminar tópico)
    public void deleteTopic(String name) {
        esperar(adminClient.deleteTopics(List.of(name)).all());
        log.info("[TOPICO-ELIMINADO] {}", name);
    }

    // updateTopicConfig (modificar configuración): un SET por clave (retention.ms, cleanup.policy...)
    public void updateTopicConfig(String name, TopicConfigUpdateDTO request) {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, name);
        List<AlterConfigOp> ops = request.configs().entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        esperar(adminClient.incrementalAlterConfigs(Map.of(recurso, ops)).all());
        log.info("[TOPICO-CONFIG] {} {}", name, request.configs());
    }

    // listConsumerGroupsWithLag (consumer groups con su lag): cuántos mensajes le faltan leer a cada grupo
    public List<Map<String, Object>> listConsumerGroupsWithLag() {
        List<Map<String, Object>> grupos = new ArrayList<>();
        for (ConsumerGroupListing grupo : esperar(adminClient.listConsumerGroups().all())) {
            Map<TopicPartition, OffsetAndMetadata> leidos =
                    esperar(adminClient.listConsumerGroupOffsets(grupo.groupId()).partitionsToOffsetAndMetadata());
            if (leidos.isEmpty()) {
                continue;
            }
            Map<TopicPartition, OffsetSpec> pedir = leidos.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ultimos = esperar(adminClient.listOffsets(pedir).all());
            Map<TopicPartition, Long> finales = ultimos.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
            grupos.add(Map.of(
                    "groupId", grupo.groupId(),
                    "state", grupo.groupState().map(Enum::name).orElse("DESCONOCIDO"),   // antes salía "Optional[STABLE]"
                    "totalLag", calcularLag(leidos, finales)));
        }
        return grupos;
    }

    // calcularLag (calcular retraso): por partición, último offset del tópico - offset leído por el grupo; se suman
    public static long calcularLag(Map<TopicPartition, OffsetAndMetadata> leidos, Map<TopicPartition, Long> finales) {
        long lag = 0;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : leidos.entrySet()) {
            long fin = finales.getOrDefault(e.getKey(), e.getValue().offset());
            lag += Math.max(0, fin - e.getValue().offset());
        }
        return lag;
    }

    // esperar: espera la respuesta de Kafka y, si falló, lanza el error REAL de Kafka (sin el ExecutionException
    // que lo envuelve); así el GlobalExceptionHandler lo traduce a 404, 409, 400 o 503
    private <T> T esperar(KafkaFuture<T> futuro) {
        try {
            return futuro.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof KafkaException errorDeKafka) {
                throw errorDeKafka;
            }
            throw new IllegalStateException("Error inesperado al hablar con Kafka", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();   // se respeta la interrupción del hilo
            throw new IllegalStateException("Operación con Kafka interrumpida", e);
        }
    }
}
