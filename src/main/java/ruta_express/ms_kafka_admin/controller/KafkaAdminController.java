package ruta_express.ms_kafka_admin.controller;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import ruta_express.ms_kafka_admin.dto.MessageResponse;
import ruta_express.ms_kafka_admin.dto.TopicConfigUpdateDTO;
import ruta_express.ms_kafka_admin.dto.TopicRequestDTO;
import ruta_express.ms_kafka_admin.service.KafkaAdminService;

// endpoints de administración de Kafka (misma forma que /api/rabbitmq de rabbit-admin)
// no pasan por el API Gateway: el puerto 8088 solo se abre a la IP del administrador
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    // POST /topics: crea un tópico -> 201
    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public MessageResponse createTopic(@Valid @RequestBody TopicRequestDTO request) {
        kafkaAdminService.createTopic(request);
        return new MessageResponse("Tópico '" + request.name() + "' creado");
    }

    // GET /topics: nombres de los tópicos
    @GetMapping("/topics")
    public Collection<String> listTopics() {
        return kafkaAdminService.listTopics();
    }

    // GET /topics/{name}: particiones y réplicas
    @GetMapping("/topics/{name}")
    public Map<String, Object> describeTopic(@PathVariable String name) {
        return kafkaAdminService.describeTopic(name);
    }

    // DELETE /topics/{name}
    @DeleteMapping("/topics/{name}")
    public MessageResponse deleteTopic(@PathVariable String name) {
        kafkaAdminService.deleteTopic(name);
        return new MessageResponse("Tópico '" + name + "' eliminado");
    }

    // PUT /topics/{name}/config: cambia retention.ms, cleanup.policy, etc.
    @PutMapping("/topics/{name}/config")
    public MessageResponse updateTopicConfig(@PathVariable String name, @Valid @RequestBody TopicConfigUpdateDTO request) {
        kafkaAdminService.updateTopicConfig(name, request);
        return new MessageResponse("Configuración de '" + name + "' actualizada");
    }

    // GET /consumer-groups: grupos (report, audit) con su estado y lag
    @GetMapping("/consumer-groups")
    public List<Map<String, Object>> listConsumerGroups() {
        return kafkaAdminService.listConsumerGroupsWithLag();
    }
}
