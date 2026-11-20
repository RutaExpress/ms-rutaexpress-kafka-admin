package ruta_express.ms_kafka_admin.controller;

import org.springframework.web.bind.annotation.*;
import ruta_express.ms_kafka_admin.dto.TopicConfigUpdateDTO;
import ruta_express.ms_kafka_admin.dto.TopicRequestDTO;
import ruta_express.ms_kafka_admin.service.KafkaAdminService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @PostMapping("/topics")
    public String createTopic(@RequestBody TopicRequestDTO request) throws ExecutionException, InterruptedException {
        kafkaAdminService.createTopic(request);
        return "Tópico creado exitosamente";
    }

    @GetMapping("/topics")
    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        return kafkaAdminService.listTopics();
    }

    @GetMapping("/topics/{name}")
    public Map<String, Object> describeTopic(@PathVariable String name) throws ExecutionException, InterruptedException {
        return kafkaAdminService.describeTopic(name);
    }

    @DeleteMapping("/topics/{name}")
    public String deleteTopic(@PathVariable String name) throws ExecutionException, InterruptedException {
        kafkaAdminService.deleteTopic(name);
        return "Tópico eliminado exitosamente";
    }

    // NUEVO ENDPOINT: Modificar configuración
    @PutMapping("/topics/{name}/config")
    public String updateTopicConfig(@PathVariable String name, @RequestBody TopicConfigUpdateDTO request) throws ExecutionException, InterruptedException {
        kafkaAdminService.updateTopicConfig(name, request);
        return "Configuración del tópico actualizada";
    }

    // NUEVO ENDPOINT: Consumer Groups y Lag
    @GetMapping("/consumer-groups")
    public List<Map<String, Object>> listConsumerGroups() throws ExecutionException, InterruptedException {
        return kafkaAdminService.listConsumerGroupsWithLag();
    }
}