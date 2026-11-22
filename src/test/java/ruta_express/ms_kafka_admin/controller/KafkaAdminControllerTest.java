package ruta_express.ms_kafka_admin.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import ruta_express.ms_kafka_admin.service.KafkaAdminService;

// test de la capa web de kafka-admin: códigos HTTP y ProblemDetail (contrato 5)
@WebMvcTest(KafkaAdminController.class)
class KafkaAdminControllerTest {

    // simulador de peticiones HTTP
    @Autowired
    private MockMvc mockMvc;

    // dependencia simulada (servicio de administración)
    @MockitoBean
    private KafkaAdminService service;

    // test 1 (regla principal): crear un tópico válido -> 201 con mensaje
    @Test
    @DisplayName("POST /topics válido: 201")
    void createTopicGets201() throws Exception {
        mockMvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"envios.prueba\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.message").exists());
    }

    // test 2 (validación): 0 particiones -> 400 y no se llama a Kafka
    @Test
    @DisplayName("POST /topics con 0 particiones: 400 sin llamar a Kafka")
    void invalidTopicGets400() throws Exception {
        mockMvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"envios.prueba\",\"partitions\":0,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    // test 3 (caso de error): el tópico ya existe -> 409
    @Test
    @DisplayName("POST /topics que existe: 409")
    void existingTopicGets409() throws Exception {
        doThrow(new TopicExistsException("ya existe")).when(service).createTopic(any());

        mockMvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"shipments.events\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title").value("Conflict"));
    }

    // test 4 (caso de error): describir un tópico que no existe -> 404
    @Test
    @DisplayName("GET /topics/no.existe: 404")
    void unknownTopicGets404() throws Exception {
        when(service.describeTopic("no.existe")).thenThrow(new UnknownTopicOrPartitionException("no existe"));

        mockMvc.perform(get("/api/kafka/topics/no.existe"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Not Found"));
    }
}
