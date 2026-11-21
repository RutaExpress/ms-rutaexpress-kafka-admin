package ruta_express.ms_kafka_admin.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ruta_express.ms_kafka_admin.dto.TopicRequestDTO;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaAdminServiceTest {

    @Mock(answer = org.mockito.Answers.RETURNS_DEEP_STUBS)
    private AdminClient adminClient;

    @InjectMocks
    private KafkaAdminService kafkaAdminService;

    @Test
    void createTopic_DebeLlamarAlAdminClient() throws Exception {
        // Arrange
        TopicRequestDTO request = new TopicRequestDTO("test-topic", 3, (short) 1);
        
        // Simulamos la respuesta de Kafka
        CreateTopicsResult mockResult = mock(CreateTopicsResult.class);
        when(adminClient.createTopics(any())).thenReturn(mockResult);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        // Act
        kafkaAdminService.createTopic(request);

        // Assert
        verify(adminClient, times(1)).createTopics(any());
    }

    @Test
    void listTopics_DebeRetornarListaDeTopicos() throws Exception {
        // Arrange
        KafkaFuture<java.util.Set<String>> future = KafkaFuture.completedFuture(Collections.singleton("topic-1"));
        when(adminClient.listTopics().names()).thenReturn(future);

        // Act
        var result = kafkaAdminService.listTopics();

        // Assert
        org.junit.jupiter.api.Assertions.assertTrue(result.contains("topic-1"));
    }
}