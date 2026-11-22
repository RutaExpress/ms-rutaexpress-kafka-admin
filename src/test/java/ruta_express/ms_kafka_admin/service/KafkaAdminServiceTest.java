package ruta_express.ms_kafka_admin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import ruta_express.ms_kafka_admin.dto.TopicConfigUpdateDTO;
import ruta_express.ms_kafka_admin.dto.TopicRequestDTO;

// test unitario con Mockito del service que administra Kafka
// AdminClient es un mock: cada operación devuelve un "resultado" con un KafkaFuture (respuesta futura)
// que se programa ya terminado (bien o con error), así no hace falta un clúster real
@ExtendWith(MockitoExtension.class)
class KafkaAdminServiceTest {

    // dependencia simulada (cliente de administración de Kafka)
    @Mock
    private AdminClient adminClient;

    // resultados simulados de cada operación
    @Mock
    private CreateTopicsResult createResult;
    @Mock
    private DescribeTopicsResult describeResult;
    @Mock
    private AlterConfigsResult alterResult;

    // clase que se prueba, con el AdminClient inyectado por el constructor
    @InjectMocks
    private KafkaAdminService service;

    // fallida (futuro fallido): un KafkaFuture que ya terminó con error, como responde Kafka cuando algo sale mal
    private static <T> KafkaFuture<T> fallida(Throwable error) {
        KafkaFutureImpl<T> futuro = new KafkaFutureImpl<>();
        futuro.completeExceptionally(error);
        return futuro;
    }

    // test 1 (caso, sección 9): crear un tópico manda el nombre, las particiones y las réplicas pedidas
    @Test
    @DisplayName("Crear tópico: pide 3 particiones y 3 réplicas")
    @SuppressWarnings("unchecked")
    void createTopicSendsPartitionsAndReplicas() {
        // arrange (preparar)
        when(adminClient.createTopics(anyCollection())).thenReturn(createResult);
        when(createResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        // act (actuar)
        service.createTopic(new TopicRequestDTO("envios.prueba", 3, (short) 3));

        // assert (revisar): se le pidió a Kafka exactamente ese tópico
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(adminClient).createTopics(captor.capture());
        NewTopic pedido = captor.getValue().iterator().next();
        assertEquals("envios.prueba", pedido.name());
        assertEquals(3, pedido.numPartitions());
        assertEquals(3, pedido.replicationFactor());
    }

    // test 2 (caso de error): el tópico ya existe -> TopicExistsException SIN envolver (el controller responde 409)
    @Test
    @DisplayName("Crear tópico que existe: TopicExistsException desenvuelta")
    void createExistingTopicThrowsTopicExists() {
        // arrange (preparar): Kafka responde con error dentro del futuro
        when(adminClient.createTopics(anyCollection())).thenReturn(createResult);
        when(createResult.all()).thenReturn(fallida(new TopicExistsException("Topic 'shipments.events' already exists.")));

        // act + assert (actuar y revisar)
        assertThrows(TopicExistsException.class,
                () -> service.createTopic(new TopicRequestDTO("shipments.events", 3, (short) 3)));
    }

    // test 3 (caso de error): describir un tópico que no existe -> UnknownTopicOrPartitionException (404, antes 500)
    @Test
    @DisplayName("Describir tópico que no existe: UnknownTopicOrPartitionException")
    void describeUnknownTopicThrowsNotFound() {
        // arrange (preparar)
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.allTopicNames()).thenReturn(fallida(new UnknownTopicOrPartitionException("no existe")));

        // act + assert (actuar y revisar)
        assertThrows(UnknownTopicOrPartitionException.class, () -> service.describeTopic("no.existe"));
    }

    // test 4 (caso, sección 9): describir muestra particiones y factor de réplica
    @Test
    @DisplayName("Describir tópico: 3 particiones y 3 réplicas")
    void describeTopicShowsPartitionsAndReplicas() {
        // arrange (preparar): un tópico con 3 particiones, cada una copiada en los 3 brokers
        List<Node> brokers = List.of(new Node(1, "kafka-1", 9092), new Node(2, "kafka-2", 9093), new Node(3, "kafka-3", 9094));
        List<TopicPartitionInfo> particiones = List.of(
                new TopicPartitionInfo(0, brokers.get(0), brokers, brokers),
                new TopicPartitionInfo(1, brokers.get(1), brokers, brokers),
                new TopicPartitionInfo(2, brokers.get(2), brokers, brokers));
        TopicDescription descripcion = new TopicDescription("shipments.events", false, particiones);
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("shipments.events", descripcion)));

        // act (actuar)
        Map<String, Object> topico = service.describeTopic("shipments.events");

        // assert (revisar)
        assertEquals("shipments.events", topico.get("name"));
        assertEquals(3, topico.get("partitions"));
        assertEquals(3, topico.get("replicationFactor"));
    }

    // test 5 (caso, sección 9): modificar la configuración manda una operación SET por cada clave
    @Test
    @DisplayName("Modificar configuración: SET de retention.ms")
    @SuppressWarnings("unchecked")
    void updateConfigSendsSetOperation() {
        // arrange (preparar)
        when(adminClient.incrementalAlterConfigs(any())).thenReturn(alterResult);
        when(alterResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        // act (actuar): 7 días en milisegundos
        service.updateTopicConfig("shipments.events", new TopicConfigUpdateDTO(Map.of("retention.ms", "604800000")));

        // assert (revisar)
        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(adminClient).incrementalAlterConfigs(captor.capture());
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, "shipments.events");
        AlterConfigOp op = captor.getValue().get(recurso).iterator().next();
        assertEquals("retention.ms", op.configEntry().name());
        assertEquals("604800000", op.configEntry().value());
        assertEquals(AlterConfigOp.OpType.SET, op.opType());
    }

    // test 6 (EP5: lag de los consumer groups): lag = último offset del tópico - offset leído por el grupo, sumado
    @Test
    @DisplayName("Lag: suma de lo que falta por leer en cada partición")
    void lagIsSumOfPendingMessages() {
        // arrange (preparar): el grupo report leyó hasta 10 y 7; el tópico llega a 15 y 7
        TopicPartition p0 = new TopicPartition("shipments.events", 0);
        TopicPartition p1 = new TopicPartition("shipments.events", 1);
        Map<TopicPartition, OffsetAndMetadata> leidos = Map.of(p0, new OffsetAndMetadata(10L), p1, new OffsetAndMetadata(7L));
        Map<TopicPartition, Long> finales = Map.of(p0, 15L, p1, 7L);

        // act (actuar)
        long lag = KafkaAdminService.calcularLag(leidos, finales);

        // assert (revisar): 5 pendientes en la partición 0 y 0 en la 1
        assertEquals(5L, lag);
        assertTrue(lag >= 0);
    }
}
