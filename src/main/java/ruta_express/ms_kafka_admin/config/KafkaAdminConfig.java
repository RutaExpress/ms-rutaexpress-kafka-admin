package ruta_express.ms_kafka_admin.config;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// crea el AdminClient (cliente de administración) conectado a los brokers de spring.kafka.bootstrap-servers
@Configuration
public class KafkaAdminConfig {

    // Spring cierra el AdminClient al apagar la app (llama a su método close)
    @Bean
    public AdminClient adminClient(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }
}
