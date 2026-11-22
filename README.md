# ms-rutaexpress-kafka-admin

Microservicio de **administración de Kafka** del caso RutaExpress: ver y gestionar tópicos y revisar el lag de los consumer groups. Es una API de operación (sin pantalla); se usa con Postman.

> Arquitectura en capas (rama `refactor/ArquitecturaCapas`).

## Tecnologías
Java 21 · Spring Boot 4.1.2 · Kafka AdminClient · Bean Validation · Lombok · JUnit 5 y Mockito

## Arquitectura en capas
Una petición baja siempre en el mismo orden: **controller → service → AdminClient**.

```
src/main/java/ruta_express/ms_kafka_admin/
├── controller/   Solo HTTP: valida la entrada y llama al service
├── service/      Operaciones con AdminClient y cálculo del lag
├── dto/          Records de entrada y salida, con sus validaciones
├── exception/    Manejador global: 400, 404, 409 y 503 (Kafka no responde)
└── config/       AdminClient con los 3 brokers
```

## Endpoints (puerto 8088)
| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/api/kafka/topics` | Listar tópicos |
| GET | `/api/kafka/topics/{name}` | Particiones y réplicas de un tópico |
| POST | `/api/kafka/topics` | Crear tópico (201; ya existe → 409) |
| PUT | `/api/kafka/topics/{name}/config` | Cambiar configuración (por ejemplo `retention.ms`) |
| DELETE | `/api/kafka/topics/{name}` | Eliminar tópico |
| GET | `/api/kafka/consumer-groups` | Grupos, estado y lag total |

## Tests
```
mvnw.cmd test
```
