# ms-rutaexpress-kafka-admin

Microservicio encargado de administrar el clúster de Kafka (crear, listar, describir y eliminar tópicos, así como monitorear consumer groups y su lag).

## 🚀 Tecnologías
- Java 21 / Spring Boot
- Spring for Apache Kafka (AdminClient)
- Maven

## ⚙️ Variables de Entorno
Revisar el archivo `.env.example`.

## 🔌 Endpoints Principales
- `POST /api/kafka/topics` (Crear tópico)
- `GET /api/kafka/topics` (Listar tópicos)
- `GET /api/kafka/topics/{name}` (Describir tópico)
- `DELETE /api/kafka/topics/{name}` (Eliminar tópico)
- `PUT /api/kafka/topics/{name}/config` (Modificar configuración, ej. retention.ms)
- `GET /api/kafka/consumer-groups` (Listar grupos y su lag)
- `GET /actuator/health` (Estado del microservicio para Docker)

## 🏗️ Cómo ejecutar en local
1. Asegurarse de tener el clúster de Kafka corriendo.
2. Ejecutar `mvn spring-boot:run`.
3. Consumir los endpoints vía `http://localhost:8088`.

## 🐳 Docker
```bash
docker build -t ms-rutaexpress-kafka-admin .