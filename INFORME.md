# Informe: Trabajo Práctico - Coordinación

## 1. Introducción y Arquitectura General

El trabajo práctico implementa un sistema distribuido y escalable de agregación y cálculo del *Top-K* de frutas a partir de flujos de datos enviados por múltiples clientes. La arquitectura sigue una división basada en capas que se comunican a través de un Middleware Orientado a Mensajes (MOM), implementado utilizando RabbitMQ:

1. **Gateway (`MessageHandler`)**: Punto de contacto con los clientes externos vía TCP. Serializa y desmultiplexa los flujos asignando un identificador unívoco (`client_id` vía UUIDv4) a cada cliente, contabiliza los mensajes emitidos y reenvía la respuesta final al cliente correspondiente.
2. **SumFilter (`Sum`)**: Múltiples instancias consumen de una cola compartida (`INPUT_QUEUE`), acumulan localmente las cantidades por fruta para cada cliente y envían los subtotales hacia la instancia de aggregation correspondiente mediante *sharding* por hash.
3. **AggregationFilter (`Aggregation`)**: Cada instancia se encarga de un subconjunto distinto de frutas, consolida los subtotales recibidos de todas las instancias de `Sum`, calcula un top parcial mediante un *Min-Heap* y lo transfiere a la etapa de join.
4. **JoinFilter (`Join`)**: Recibe los tops parciales de todos los aggregators, combina las cantidades finales y genera el top global que es devuelto al Gateway.

La topología de mensajería se estructura de la siguiente manera:
- **Clientes $\leftrightarrow$ Gateway**: Comunicación bidireccional sobre sockets TCP externos.
- **Gateway $\rightarrow$ SumFilter**: Distribución de mensajes de datos y EOF a través de una cola compartida (`INPUT_QUEUE`).
- **SumFilter $\leftrightarrow$ SumFilter**: Comunicación de sincronización y consenso mediante un exchange de control (`SUM_CONTROL_EXCHANGE`).
- **SumFilter $\rightarrow$ AggregationFilter**: Envío particionado por clave de fruta a través de un exchange (`AGGREGATION_PREFIX`) con routing keys individuales por aggregator.
- **AggregationFilter $\rightarrow$ JoinFilter**: Envío de tops parciales consolidados hacia una cola de agregación (`OUTPUT_QUEUE`).
- **JoinFilter $\rightarrow$ Gateway**: Envío del top definitivo consolidado hacia la cola de resultados para su entrega al cliente.

---

## 2. Coordinación y Sincronización entre Instancias

### 2.1. Race Condition en la Detección de EOF para un Cliente

En un entorno distribuido donde múltiples réplicas de `SumFilter` consumen de una única cola compartida (`INPUT_QUEUE`), surge el siguiente problema de sincronización:
- El cliente envía un flujo de mensajes de datos y, al concluir, emite un mensaje de fin de transmisión (EOF).
- Dado que los mensajes se distribuyen de forma concurrente entre los nodos de `Sum`, el mensaje de EOF es consumido por **una única instancia** de `Sum`.
- Si dicha instancia concluyera inmediatamente su ejecución y notificara a los `AggregationFilter`, se generaría una race condition crítica: las restantes instancias de `Sum` podrían continuar procesando mensajes de datos rezagados o retenidos en sus buffers de red para ese mismo cliente.
- En consecuencia, los aggregators recibirían señales de finalización prematuras, cerrarían el cálculo y emitirían resultados incompletos y erróneos hacia el `JoinFilter`.

### 2.2. Solución Implementada: Sincronización mediante Conteo Global de Mensajes

Para solucionar esta race condition de manera determinista y robusta, diseñé una especie de "protocolo" de consenso y sincronización entre las instancias de `Sum` a través de un canal de control dedicado (`SUM_CONTROL_EXCHANGE`):

1. **Conteo en el Gateway**: Al serializar cada mensaje de un cliente, el `MessageHandler` mantiene un contador de mensajes enviados (`sent_messages`). Al finalizar la ingesta, envía en el payload del EOF dicho total: `[client_id, sent_messages]`.
2. **Rol de Coordinador del EOF**: La instancia de `Sum` que consume el EOF del Gateway asume el rol de *coordinador de finalización* para ese `client_id`, inicializando una estructura de seguimiento `received_eof[client_id] = (total_messages, 0)`.
3. **Sincronización de Pre-cierre**: El coordinador realiza un *broadcast* del EOF a todas las instancias de `Sum` a través del `SUM_CONTROL_EXCHANGE`.
4. **Reporte de Mensajes**: Cada instancia de `Sum` (incluyendo al coordinador), al recibir el aviso de control:
   - Marca al cliente en `client_ids_to_report`.
   - Envía inmediatamente los datos acumulados para ese cliente a los aggregators correspondientes.
   - Publica en el canal de control la cantidad de mensajes que procesó localmente para ese cliente (`_report_amount_of_received_messages`).
   - Si en el hilo principal aún llegan mensajes rezagados de ese cliente, estos se procesan y despachan de inmediato a los aggregators.
5. **Cálculo de Finalización**: El coordinador escucha los reportes de mensajes de todos los `Sum`. Cuando la suma acumulada de mensajes procesados iguala o supera el `total_messages` registrado en el EOF original, hay certeza de que **ningún mensaje quedó en camino ni en un buffer**.
6. **Cierre Sincronizado**: El coordinador emite por el canal de control el mensaje de finalización definitiva (`REPORT_EOF_TO_AGGREGATORS_FLAG`). Al recibirlo, cada instancia de `Sum` emite exactamente un mensaje de EOF hacia los `AggregationFilter`.


### 2.3. Análisis de Alternativas Consideradas

Durante el proceso de diseño evalué e implementé dos alternativas adicionales que finalmente descarté frente a la solución adoptada:

1. **Uso de `basic_qos(prefetch_count=1)` en el Middleware**:
   - *Mecanismo*: Limitar la ventana de pre-obtención de RabbitMQ a 1 mensaje sin confirmar por consumidor, evitando la acumulación de mensajes en los buffers locales.
   - *Desventajas*: El correcto funcionamiento de la lógica de negocio del sistema pasa a depender de la configuración interna del canal de comunicación (un detalle de infraestructura que no pertenece al dominio). Además, ralentiza considerablemente el flujo de mensajes a través del canal.
2. **Reencolamiento Sucesivo del Mensaje de EOF**:
   - *Mecanismo*: La instancia de `Sum` que consume el EOF anexa su identificador a una lista dentro del mensaje y lo reencola en `INPUT_QUEUE`. Las restantes instancias toman el mensaje, añaden su firma si ya procesaron sus datos y lo reencolan sucesivamente hasta que todas las réplicas hayan firmado.
   - *Desventajas*: Resulta sumamente ineficiente. Compite directamente con el tráfico de datos en la cola principal, y puede inducir latencias muy altas o hasta *starvation*.

### 2.4. Coordinación entre Aggregation y Join

- **"Barrera" de Sincronización en Aggregation**: Cada nodo `AggregationFilter` conoce la cantidad total de instancias de `Sum` configuradas (`SUM_AMOUNT`). Mantiene un contador `eof_by_client[client_id]` y procesa los datos incrementalmente. Únicamente cuando recibe exactamente `SUM_AMOUNT` mensajes de fin para un cliente, calcula su top local y lo envía al `JoinFilter`.
- **Consolidación en Join**: De forma análoga, el `JoinFilter` conoce la cantidad de instancias de aggregation (`AGGREGATION_AMOUNT`). Aguarda a recibir los tops parciales de todas las réplicas de aggregation antes de computar el Top-$K$ global y remitir el resultado definitivo al Gateway.

---

## 3. Optimización Algorítmica

### 3.1. Ineficiencia del Enfoque Original

En implementaciones basadas en listas ordenadas:
- Cada inserción o actualización requiere una búsqueda lineal o binaria ($O(N)$ o $O(\log N)$) seguida por un desplazamiento en memoria para mantener el orden ($O(N)$), resultando en una complejidad de $O(M \cdot N)$ para procesar $M$ mensajes de datos con $N$ frutas distintas.
- Si se opta por reordenar la lista completa al final mediante algoritmos de ordenamiento tradicionales, el costo es de $O(N \log N)$. Si la cantidad de claves distintas $N$ crece significativamente, este enfoque penaliza mucho el tiempo de respuesta.

### 3.2. Implementación con Hash y Min-Heap

Tanto en `AggregationFilter` como en `JoinFilter` desacoplé la fase de acumulación de datos de la fase de extracción del top:

1. **Acumulación ($O(1)$)**:
   Durante la llegada de mensajes, los ítems se acumulan en una tabla de hash (`dict` en Python) mapeando `fruta -> FruitItem`. La verificación e incremento se realiza en tiempo constante ($\mathcal{O}(1)$).

2. **Extracción del Top-$K$ mediante Min-Heap ($O(N \log K)$)**:
   Una vez alcanzada la barrera de sincronización (EOF), se utiliza la función `heapq.nlargest(TOP_SIZE, items)`, que internamente obtiene los K elementos mayores utilizando un Min-Heap en tiempo $O(N \log K)$. Dado que $K$ (`TOP_SIZE`) es una constante pequeña, $\log K$ es un factor mínimo y prácticamente constante, por lo que el cálculo del top pasa a ser lineal respecto a las frutas $\mathcal{O}(N)$ e independiente del volumen total de mensajes $M$.

---

## 4. Particionamiento y Distribución de Carga (Sharding por Hashing)

### 4.1. Estrategia de Sharding

Para lograr escalabilidad horizontal en la capa de aggregation, las instancias de `SumFilter` no realizan *broadcast* de cada fruta a todos los aggregators (provocando trabajo redundante). En su lugar, las claves (nombres de frutas) se particionan de manera determinista y envían a una determinada instancia:

$$\text{aggregator\_index} = \text{hash}(\text{fruta}) \pmod{\text{AGGREGATION\_AMOUNT}}$$

### 4.2. Elección del Algoritmo de Hashing: Adler-32

Para la función $\text{hash}()$ seleccioné **Adler-32** (`zlib.adler32`) sobre otras opciones:

- **Frente a SHA-256 (y otros algoritmos criptográficos)**:
  Algoritmos como SHA-256 o MD5 están diseñados para proveer seguridad y resistencia contra ataques. Para la distribución de datos desarrollada, su costo computacional es innecesario y representaría un cuello de botella en la ingesta.
- **Frente al `hash()` nativo de Python**:
  A partir de Python 3.3, la función built-in `hash()` incorpora randomización para mitigar ataques DoS. Esto significa que dos procesos independientes de Python (o dos contenedores de `SumFilter` distintos) obtendrían valores de hash diferentes para el mismo string `"manzana"`. Utilizar `hash()` rompería por completo el particionamiento, enrutando la misma fruta a aggregators distintos según qué nodo ejecute la suma.
- **Frente a CRC-32**:
  Si bien CRC-32 es no criptográfico y determinista, Adler-32 fue diseñado específicamente para ser significativamente más rápido en software.

---

## 5. Manejo de Señales `SIGTERM` y `SIGINT` de Forma Threadsafe

### 5.1. Manejo de Señales (`SIGTERM` / `SIGINT`)

Para garantizar que los contenedores puedan ser detenidos por Docker de forma controlada (`docker stop`):
- Registré handlers de señales para `SIGTERM` y `SIGINT` en todas las etapas (`SumFilter`, `AggregationFilter`, `JoinFilter`).
- El handler **no cierra las conexiones bruscamente ni finaliza el proceso de manera abrupta**. Su única responsabilidad es ordenar el cese del consumo (`stop_consuming`).
- El bucle principal de Pika (`start_consuming`) se desbloquea naturalmente, permitiendo que la ejecución ingrese a los bloques `finally` donde se cierran ordenadamente canales y sockets TCP, retornando con código de salida `0`.

### 5.2. Problemática de Pika y Diseño de `threadsafe_stop_consuming`

La arquitectura de `SumFilter` utiliza concurrencia mediante hilos:
- El hilo principal ejecuta el consumo de datos desde la cola `input_queue`.
- Un hilo secundario ejecuta el consumo de mensajes de sincronización desde `control_receiver`.

La biblioteca `pika` (`BlockingConnection`) **no es thread-safe**. Si el manejador de señales (que corre en el hilo principal) invoca directamente `control_receiver.stop_consuming()`, se produce una race condition interna sobre el canal del hilo secundario, generando excepciones y corrompiendo la conexión.

Para resolverlo, implementé en `_MessageMiddlewareRabbitMQ` el método `threadsafe_stop_consuming()`:

```python
def threadsafe_stop_consuming(self):
    self._stop_consuming(
        lambda: self.connection.add_callback_threadsafe(self.channel.stop_consuming)
    )
```

Al utilizar `add_callback_threadsafe`, la orden de interrupción se encola de forma segura en el event loop del hilo propietario de la conexión de control, garantizando una desconexión limpia y con liberación inmediata de recursos.

---

## 6. Escalabilidad del Sistema

El sistema aborda la escalabilidad en tres aspectos fundamentales:

### 6.1. Escalabilidad respecto a Clientes
- **Desmultiplexación por UUID**: Cada sesión o consulta iniciada en el Gateway recibe un identificador único global (`client_id`). Todos los mensajes de datos, de control y de EOF viajan con este identificador incorporado.
- **Aislamiento de Estado**: En todos los filtros (`Sum`, `Aggregation`, `Join`), las estructuras internas de acumulación y control se particionan por `client_id` (por ej., `amount_by_client_by_fruit[client_id]`). Esto permite que múltiples clientes transmitan en paralelo sin interferencia cruzada.
- **Recolección de Memoria**: Tan pronto como un cliente completa su ciclo y se emite su resultado, todas las estructuras asociadas a su `client_id` se eliminan explícitamente (`pop`), impidiendo *memory leaks* ante cargas de trabajo prolongadas.

### 6.2. Escalabilidad respecto a Grandes Volúmenes de Datos
- **Procesamiento en Streaming**: Los datos no se almacenan como colecciones de mensajes individuales ni en disco ni en listas en memoria; se procesan de forma incremental a medida que llegan.
- **Acumulación en Memoria**: La memoria utilizada por cada nodo no escala con la cantidad total de mensajes ($M$), sino únicamente con la cantidad de frutas únicas existentes ($N$).
- **Uso Eficiente del CPU**: La combinación de particionamiento por hash y cálculo de top mediante heaps reduce al mínimo la utilización de CPU por mensaje procesado.

### 6.3. Escalabilidad respecto a la Multiplicidad de Réplicas
- **Capa de Suma**: Al aumentar la cantidad de réplicas de `SumFilter`, RabbitMQ balancea automáticamente la carga de ingesta de la cola `INPUT_QUEUE`. El protocolo de control distribuido mediante conteo global escala de manera transparente para cualquier valor de `SUM_AMOUNT`.
- **Capa de Aggregation**: Al aumentar `AGGREGATION_AMOUNT`, la función de hash distribuye las frutas en un mayor número de nodos, reduciendo la memoria y el trabajo de cómputo en cada aggregator.
- **Configuración de Multiplicidad**: La multiplicidad de réplicas se define exclusivamente mediante variables de entorno (`SUM_AMOUNT`, `AGGREGATION_AMOUNT`, `TOP_SIZE`), sin requerir modificaciones en el código.

### 6.4. Escalabilidad vs Elasticidad
Es importante destacar que el sistema resultante es **escalable pero no elástico**:
- **Escalabilidad**: Posee la capacidad de tolerar mayores volúmenes de datos y clientes simplemente configurando una mayor cantidad de réplicas tanto en la etapa de suma como en la de agregación, distribuyendo el cómputo y el consumo de memoria de forma horizontal.
- **Falta de Elasticidad**: No es posible incorporar ni remover réplicas de manera dinámica durante la ejecución. Parámetros críticos como `SUM_AMOUNT` y `AGGREGATION_AMOUNT` son configurados como variables de entorno al inicializar los contenedores y se asumen constantes:
  - En `AggregationFilter`, la barrera de sincronización espera recibir exactamente `SUM_AMOUNT` mensajes de fin para cerrar el cliente. Modificar la cantidad de instancias de `Sum` en ejecución generaría bloqueos indefinidos o emisiones prematuras del top parcial.
  - En `JoinFilter`, se espera recibir exactamente `AGGREGATION_AMOUNT` tops parciales para consolidar el top global.
  - En `SumFilter`, el particionamiento de frutas se computa mediante `hash(fruta) % AGGREGATION_AMOUNT` sobre routing keys fijas. Variar dinámicamente `AGGREGATION_AMOUNT` alteraría el destino de las frutas, fragmentando los subtotales.

Por lo tanto, para escalar el sistema ante un incremento de demanda es necesario reconfigurar la cantidad de réplicas deseadas en el archivo de orquestación (`docker-compose.yaml`) e iniciar el entorno.

---

## 7. Ejecución

**Aclaración**: Los siguientes comandos deben ejecutarse dentro del directorio `python`.

- **Selección de Escenario de Prueba**:
  ```bash
  make switch
  ```
  Permite seleccionar entre los escenarios 1 al 5 definidos en la carpeta `scenarios/`:
  1. *Escenario 1*: Un cliente, réplicas unitarias.
  2. *Escenario 2*: Múltiples clientes concurrentes, réplicas unitarias.
  3. *Escenario 3*: Múltiples clientes, `Sum` replicado y un único `Aggregation`.
  4. *Escenario 4*: Múltiples clientes y múltiples réplicas en `Sum` y `Aggregation`.
  5. *Escenario 5*: Múltiples clientes, múltiples réplicas y nombres de contenedores aleatorios.

- **Iniciar Contenedores y Seguir Logs**:
  ```bash
  make up
  ```
  Buildea y arranca los servicios en segundo plano y redirige la salida combinada de todos los contenedores a la terminal.

- **Ejecutar Pruebas Automatizadas**:
  ```bash
  make test
  ```
  Inicia el entorno de Docker Compose, aguarda la finalización de los clientes, ejecuta `verify_output.py` para comparar las salidas contra la solución esperada y finaliza deteniendo los contenedores. Retorna código de salida `0` si todos los resultados coinciden.

- **Detener el Sistema**:
  ```bash
  make down
  ```
  Envía la señal de detención `SIGTERM` con un tiempo de gracia de 5 segundos (`stop -t 5`) y posteriormente remueve los contenedores y redes asociadas (`down`).
