package main

// cada nodo se repite segun su cantidad de replicas; los *_AMOUNT salen de las
// replicas del nodo vecino al que le manda o del que espera los EOF
const composeTemplate = `# generado por make compose a partir de nodes.yaml, no editar a mano
name: tp-distribuidos

services:
  rabbitmq:
    build: ./src/rabbitmq
    ports:
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"]
      interval: 5s
      timeout: 10s
      retries: 12
      start_period: 10s
{{range $i := seq .Gateway}}
  gateway_{{$i}}:
    build:
      context: ./src
      dockerfile: gateway/Dockerfile
    environment:
      GATEWAY_ID: gateway_{{$i}}
      SERVER_PORT: "8080"
      OUTPUT_FILTER_EXCHANGE: records
      FILTER_PREFIX: filter
      FILTER_AMOUNT: "{{$.Filter}}"
      JOIN_AMOUNT: "{{$.Join}}"
      ABOVE_AVERAGE_AMOUNT: "{{$.AboveAverage}}"
      IDOWNLOADER_AMOUNT: "{{$.ImageDownloader}}"
      RESULT_BATCH_SIZE: "1048576"
      INPUT_RESULTS_EXCHANGE: results
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .Filter}}
  filter_{{$i}}:
    build:
      context: ./src
      dockerfile: filter/Dockerfile
    environment:
      INPUT_GATEWAY_EXCHANGE: records
      FILTER_PREFIX: filter
      FILTER_ID: "{{$i}}"
      OUTPUT_STOPWORDS_EXCHANGE: stopwords
      STOPWORDS_PREFIX: stopwords
      STOPWORDS_AMOUNT: "{{$.Stopwords}}"
      OUTPUT_IDOWNLOADER_EXCHANGE: q2
      IDOWNLOADER_PREFIX: q2
      IDOWNLOADER_AMOUNT: "{{$.ImageDownloader}}"
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
      OUTPUT_GATEWAY_EXCHANGE: results
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .ImageDownloader}}
  image_downloader_{{$i}}:
    build:
      context: ./src
      dockerfile: image_downloader/Dockerfile
    environment:
      INPUT_FILTER_EXCHANGE: q2
      IDOWNLOADER_PREFIX: q2
      IDOWNLOADER_ID: "{{$i}}"
      FILTER_AMOUNT: "{{$.Filter}}"
      IDOWNLOADER_WORKERS: "4"
      OUTPUT_GATEWAY_EXCHANGE: results
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
      USER_AGENT: "tp-distribuidos-fiuba/0.1 (ffialayre@fi.uba.ar)"
      HTTP_TIMEOUT_SECONDS: "20"
      HTTP_MAX_RETRIES: "3"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .Stopwords}}
  stopwords_{{$i}}:
    build:
      context: ./src
      dockerfile: stopwords/Dockerfile
    environment:
      INPUT_FILTER_EXCHANGE: stopwords
      STOPWORDS_PREFIX: stopwords
      STOPWORDS_ID: "{{$i}}"
      FILTER_AMOUNT: "{{$.Filter}}"
      OUTPUT_JOIN_EXCHANGE: join
      JOIN_PREFIX: join
      JOIN_AMOUNT: "{{$.Join}}"
      OUTPUT_ACCUMULATOR_EXCHANGE: accumulator
      ACCUMULATOR_PREFIX: accumulator
      ACCUMULATOR_AMOUNT: "{{$.Accumulator}}"
      OUTPUT_ABOVE_AVERAGE_EXCHANGE: above_average
      ABOVE_AVERAGE_PREFIX: above_average
      ABOVE_AVERAGE_AMOUNT: "{{$.AboveAverage}}"
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .Join}}
  join_{{$i}}:
    build:
      context: ./src
      dockerfile: join/Dockerfile
    environment:
      INPUT_STOPWORDS_EXCHANGE: join
      JOIN_PREFIX: join
      JOIN_ID: "{{$i}}"
      STOPWORDS_AMOUNT: "{{$.Stopwords}}"
      OUTPUT_GATEWAY_EXCHANGE: results
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .Accumulator}}
  accumulator_{{$i}}:
    build:
      context: ./src
      dockerfile: accumulator/Dockerfile
    environment:
      INPUT_STOPWORDS_EXCHANGE: accumulator
      ACCUMULATOR_PREFIX: accumulator
      ACCUMULATOR_ID: "{{$i}}"
      STOPWORDS_AMOUNT: "{{$.Stopwords}}"
      OUTPUT_REDUCER_EXCHANGE: reducer
      REDUCER_PREFIX: reducer
      REDUCER_AMOUNT: "{{$.Reducer}}"
      OUTPUT_JOIN_AVERAGE_EXCHANGE: join_average
      JOIN_AVERAGE_PREFIX: join_average
      JOIN_AVERAGE_AMOUNT: "{{$.JoinAverage}}"
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .Reducer}}
  reducer_{{$i}}:
    build:
      context: ./src
      dockerfile: reducer/Dockerfile
    environment:
      INPUT_ACCUMULATOR_EXCHANGE: reducer
      REDUCER_PREFIX: reducer
      REDUCER_ID: "{{$i}}"
      ACCUMULATOR_AMOUNT: "{{$.Accumulator}}"
      OUTPUT_SORTER_EXCHANGE: sorter
      SORTER_PREFIX: sorter
      SORTER_AMOUNT: "{{$.Sorter}}"
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .Sorter}}
  sorter_{{$i}}:
    build:
      context: ./src
      dockerfile: sorter/Dockerfile
    environment:
      INPUT_REDUCER_EXCHANGE: sorter
      SORTER_PREFIX: sorter
      SORTER_ID: "{{$i}}"
      REDUCER_AMOUNT: "{{$.Reducer}}"
      OUTPUT_GATEWAY_EXCHANGE: results
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .JoinAverage}}
  join_average_{{$i}}:
    build:
      context: ./src
      dockerfile: join_average/Dockerfile
    environment:
      INPUT_ACCUMULATOR_EXCHANGE: join_average
      JOIN_AVERAGE_PREFIX: join_average
      JOIN_AVERAGE_ID: "{{$i}}"
      ACCUMULATOR_AMOUNT: "{{$.Accumulator}}"
      OUTPUT_ABOVE_AVERAGE_EXCHANGE: average_result
      ABOVE_AVERAGE_PREFIX: above_average
      ABOVE_AVERAGE_AMOUNT: "{{$.AboveAverage}}"
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .AboveAverage}}
  above_average_{{$i}}:
    build:
      context: ./src
      dockerfile: above_average/Dockerfile
    environment:
      INPUT_STOPWORDS_EXCHANGE: above_average
      INPUT_AVERAGE_EXCHANGE: average_result
      ABOVE_AVERAGE_PREFIX: above_average
      ABOVE_AVERAGE_ID: "{{$i}}"
      STOPWORDS_AMOUNT: "{{$.Stopwords}}"
      OUTPUT_GATEWAY_EXCHANGE: results
      MOM_HOST: rabbitmq
      MOM_PORT: "5672"
    depends_on:
      rabbitmq:
        condition: service_healthy
{{end -}}
{{range $i := seq .Clients}}
  client_{{$i}}:
    build:
      context: ./src
      dockerfile: client/Dockerfile
    environment:
      GATEWAY_ADDRESSES: {{gatewayAddresses $.Gateway}}
      INPUT_DIR: /input/client_{{$i}}
      OUTPUT_DIR: /output/client_{{$i}}
      BATCH_SIZE: "100"
    volumes:
      - ./input:/input:ro
      - ./output:/output
    depends_on:
{{- range $g := seq $.Gateway}}
      - gateway_{{$g}}
{{- end}}
    restart: "no"
{{end -}}
`
