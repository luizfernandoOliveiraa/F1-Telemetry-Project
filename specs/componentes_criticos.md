1. **UDP Receiver (Goroutine A):** Abre o socket UDP e executa apenas `ReadFromUDP`. Coloca os bytes brutos imediatamente em um canal bufferizado. Não faz parse, não valida dados. Overhead quase zero.
2. **Parser & Kafka Producer (Goroutine B):** Lê os bytes brutos do canal, faz o mapeamento de ponteiros binários usando `unsafe` ou leitura posicional binária para as structs do Go, valida o cabeçalho e publica no Kafka de forma assíncrona.
3. **ADLS Gen2 Storage Sink (Módulo Separado / Consumidor):** Um worker (ou microsserviço em Go separado) consome o tópico do Kafka, acumula os eventos em janelas de tempo/tamanho (ex: a cada 50.000 registros ou 1 minuto) e faz o upload em blocos para o Azure Blob Storage / ADLS Gen2. Isso otimiza o custo de transação na Azure e evita arquivos minúsculos (*small file problem*).

2. Estrutura de Diretórios do Projeto

Seguindo os padrões idiomáticos de arquitetura em Go (`golang-standards/project-layout`):

f1-Telemetry/
│
├── cmd/
│   └── collector/
│       └── main.go         # Ponto de entrada da aplicação
│
├── internal/
│   ├── models/
│   │   └── telemetry.go    # Estruturas C-Structs mapeadas para Go (Header, Lap, Motion)
│   │
│   ├── network/
│   │   └── udp_listener.go # Servidor UDP assíncrono de alta performance
│   │
│   ├── queue/
│   │   └── kafka_prod.go   # Produtor do Kafka otimizado para alto throughput
│   │
│   └── storage/
│       └── azure_adls.go   # Cliente Azure SDK para persistência em lote no Data Lake
│
├── go.mod                  # Módulo Go e dependências
└── go.sum