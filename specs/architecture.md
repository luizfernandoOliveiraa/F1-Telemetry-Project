

A estrutura do projeto deve contemplar os seguintes módulos e decisões de design:

1. ARQUITETURA DE REDE (INGESTÃO DE DADOS):
- Configurar um socket UDP assíncrono rodando em goroutine separado para escutar na porta 20777.
- Implementar buffers de leitura otimizados para evitar gargalos caso o jogo envie dados a 60Hz.
- A interface da aplicação (software via exe ou web) deve ter a opção de iniciar/encerrar sessão de caputra dos dados e ter os seguintes metadados informados na tela de gravação:
    - Track: Local onde a corrida/treino/qualify está/aconteceu.
    - Vehicle: Equipe/Marca do carro.
    - Type: Tipo de evento (corrida, treino, qualify, etc).
    - Laps: Quantidade de voltas registradas.
    - Best Lap: Melhor volta registrada.
    - Best Sectors: Melhores setores registrados.
    - Date: Data e hora do evento.

Ex de tela:
![alt text](image.png)

2. MAPEAMENTO COMPLETO DE ESTRUTURAS:
- Definir o PacketHeader padrão de 29 bytes, com máximo de 60 hz por segundo.
- Mapear a assinatura completa dos pacotes cruciais do F1 2025 baseado nas especificações da Codemasters, observados no documentos localizado em kb/data_specs_telemetry.docx, modelo de 2024 porem utilizado nas configurações para 2025.

3. ARQUITETURA DE ARMAZENAMENTO (PERSISTÊNCIA):
- Como os dados chegam em alto volume, separe a thread de recepção de rede da thread de gravação em disco.
- Quero implementar um buffer, se for necessário, para armazenamento dos dados em memória e depois envia-los atraves dessa aplicação para o kafka, que também deverá ser construido nessa aplicação. 
- Será necessário implementar um producer do kafka para enviar os dados para o tópico "f1-telemetry" e um consumer, que será um container do adl2 para ler os dados do tópico "f1-telemetry" e gravar os dados em nuvem, na minha conta de armazenamento azure. 
- Todo o processo dos dados de transito entre o local, kafka e azure deverá ser feito atraves de arquivos .parquet.


## Exemplo de arquitetura

+--------------------+
|   PS5 (F1 2025)    |  [Envio de Telemetria a 60Hz]
+--------------------+
|
| UDP (Porta 20777)
v
+--------------------+
|  UDP Ingestion     |  -> Escuta a porta local com buffers otimizados.
|  (Go Goroutine)    |
+--------------------+
|
| Go Channel (Bufferizado)
v
+--------------------+
|  Parser & Router   |  -> Decodifica os bytes estruturados (Little Endian).
|  (Go Goroutine)    |  -> Extrai metadados do Packet Header.
+--------------------+
|
| Kafka Producer (confluent-kafka-go / franz-go)
v
+--------------------+
|    Apache Kafka    |  -> Tópico centralizado: f1-telemetry-raw
+--------------------+
|
| Assíncrono (Batch Consumer)
v
+--------------------+
|  Azure ADLS Gen2   |  -> Agrupamento de dados em memória.
|  Sink (Go Engine)  |  -> Escrita em lote formatado (Parquet ou JSON Linhas).

