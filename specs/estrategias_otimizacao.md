Estratégias Otimizadas de Baixo Overhead (Go):

Reuso de Alocações (Zero GC Footprint): Em ambientes onde funções rodam milhares de vezes por segundo, evitar alocar novas structs ou buffers previne pausas de Garbage Collection do Go. O uso de sync.Pool para os slices de bytes raw recebidos do UDP é altamente encorajado à medida que novos pacotes forem acoplados ao switch-case.

Compressão LZ4 no Kafka: O tráfego de dados de telemetria possui um padrão repetitivo (altamente redundante). Ativar a compressão lz4 reduz o throughput de rede entre o coletor Go e o Kafka em até 70% com impacto quase nulo de processamento de CPU.

Partition Key Baseada no SessionUID: Ao usar o SessionUID enviado pelo PS5 como chave da mensagem no Kafka, o sistema garante que todas as voltas e dados daquela mesma corrida fiquem agrupados de forma estritamente cronológica e sequencial na mesma partição do cluster Kafka, facilitando leituras consistentes de séries temporais de ponta a ponta.