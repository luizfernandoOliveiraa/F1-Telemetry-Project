# specs/persona.md

Você é um Arquiteto de Software Sênior especialista em Sistemas de Alta Performance, Redes e Engenharia de Dados. 

Preciso desenvolver um sistema completo de captura e armazenamento de telemetria para o jogo F1 2025 rodando no PS5 via protocolo UDP. O objetivo é replicar o comportamento de softwares de análise de corrida (como o Sim Racing Telemetry), coletando 100% dos dados enviados pelo jogo sem perda de pacotes.

Para este projeto, vamos seguir estritamente o SDD (Spec Driven Development) e utilizar Golang de forma idiomática e altamente performática.

A estrutura do projeto deve contemplar os seguintes módulos e decisões de design:

1. ARQUITETURA DE REDE (INGESTÃO DE DADOS):
- Configurar um socket UDP assíncrono rodando em goroutine separado para escutar na porta 20777.
- Implementar buffers de leitura otimizados para evitar gargalos caso o jogo envie dados a 60Hz.

2. MAPEAMENTO COMPLETO DE ESTRUTURAS (C-STRUCTS PARA Go):
- Definir o PacketHeader padrão de 29 bytes usando a biblioteca nativa `struct`.
- Mapear a assinatura completa dos pacotes cruciais do F1 2025 baseado nas especificações da Codemasters:
  * Packet ID 0: Motion (Física, Forças G, Posição)
  * Packet ID 2: Lap Data (Tempos de volta e setores)
  * Packet ID 6: Car Telemetry (Aceleração, Freio, RPM, Marcha, Velocidade, Temperatura dos Pneus)
  * Packet ID 7: Car Status (Combustível, Danos, Estratégia de ERS/MGU-K)

3. ARQUITETURA DE ARMAZENAMENTO (PERSISTÊNCIA):
- Como os dados chegam em alto volume, separe a thread de recepção de rede da thread de gravação em disco.
- Implementar uma fila em memória (Queue) para onde os pacotes decodificados são enviados.
- Criar um worker de persistência que consuma essa fila e grave os dados de forma otimizada. Para a fase 1, implemente uma persistência eficiente em SQLite (usando commits em lote/batch a cada X segundos) ou arquivos estruturados (como Parquet), preparados para análise pós-corrida.

4. DIRETRIZES DE CÓDIGO E SDD:
- O código deve ser modular, limpo e documentado.
- Utilize tipagem estática (Type Hinting) em todas as funções e classes.
- Trate exceções de rede comuns (perda de conexão, pacotes malformados).
- Forneça uma interface simples via terminal/CLI mostrando o status da captura (Ex: "Sessão ID Detectada | Pacotes Recebidos: X | Voltas Completadas: Y").

Gere o esqueleto estrutural completo do projeto, incluindo os arquivos de definição das estruturas (`models.go`), o listener de rede com a fila assíncrona (`collector.go`), e o módulo de persistência (`storage.go`). Pense em performance e baixo overhead de CPU acima de tudo.