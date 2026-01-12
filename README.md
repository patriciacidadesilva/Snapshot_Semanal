# 🐍 Snapshot Semanal (Full Snapshot) em Python — Databricks + Spark

## 📸 O que é um Snapshot?

Um **snapshot** é uma **fotografia completa do estado dos dados em um ponto específico no tempo**.

Em vez de atualizar ou sobrescrever registros existentes, o snapshot **congela todas as informações exatamente como elas estavam no momento da execução**, preservando o histórico.  
Cada execução gera uma nova versão da base, permitindo análises temporais, auditoria, rastreabilidade e comparações entre períodos.

### Em termos simples
- 📅 **Hoje**: os dados têm um estado atual  
- ⏱️ **Snapshot**: salva esse estado como uma foto  
- 🔁 **Semana seguinte**: nova foto, mesmo que os dados não tenham mudado  

Isso garante que **o passado nunca seja perdido**, mesmo que os dados de origem sejam corrigidos ou sobrescritos futuramente.

---

## 🤔 Quando usar Snapshot?

Snapshots são indicados quando existe necessidade de:

- 📊 Análise de **evolução histórica** (ex.: risco, inadimplência, carteira, status)
- 🕵️‍♀️ **Auditoria e compliance**
- 🧠 Comparação entre períodos (“como era” vs “como ficou”)
- 🧾 Evidência confiável para análises financeiras e regulatórias
- ⏳ Análises de tendência e séries temporais

---

## 🔁 Snapshot vs Incremental

| Estratégia | O que grava | Quando usar |
|-----------|------------|-------------|
| **Snapshot (Full)** | Todos os registros em toda execução | Histórico, governança, auditoria |
| Incremental | Apenas alterações (delta) | Performance, cargas operacionais |

📌 **Este projeto implementa Snapshot Semanal Full**, priorizando confiabilidade, rastreabilidade e governança dos dados.

---

## 🎯 Objetivo do Projeto

Demonstrar um **padrão de Engenharia de Dados** para criação de **Snapshot Semanal**, replicando **100% dos registros da tabela de origem a cada execução**, independentemente de alterações.

Esse padrão é amplamente utilizado em ambientes analíticos para:
- análises históricas confiáveis
- reconciliação de dados
- auditoria
- “time travel” analítico

**Cadência recomendada:** semanal (ex.: toda quinta-feira às 11h).

---

## 🏗️ Arquitetura (Visão Geral)

1. Leitura da tabela de origem (*source of truth*).
2. Criação de colunas técnicas de snapshot:
   - `Data_Ingestao_Congelamento`
   - `ID_Snapshot`
   - `Execucao_Snapshot`
   - `Versao_Snapshot`
3. Escrita na tabela destino em **modo append** (Delta Lake).
4. Aplicação de política de retenção de dados.

---

## 🧾 Metadados do Snapshot

A cada execução, os registros recebem:

- `Data_Ingestao_Congelamento` → timestamp do congelamento
- `ID_Snapshot` → identificador único da execução
- `Execucao_Snapshot` → data/hora da execução
- `Versao_Snapshot` → versão incremental do snapshot

Esses campos permitem reconstruir o estado da base em qualquer ponto do tempo.

---

## 🗄️ Retenção de Dados

Após a gravação do snapshot, é aplicada uma política de retenção, por exemplo:

- manter apenas os últimos **365 dias** de snapshots

Benefícios:
- controle de custo
- melhor performance
- histórico suficiente para análises anuais

---

## 🚀 Como Executar

### Execução manual
1. Abrir o notebook no Databricks
2. Iniciar o cluster
3. Executar todas as células

### Execução automatizada (produção)
Criar um **Databricks Job**:
- Tipo: Notebook Task
- Agendamento: semanal
- Cluster: job cluster ou cluster existente
- Parâmetros: `catalog` (se aplicável)

Isso garante **previsibilidade, governança e observabilidade**.

---

## 🔍 Observabilidade

Boas práticas recomendadas:
- validar crescimento do volume após cada execução
- logar `Versao_Snapshot`
- alertar quando nenhum novo snapshot for inserido

---

## 🔐 Boas Práticas de Segurança

- Nunca versionar senhas ou tokens no repositório
- Utilizar `dbutils.secrets.get(...)`
- Evitar logs com informações sensíveis
- Separar código de configuração sensível

---

## 📌 Possíveis Evoluções

- Particionamento por data de snapshot
- Tabela de auditoria de execuções
- Testes de qualidade antes da persistência
- Monitoramento automatizado

[📄 Ver o código completo do Snapshot em PySpark](https://github.com/patriciacidadesilva/Snapshot_Semanal/blob/main/Codigo_Completo_Snapshot_Semanal.py)

