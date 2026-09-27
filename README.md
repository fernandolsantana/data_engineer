# data_engineer
mvp_data_engineer


Análise Estratégica da Balança Comercial Brasileira

Objetivo: Desenvolver um pipeline de Engenharia de Dados escalável no Databricks que ingira microdados aduaneiros brutos e instáveis, aplique governança e qualidade, e consolide um modelo dimensional capaz de fornecer respostas estratégicas precisas para a alta gestão macroeconômica.

**1. Contexto de negócio e Perguntas (Etapa 2 e 4.1)**

O Brasil exerce protagonismo no comércio global, contudo, a análise de sua balança comercial frequentemente restringe-se a volumes brutos de superávit ou déficit, ofuscando vulnerabilidades estruturais. O problema central que este projeto endereça é a ausência de visibilidade granular sobre a matriz de dependência externa em setores críticos, a assimetria regional na adoção de tecnologias de transição energética e a real complexidade da pauta exportadora (commodities versus manufaturados).

A arquitetura de dados foi desenhada para responder a três questionamentos estratégicos fundamentais:

**Vulnerabilidade da Cadeia de Suprimentos**: Qual é o nível de concentração e dependência geopolítica do Brasil no fornecimento de insumos críticos (Insumos Agrícolas, Fármacos/Equipamentos Médicos, Minérios/Energia, Química Fina/Polímeros e Tecnologia)?

**Distribuição da Transição Energética e Inovação**: Quais unidades federativas centralizam os aportes (via importação) em infraestrutura de eletrificação, geração renovável e componentes inteligentes?

**Qualidade da Balança Comercial**: Analisando os 10 maiores parceiros comerciais do país, qual é o saldo financeiro real e o grau de dependência da exportação de produtos primários (commodities) em detrimento de bens de maior valor agregado?

**Estrutura da Origem dos Dados**
Os microdados foram extraídos da plataforma governamental Comex Stat, estruturados em:

Tabelas Fato (Transacionais): EXP_2024.csv, EXP_2025.csv, IMP_2024.csv, IMP_2025.csv. Registram a granularidade máxima de cada operação aduaneira (ano, mês, NCM, país, UF, via logística, peso e valor FOB em dólares).

Tabelas de Dimensão (Dicionários): NCM.csv, PAIS.csv, UF.csv, VIA.csv. Fornecem o contexto descritivo das chaves estrangeiras, funcionando como um dicionário para traduzir os registros em códigos numéricos presentes nas Tabelas Fato.

Licenciamento: Domínio público, disponibilizado em formato de Dados Abertos pelo Ministério do Desenvolvimento, Indústria, Comércio e Serviços (MDIC). (https://comexstat.mdic.gov.br/pt/home)

**2. Carga dos Dados (Etapa 4.2)**

A extração ocorreu mediante aquisição direta dos arquivos .CSV no repositório do Comex Stat. Para a etapa de carga, foi empregado o Databricks.

**3. Modelagem e Catálogo de Dados (Etapa 4.3)**

O fluxo ETL (Extract, Transform, Load) foi consolidado em um único Notebook PySpark no Databricks, arquitetado sob o paradigma Medallion Architecture, ramificado metodologicamente em três estágios lógicos de processamento:

Camada Bronze (Raw): Ingestão escalável dos arquivos CSV a partir do Volume, unionByName das séries temporais anuais e inserção da flag operacional (TIPO_OPERACAO). Persistência no formato colunar Delta Lake.

Camada Silver (Cleansed): Execução de data profiling, saneamento de anomalias, type casting rigoroso de variáveis métricas e higienização de nulos.

Camada Gold (Curated): Modelagem analítica baseada no Star Schema. Execução de LEFT JOINs entre a Tabela Fato e os Dicionários, resultando em uma One Big Table desnormalizada, indexada e otimizada para o consumo das regras de negócio via SQL.

<img width="362" height="325" alt="tabelas-mvp" src="https://github.com/user-attachments/assets/64e4ace4-8b45-4417-a666-65f078747040" />

A modelagem dimensional eliminou a opacidade dos códigos alfanuméricos aduaneiros (NCM), entregando semântica de negócios imediata na Camada Gold, conforme estruturado no dicionário abaixo.

Catálogo de Dados - Tabela gold_comex_analitica


