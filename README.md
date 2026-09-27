# data_engineer
mvp_data_engineer

**Nome**: Fernando Lopes Santana
**Matrícula**: 40530010057_20260_01
**Disciplina/Sprint**: Engenharia de Dados

**Análise Estratégica da Balança Comercial Brasileira**

**Objetivo**: Desenvolver um pipeline de Engenharia de Dados escalável no Databricks que ingira microdados aduaneiros brutos e instáveis, aplique governança e qualidade, e consolide um modelo dimensional capaz de fornecer respostas estratégicas precisas para a alta gestão macroeconômica.

**1. Contexto de negócio e Perguntas (Etapa 2 e 4.1)**

O Brasil exerce protagonismo no comércio global, contudo, a análise de sua balança comercial frequentemente restringe-se a volumes brutos de superávit ou déficit, ofuscando vulnerabilidades estruturais. O problema central que este projeto endereça é a ausência de visibilidade granular sobre a matriz de dependência externa em setores críticos, a assimetria regional na adoção de tecnologias de transição energética e a real complexidade da pauta exportadora (commodities versus manufaturados).

A arquitetura de dados foi desenhada para responder a três questionamentos estratégicos fundamentais:

**Vulnerabilidade da Cadeia de Suprimentos**: Qual é o nível de concentração e dependência geopolítica do Brasil no fornecimento de insumos críticos (Insumos Agrícolas, Fármacos/Equipamentos Médicos, Minérios/Energia, Química Fina/Polímeros e Tecnologia)?

**Distribuição da Transição Energética e Inovação**: Quais unidades federativas centralizam os aportes (via importação) em infraestrutura de eletrificação, geração renovável e componentes inteligentes?

**Qualidade da Balança Comercial**: Analisando os 10 maiores parceiros comerciais do país, qual é o saldo financeiro real e o grau de dependência da exportação de produtos primários (commodities) em detrimento de bens de maior valor agregado?

**1.1. Estrutura da Origem dos Dados**
Os microdados foram extraídos da plataforma governamental Comex Stat, estruturados em:

Tabelas Fato (Transacionais): EXP_2024.csv, EXP_2025.csv, IMP_2024.csv, IMP_2025.csv. Registram a granularidade máxima de cada operação aduaneira (ano, mês, NCM, país, UF, via logística, peso e valor FOB em dólares).

Tabelas de Dimensão (Dicionários): NCM.csv, PAIS.csv, UF.csv, VIA.csv. Fornecem o contexto descritivo das chaves estrangeiras, funcionando como um dicionário para traduzir os registros em códigos numéricos presentes nas Tabelas Fato.

Licenciamento: Domínio público, disponibilizado em formato de Dados Abertos pelo Ministério do Desenvolvimento, Indústria, Comércio e Serviços (MDIC). (https://comexstat.mdic.gov.br/pt/home)

**2. Carga dos Dados (Etapa 4.2)**

A extração ocorreu mediante aquisição direta dos arquivos .CSV no repositório do Comex Stat. Para a etapa de carga, foi empregado o Databricks. A extração primária ocorreu mediante aquisição direta de arquivos (.CSV) no portal governamental Comex Stat. Para a etapa de carga e processamento, a arquitetura foi desenhada na nuvem utilizando o Databricks.

Os arquivos brutos foram carregados nativamente para o armazenamento distribuído da plataforma, sendo alocados em um Volume seguro do Unity Catalog (`/Volumes/mvp/mvp_data_engineering/mvp_comex/`). Esta abordagem evitou a dependência de ferramentas externas de ingestão (Data Ingestion tools) para este MVP, garantindo que o processamento PySpark acessasse os dados localmente com alta performance.

<img width="1128" height="544" alt="image" src="https://github.com/user-attachments/assets/d59dea2a-1beb-4d43-b26e-42987098b6dc" />

**3. Modelagem e Catálogo de Dados (Etapa 4.3)**

**3.1. Modelagem**

O fluxo ETL (Extract, Transform, Load) foi consolidado em um único Notebook PySpark no Databricks, arquitetado sob o paradigma Medallion Architecture, ramificado metodologicamente em três estágios lógicos de processamento:

Camada Bronze (Raw): Ingestão escalável dos arquivos CSV a partir do Volume, unionByName das séries temporais anuais e inserção da flag operacional (TIPO_OPERACAO). Persistência no formato colunar Delta Lake.

Camada Silver (Cleansed): Execução de data profiling, saneamento de anomalias, type casting rigoroso de variáveis métricas e higienização de nulos.

Camada Gold (Curated): As camadas Bronze e Silver preservam a separação relacional entre fato e dimensão (estrutura próxima ao Star Schema). Contudo, para otimizar o consumo analítico direto e reduzir a complexidade das consultas de negócio, a camada Gold orquestra a consolidação dessas tabelas. Através de operações de `LEFT JOIN`, os identificadores numéricos são cruzados com seus respectivos dicionários, resultando em uma One Big Table (OBT) totalmente desnormalizada.

A modelagem dimensional eliminou a opacidade dos códigos alfanuméricos aduaneiros (NCM), entregando semântica de negócios imediata na Camada Gold, conforme estruturado no dicionário abaixo.

**3.2. Catálogo de Dados**

### 3.2. Catálogo de Dados, Linhagem e Domínio

Para garantir a governança e a rastreabilidade das transformações ao longo do pipeline Medallion, o catálogo abaixo detalha o esquema estrutural, o domínio numérico/categórico esperado e a linhagem de dados das tabelas curadas (Silver e Gold).

| Tabela | Coluna | Tipo_de_Dado | Descricao | Domínio de Valores (Min/Max/Cat) | Linhagem (Origem / Transformação) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **silver_fato_comex** | CO_ANO | INT | Ano do desembaraço aduaneiro. | `2024`, `2025` | Origem: Arquivos brutos. Tipagem explícita para INT aplicada na ingestão. |
| **silver_fato_comex** | CO_MES | INT | Mês do desembaraço aduaneiro. | `1` a `12` | Origem: Arquivos brutos. Tipagem explícita para INT aplicada na ingestão. |
| **silver_fato_comex** | CO_NCM | INT | Código numérico da NCM. | `1000000` a `99999999` | Origem: Arquivos brutos. Conversão via PySpark (`cast("int")`). |
| **silver_fato_comex** | CO_UNID | INT | Código da unidade de medida. | `10` a `99` (Categorias do MDIC) | Origem: Arquivos brutos. Conversão via PySpark (`cast("int")`). |
| **silver_fato_comex** | CO_PAIS | INT | Código BACEN do país. | `1` a `999` | Origem: Arquivos brutos. Conversão via PySpark (`cast("int")`). |
| **silver_fato_comex** | SG_UF_NCM | STRING | Sigla da UF do domicílio fiscal. | `SP`, `MG`, `ND`, etc. | Origem: Arquivos brutos. Leitura mantida como string original. |
| **silver_fato_comex** | CO_VIA | INT | Código da via de transporte. | `1` a `15` | Origem: Arquivos brutos. Conversão via PySpark (`cast("int")`). |
| **silver_fato_comex** | CO_URF | INT | Código da Unidade da Receita. | Ex: `817800` (Códigos RFB) | Origem: Arquivos brutos. Conversão via PySpark (`cast("int")`). |
| **silver_fato_comex** | QT_ESTAT | LONG | Quantidade estatística do item. | `>= 0` | Origem: Arquivos brutos. Conversão via PySpark (`cast("long")`). |
| **silver_fato_comex** | KG_LIQUIDO | DOUBLE | Peso líquido total (Kg). | `>= 0.0` | Origem: Arquivos brutos. Conversão via PySpark (`cast("double")`). |
| **silver_fato_comex** | VL_FOB | DOUBLE | Valor FOB (US$). | `> 0.0` | Origem: Arquivos brutos. `cast("double")` + Higienização (`filter(col("VL_FOB") > 0)`). |
| **silver_fato_comex** | TIPO_OPERACAO | STRING | Classificador do vetor comercial. | `'EXP'`, `'IMP'` | Criada logicamente via PySpark `lit()` no _unionByName_ da camada Bronze. |
| **silver_fato_comex** | VL_FRETE | INT | Valor do frete internacional (US$). | `>= 0` | Origem: Arquivos brutos. Conversão via PySpark (`cast("int")`). |
| **silver_fato_comex** | VL_SEGURO | INT | Valor do seguro internacional (US$). | `>= 0` | Origem: Arquivos brutos. Conversão via PySpark (`cast("int")`). |
| **gold_comex_analitica** | ano | INT | Período anual de registro. | `2024`, `2025` | Herança: `silver_fato_comex.CO_ANO`. Renomeada (`withColumnRenamed`). |
| **gold_comex_analitica** | mes | INT | Período mensal de registro. | `1` a `12` | Herança: `silver_fato_comex.CO_MES`. Renomeada (`withColumnRenamed`). |
| **gold_comex_analitica** | tipo_operacao | STRING | Classificador do vetor comercial. | `'EXP'`, `'IMP'` | Herança: `silver_fato_comex.TIPO_OPERACAO`. Letras minúsculas no padrão OBT. |
| **gold_comex_analitica** | cod_ncm | INT | Código numérico da NCM. | `1000000` a `99999999` | Herança: `silver_fato_comex.CO_NCM`. |
| **gold_comex_analitica** | desc_produto | STRING | Descritor textual do produto. | Diversas categorias textuais | Cruzamento: `LEFT JOIN` com `bronze_dim_ncm`. Tratamento de nulos com `coalesce("ND")`. |
| **gold_comex_analitica** | pais | STRING | Nação correspondente à operação. | `Estados Unidos`, `China`, etc. | Cruzamento: `LEFT JOIN` com `bronze_dim_pais`. Tratamento de nulos com `coalesce("ND")`. |
| **gold_comex_analitica** | estado_uf | STRING | UF correspondente ao domicílio. | `São Paulo`, `Minas Gerais`, etc. | Cruzamento: `LEFT JOIN` com `bronze_dim_uf`. Tratamento de nulos com `coalesce("ND")`. |
| **gold_comex_analitica** | via_transporte | STRING | Modal logístico utilizado. | `Via Marítima`, `Via Aérea`, etc. | Cruzamento: `LEFT JOIN` com `bronze_dim_via`. Tratamento de nulos com `coalesce("ND")`. |
| **gold_comex_analitica** | peso_liquido_kg | DOUBLE | Massa física total (Kg). | `>= 0.0` | Herança: `silver_fato_comex.KG_LIQUIDO`. Renomeada (`withColumnRenamed`). |
| **gold_comex_analitica** | valor_fob_dolar | DOUBLE | Montante financeiro (US$). | `> 0.0` | Herança: `silver_fato_comex.VL_FOB`. Renomeada (`withColumnRenamed`). |

Tabela 1. Catálogo de dados contendo todos atributos utilizados neste trabalho.

**4. Pipeline de Dados (Etapa 4.4)**

O processo de ETL foi realizado em um único Notebook no Databricks, utilizando a linguagem PySpark. Para manter o fluxo coeso e simplificar a governança, a ramificação do processamento não foi feita dividindo o código em múltiplos arquivos, mas sim estruturada de forma lógica e sequencial dentro do mesmo script, adotando rigorosamente a Arquitetura Medallion em três fases de processamento direto:

**Camada Bronze** 
A etapa inicial é responsável pela ingestão escalável das fontes governamentais. O script inicia consumindo os oito arquivos nativos .csv. A ramificação lógica desta fase consiste na união vertical das bases históricas, agregando os anos de 2024 e 2025, e na criação de uma coluna identificadora primária, denominada TIPO_OPERACAO, para distinguir os fluxos de exportação ('EXP') e importação ('IMP'). Finalizadas essas transformações primárias, os dados brutos foram salvos fisicamente como tabelas no formato Delta Lake, preservando o histórico imutável das transações.

**Camada Silver**
Na sequência, o pipeline avança para a fase de higienização, consumindo diretamente a tabela consolidada na etapa Bronze. A ramificação lógica da camada Silver concentra-se na qualidade de dados: o script aplica a correção de _encoding_ e implementa filtros defensivos rígidos (como filter(col("VL_FOB") > 0)). Esse filtro garante o descarte automático de registros aduaneiros corrompidos ou sem impacto financeiro real.A base limpa é persistida sobrepondo a camada intermediária (silver_fato_comex), que passa a atuar como a fonte oficial do projeto. 

**Camada Gold**
O processamento é finalizado na camada destinada à inteligência de negócio. O script orquestra a consolidação dos dados realizando a leitura da camada Silver em conjunto com as tabelas de Dimensões textuais (Bronze). A ramificação lógica constrói uma modelagem dimensional (Star Schema) por meio de cruzamentos do tipo LEFT JOIN. Para proteger a integridade estatística da balança comercial, a função coalesce é combinada com lit para tratar as lacunas governamentais de origem ("ND") sem causar distorções numéricas. O resultado desta operação é materializado e salvo na tabela gold_comex_analitica, entregando uma One Big Table fisicamente otimizada para as consultas gerenciais em SQL.

<img width="362" height="325" alt="tabelas-mvp" src="https://github.com/user-attachments/assets/64e4ace4-8b45-4417-a666-65f078747040" />

Figura 1. Listagem das tabelas criadas (bronze, silver e gold)

O fluxo Medallion completo (Bronze > Silver > Gold) está versionado neste repositório: pipeline_etl_comex.ipynb.

**5. Qualidade de Dados (Etapa 4.5)**

O perfilamento dos dados revelou instabilidades sistêmicas nos registros governamentais. Evidenciando a baixa qualidade na coleta da origem. Contextualizando, no comércio exterior real, muitas operações aduaneiras (especialmente importações de bens nacionalizados em portos genéricos ou compras governamentais) são registradas sem a declaração do estado de destino final. O sistema do Siscomex registra essas transações com a sigla "ND" (Não Declarado). Portanto, o alto volume reflete a realidade operacional imperfeita da balança comercial brasileira, onde a rastreabilidade regional possui furos. A auditoria de qualidade processou as cinco dimensões críticas de higienização durante a transição estrutural da Camada Silver para a Gold:

**Completude**: Detectou-se uma proporção significativa de valores classificados como "ND" (Não Declarado) na coluna de Unidades Federativas de destino/origem, caracterizando um furo na rastreabilidade do Siscomex. A mera supressão dessas linhas corromperia os totais financeiros globaisn não podendo ser adotada. Neste caso, a solução utilizada foi a inserção de um LEFT JOIN para preservar as transações orfãs e aplicou a função coalesce para inserir "Estado Não Informado", mantendo a base analiticamente coesa e sem imputações enviesadas.

**Consistência**: Falhas críticas de encoding e tipagem. A base nativa utiliza latin1, gerando corrupção de caracteres especiais se lida em UTF-8. Além disso, aspas mal formatadas no dicionário NCM deslocavam colunas, causando erros críticos no cruzamento de dados (CAST_INVALID_INPUT). Para isso, foi realizada a inserção do parâmetro encoding="latin1" na ingestão (Bronze) e conversão explícita forçada para texto (.cast("string")) em todas as chaves relacionais na Camada Gold.

**Unicidade**: Avaliou-se o risco de duplicidade de registros. Como a Tabela Fato reflete transações individuais que podem possuir os mesmos exatos atributos em dias diferentes, não se aplicou a função dropDuplicates(), sob pena de apagar volumes legítimos de operações recorrentes. A granularidade da origem manteve-se intacta.

**Acurácia**: Avaliação de coerência física e financeira. Identificaram-se registros nulos ou zerados na coluna de montante financeiro, sem amparo lógico aduaneiro. Neste caso, foi realizada a implementação de filtro na Camada Silver (filter(col("VL_FOB") > 0) e conversão matemática para Double), descartando linhas sem valor econômico.

**Outliers**: A análise descritiva identificou transações com valores FOB isolados na casa de centenas de milhões de dólares. Porém, no contexto de comércio exterior (ex: importação de plataformas de petróleo ou exportação de lote de aeronaves intercontinentais), operações com valores extremos são ocorrências factuais, e não erros sistêmicos. Os outliers foram validados teoricamente e mantidos no escopo para não distorcer o resultado financeiro total do país.

Abaixo, uma tabela contendo os resultados obtidos utilizando a **Validação Automatizada de Qualidade** presente nativamente no Databricks:

| Métrica Avaliada             	| Coluna(s) de Referência          	| Resultado                                   	| Evidência da Transformação                                                                                                                       	|
|------------------------------	|----------------------------------	|---------------------------------------------	|--------------------------------------------------------------------------------------------------------------------------------------------------	|
| Completude (Null Ratio)      	| estado_uf, pais, desc_produto    	| 0% de Nulos                                 	| Confirma a eficácia do uso da função coalesce para tratamento de registros órfãos, substituindo lacunas governamentais por descritivos literais. 	|
| Acurácia Numérica            	| valor_fob_dolar, peso_liquido_kg 	| 0% de Zeros                                 	| Valida a limpeza realizada na Camada Silver, que filtrou anomalias e registros aduaneiros sem impacto financeiro real.                           	|
| Consistência (Cardinalidade) 	| tipo_operacao                    	| 2 Valores                                   	| A exatidão de apenas dois vetores ('EXP' e 'IMP') comprova a ausência de ruídos ou erros de categorização na ingestão.                           	|
| Volumetria Global            	| Tabela Inteira (gold_comex)      	| 7,97 milhões de registros 	| Demonstra a estabilidade do pipeline na consolidação integral da carga histórica (2024-2025).                                                    	|


Tabela 2. resultado extraído do Databricks Lakehouse Monitoring.

**6. Análise de Dados (Etapa 4.5)**
**6.1. Vulnerabilidade da Cadeia de Suprimentos**

Qual é o nível de concentração e dependência geopolítica do Brasil no fornecimento de insumos críticos (Insumos Agrícolas, Fármacos/Equipamentos Médicos, Minérios/Energia, Química Fina/Polímeros e Tecnologia)? A balança comercial evidencia uma assimetria perigosa. A consolidação dos 5 eixos de vulnerabilidade demonstra o quanto de capital é imobilizado na sustentação estrutural do país.

| categoria_critica               	| bilhoes_us 	| milhoes_ton 	|
|---------------------------------	|------------	|-------------	|
| Minérios e Energia              	| 29.65      	| 84.9        	|
| Fármacos e Equipamentos Médicos 	| 17.46      	| 0.13        	|
| Insumos Agrícolas               	| 11.88      	| 13.71       	|
| Química Fina e Polímeros        	| 8.23       	| 3.6         	|
| Tecnologia                      	| 6.93       	| 0.22        	|

Tabela 3. Volumes de importação do Top 5 de insumos críticos (categorias).

<img width="1044" height="376" alt="newplot (3)" src="https://github.com/user-attachments/assets/c02384f6-016d-4dfb-afed-a3a2ed902f92" />

Figura 2. Gráfico de barras com volumes de importação do Top 5 de insumos críticos (categorias).

A matriz geopolítica abaixo revela a extrema concentração de fornecimento:

| pais_origem    	| bilhoes_us 	| milhoes_ton 	| percentual_participacao 	|
|----------------	|------------	|-------------	|-------------------------	|
| Estados Unidos 	| 17.83      	| 35.15       	| 24.05                   	|
| China          	| 10.81      	| 6.99        	| 14.58                   	|
| Arábia Saudita 	| 3.91       	| 6.8         	| 5.28                    	|
| Alemanha       	| 3.22       	| 0.41        	| 4.35                    	|
| Bolívia        	| 2.25       	| 7.5         	| 3.04                    	|

Tabela 4. Volume e concentração de importação de insumos críticos por parceiro comercial.

<img width="856" height="482" alt="newplot" src="https://github.com/user-attachments/assets/a93fdacb-6774-4e4c-8c6c-559703e5f029" />

Figura 3. Representação gráfica da concentração de importação de insumos críticos por parceiro comercial.

Os dados evidenciam que a cadeia de suprimentos brasileira opera sob elevado risco geopolítico. Constata-se uma dependência massiva de poucas nações para garantir insumos básicos do agronegócio e compostos farmacêuticos primários (IFAs), com uma concentração muito alta dos dois principai parceiros comerciais do Brasil. Também foi observada a vulnerabilidade da cadeia de suprimentos: choques logísticos em nações fornecedoras possuem potencial imediato para paralisar as operações do Brasil, denotando urgência em políticas de nearshoring e incentivo à produção interna de defensivos e tecnologia.

**6.2. A Corrida da Transição Energética e Inovação Tecnológica**

Quais unidades federativas centralizam os aportes (via importação) em infraestrutura de eletrificação, geração renovável e componentes inteligentes? O ranking de alocação financeira estadual para importação de infraestrutura moderna (células fotovoltaicas, aero geradores, baterias de lítio e semicondutores).

| estado_uf      	| bilhoes_us 	| percentual_participacao 	|
|----------------	|------------	|-------------------------	|
| Espírito Santo 	| 6.79       	| 34.52                   	|
| Amazonas       	| 3.21       	| 16.34                   	|
| São Paulo      	| 2.86       	| 14.54                   	|
| Santa Catarina 	| 2.42       	| 12.31                   	|
| Minas Gerais   	| 0.8        	| 4.07                    	|

Tabela 5. Volume e participação na importação de itens de transição energética e inovação tecnológica.

<img width="856" height="482" alt="newplot (1)" src="https://github.com/user-attachments/assets/f7539b0b-4258-45d5-b992-1cf19715a837" />

Figura 4. Representação gráfica do volume e participação na importação de itens de transição energética e inovação tecnológica.

Os resultados comprovam uma severa assimetria geográfica na modernização do parque industrial e matriz energética. Uma parcela esmagadora das inovações de alto valor tecnológico é absorvida quase que exclusivamente pelos estados da região Sudeste e Sul, marginalizando outras regiões do processo de eletrificação e autonomia produtiva. Isso reforça que a adoção tecnológica reflete diretamente o Produto Interno Bruto (PIB) regionalizado, perpetuando o abismo estrutural entre os estados da federação.

**6.3. Qualidade da Balança Comercial e Nível de Manufatura**

Analisando os 10 maiores parceiros comerciais do país, qual é o saldo financeiro real e o grau de dependência da exportação de produtos primários (commodities) em detrimento de bens de maior valor agregado? A análise qualitativa das trocas comerciais com as 10 maiores economias parceiras, contrastando o saldo absoluto com a proporção de produtos primários e rudimentares exportados (soja, minério, petróleo bruto, carnes in natura).

| pais                    	| montante_bilhoes_us 	| saldo_comercial_bilhoes_us 	| percentual_commodities_exportadas 	|
|-------------------------	|---------------------	|----------------------------	|------------------------------------	|
| China                   	| 328.86              	| 59.76                      	| 80.66                              	|
| Estados Unidos          	| 163.85              	| -7.74                      	| 21.02                              	|
| Argentina               	| 58.39               	| 5.37                       	| 5.42                               	|
| Alemanha                	| 40.56               	| -15.82                     	| 30.69                              	|
| Países Baixos (Holanda) 	| 28.12               	| 18.77                      	| 54                                 	|
| México                  	| 27.51               	| 3.55                       	| 13.08                              	|
| Índia                   	| 27.33               	| -3.05                      	| 74.85                              	|
| Espanha                 	| 26.55               	| 10.96                      	| 78.55                              	|
| Chile                   	| 23.41               	| 4.25                       	| 32.15                              	|
| Rússia                  	| 23.35               	| -17.4                      	| 25.06                              	|

Tabela 6. Qualidade de balança comercial do Top 10 principais parceiros comerciais do Brasil.

<img width="856" height="482" alt="newplot (2)" src="https://github.com/user-attachments/assets/20ec52f7-6713-4e09-b8e6-775af7bfd9d5" />

Figura 5. Representação gráfica da qualidade de balança comercial do Top 10 principais parceiros comerciais do Brasil.

A qualidade na balança comercial brasileira se demonstrou deficiente. Embora o Brasil registre volumosos superávits em bilhões em relação à boa parte dos top 10 parceiros comerciais, a análise qualitativa demonstra um cenário comercial desfavorável em termos de valor agregado. A esmagadora maioria do volume financeiro de exportação destina-se a parceiros que utilizam o Brasil como celeiro primário e polo extrativista. Observa-se que, com potências tecnológicas, as commodities chegam a representar a quase totalidade do volume exportado, enquanto o Brasil absorve todo o passivo da importação de manufaturados avançados oriundos dessas mesmas nações.

**7. Autoavaliação**

A realização deste projeto foi enriquecedora, com muitos desafios e obstáculos vencidos. No que se refere ao projeto em si, acredito que o consegui alcançar os objetivos e entregar as respostas estipuladas. O processamento escalável via Databricks demonstrou boa performance, possibilitando a consolidação da Arquitetura Medallion, blindando o ambiente de exploração contra anomalias na estrutura de dados governamentais.  O principal percalço sistêmico relacionou-se à baixa maturidade e instabilidade da fonte primária (Comex Stat). Os defeitos de encoding textuais e o altíssimo volume de lacunas ("ND") na matriz relacional (UF e Países) exigiram forte atuação saneante na camada Silver. Além disso, outro grande desafio foi adequar lógicas de negócio puras na camada técnica, forçando chaves primárias textuais para garantir a resiliência dos registros.

Para futuros trabalhos, olhando o escopo atual, temos três incrementos que podem enriquecer a solução das perguntas realizadas:

- Substituir a coleta e ingestão de dados manualmente pela implementação de pipelines via Databricks Workflows, consumindo a API REST oficial do governo de forma incremental.
- Incorporar bibliotecas especializadas como Great Expectations diretamente no código PySpark para gerar relatórios de auditoria e validação de regras de domínio de maneira automatizada na Camada Silver.
- Ingerir as tabelas de taxas de câmbio (PTAX) mantidas pelo Banco Central do Brasil para cruzar com a camada Gold, permitindo a apuração de impactos analíticos ajustados à flutuação cambial do Real (BRL) durante a competência analisada.



