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

**3.1. Modelagem**

O fluxo ETL (Extract, Transform, Load) foi consolidado em um único Notebook PySpark no Databricks, arquitetado sob o paradigma Medallion Architecture, ramificado metodologicamente em três estágios lógicos de processamento:

Camada Bronze (Raw): Ingestão escalável dos arquivos CSV a partir do Volume, unionByName das séries temporais anuais e inserção da flag operacional (TIPO_OPERACAO). Persistência no formato colunar Delta Lake.

Camada Silver (Cleansed): Execução de data profiling, saneamento de anomalias, type casting rigoroso de variáveis métricas e higienização de nulos.

Camada Gold (Curated): Modelagem analítica baseada no Star Schema. Execução de LEFT JOINs entre a Tabela Fato e os Dicionários, resultando em uma One Big Table desnormalizada, indexada e otimizada para o consumo das regras de negócio via SQL.

A modelagem dimensional eliminou a opacidade dos códigos alfanuméricos aduaneiros (NCM), entregando semântica de negócios imediata na Camada Gold, conforme estruturado no dicionário abaixo.

**3.2. Catálogo de Dados**

| Tabela               	| Coluna          	| Tipo_de_Dado 	| Descricao                                                                         	|
|----------------------	|-----------------	|--------------	|-----------------------------------------------------------------------------------	|
| bronze_dim_ncm       	| CO_NCM          	| STRING       	| Chave primária: Código numérico da Nomenclatura Comum do Mercosul (8 dígitos).    	|
| bronze_dim_ncm       	| CO_UNID         	| STRING       	| Código da unidade de medida estatística vinculada.                                	|
| bronze_dim_ncm       	| CO_SH6          	| STRING       	| Código do Sistema Harmonizado internacional (6 dígitos).                          	|
| bronze_dim_ncm       	| CO_PPE          	| STRING       	| Código da Pauta de Produtos de Exportação.                                        	|
| bronze_dim_ncm       	| CO_PPI          	| STRING       	| Código da Pauta de Produtos de Importação.                                        	|
| bronze_dim_ncm       	| CO_FAT_AGREG    	| STRING       	| Código do Fator Agregado (Ex: Básico, Semimanufaturado, Manufaturado).            	|
| bronze_dim_ncm       	| CO_CUCI_ITEM    	| STRING       	| Código da Classificação Uniforme para o Comércio Internacional (CUCI).            	|
| bronze_dim_ncm       	| CO_CGCE_N3      	| INT          	| Código da Classificação Geral de Atividades Econômicas (Nível 3).                 	|
| bronze_dim_ncm       	| CO_SIIT         	| STRING       	| Código do Sistema Integrado de Informações Tarifárias (SIIT).                     	|
| bronze_dim_ncm       	| CO_ISIC_CLASSE  	| INT          	| Código da Classificação Internacional Industrial Uniforme (ISIC Classe).          	|
| bronze_dim_ncm       	| CO_EXP_SUBSET   	| STRING       	| Código do subconjunto de macro-exportação.                                        	|
| bronze_dim_ncm       	| NO_NCM_POR      	| STRING       	| Descrição detalhada do produto na língua portuguesa.                              	|
| bronze_dim_ncm       	| NO_NCM_ESP      	| STRING       	| Descrição detalhada do produto na língua espanhola.                               	|
| bronze_dim_ncm       	| NO_NCM_ING      	| STRING       	| Descrição detalhada do produto na língua inglesa.                                 	|
| bronze_dim_pais      	| CO_PAIS         	| INT          	| Chave primária: Código identificador interno do país no Siscomex.                 	|
| bronze_dim_pais      	| CO_PAIS_ISON3   	| INT          	| Código numérico padrão internacional (ISO-3166).                                  	|
| bronze_dim_pais      	| CO_PAIS_ISOA3   	| STRING       	| Sigla internacional de 3 letras do país (ISO-3166 Alpha-3).                       	|
| bronze_dim_pais      	| NO_PAIS         	| STRING       	| Nome oficial do país na língua Portuguesa.                                        	|
| bronze_dim_pais      	| NO_PAIS_ING     	| STRING       	| Nome oficial do país na língua Inglesa.                                           	|
| bronze_dim_pais      	| NO_PAIS_ESP     	| STRING       	| Nome oficial do país na língua Espanhola.                                         	|
| bronze_dim_uf        	| CO_UF           	| INT          	| Código numérico identificador do IBGE para o Estado.                              	|
| bronze_dim_uf        	| SG_UF           	| STRING       	| Chave primária: Sigla de duas letras da Unidade da Federação (Ex: SP, MG).        	|
| bronze_dim_uf        	| NO_UF           	| STRING       	| Nome completo descritivo da Unidade da Federação.                                 	|
| bronze_dim_uf        	| NO_REGIAO       	| STRING       	| Macrorregião geográfica brasileira a qual o estado pertence (Ex: Sudeste, Norte). 	|
| bronze_dim_via       	| CO_VIA          	| INT          	| Chave primária: Código identificador do modal de transporte.                      	|
| bronze_dim_via       	| NO_VIA          	| STRING       	| Descrição textual do modal (Ex: Via Marítima, Via Aérea, Rodoviária).             	|
| bronze_fato_comex    	| CO_ANO          	| INT          	| Ano em que ocorreu o desembaraço aduaneiro da operação.                           	|
| bronze_fato_comex    	| CO_MES          	| INT          	| Mês em que ocorreu o desembaraço aduaneiro da operação.                           	|
| bronze_fato_comex    	| CO_NCM          	| INT          	| Código numérico da Nomenclatura Comum do Mercosul (Produto).                      	|
| bronze_fato_comex    	| CO_UNID         	| INT          	| Código da unidade de medida estatística padrão do produto.                        	|
| bronze_fato_comex    	| CO_PAIS         	| INT          	| Código do país de destino (exportação) ou de origem (importação).                 	|
| bronze_fato_comex    	| SG_UF_NCM       	| STRING       	| Sigla da Unidade da Federação do domicílio fiscal do importador/exportador.       	|
| bronze_fato_comex    	| CO_VIA          	| INT          	| Código da via de transporte (modal logístico).                                    	|
| bronze_fato_comex    	| CO_URF          	| INT          	| Código da Unidade da Receita Federal (Alfândega de despacho da mercadoria).       	|
| bronze_fato_comex    	| QT_ESTAT        	| LONG         	| Quantidade da mercadoria na unidade de medida estatística.                        	|
| bronze_fato_comex    	| KG_LIQUIDO      	| LONG         	| Peso líquido total da mercadoria expresso em quilogramas (Kg).                    	|
| bronze_fato_comex    	| VL_FOB          	| LONG         	| Valor financeiro da mercadoria em Dólares Americanos (US$) sob o Incoterm FOB.    	|
| bronze_fato_comex    	| TIPO_OPERACAO   	| STRING       	| Classificador do vetor comercial (EXP Exportação, IMP Importação).                	|
| bronze_fato_comex    	| VL_FRETE        	| INT          	| Valor financeiro do frete internacional em Dólares Americanos (US$).              	|
| bronze_fato_comex    	| VL_SEGURO       	| INT          	| Valor financeiro do seguro internacional em Dólares Americanos (US$).             	|
| gold_comex_analitica 	| ano             	| INT          	| Período anual de registro do desembaraço aduaneiro.                               	|
| gold_comex_analitica 	| mes             	| INT          	| Período mensal de registro do desembaraço aduaneiro.                              	|
| gold_comex_analitica 	| tipo_operacao   	| STRING       	| Classificador do vetor comercial (EXP Exportação, IMP Importação).                	|
| gold_comex_analitica 	| cod_ncm         	| INT          	| Identificador numérico da Nomenclatura Comum do Mercosul.                         	|
| gold_comex_analitica 	| desc_produto    	| STRING       	| Descritor textual detalhado do item transacionado.                                	|
| gold_comex_analitica 	| pais            	| STRING       	| Nação de origem (importação) ou destino (exportação).                             	|
| gold_comex_analitica 	| estado_uf       	| STRING       	| Unidade Federativa correspondente ao domicílio fiscal da operação.                	|
| gold_comex_analitica 	| via_transporte  	| STRING       	| Modal logístico primário utilizado para movimentação da carga.                    	|
| gold_comex_analitica 	| peso_liquido_kg 	| DOUBLE       	| Massa física total da mercadoria expressa em quilogramas.                         	|
| gold_comex_analitica 	| valor_fob_dolar 	| DOUBLE       	| Montante financeiro avaliado sob o Incoterm FOB, em dólares americanos.           	|
| silver_fato_comex    	| CO_ANO          	| INT          	| Ano em que ocorreu o desembaraço aduaneiro da operação.                           	|
| silver_fato_comex    	| CO_MES          	| INT          	| Mês em que ocorreu o desembaraço aduaneiro da operação.                           	|
| silver_fato_comex    	| CO_NCM          	| INT          	| Código numérico da Nomenclatura Comum do Mercosul (Produto).                      	|
| silver_fato_comex    	| CO_UNID         	| INT          	| Código da unidade de medida estatística padrão do produto.                        	|
| silver_fato_comex    	| CO_PAIS         	| INT          	| Código do país de destino (exportação) ou de origem (importação).                 	|
| silver_fato_comex    	| SG_UF_NCM       	| STRING       	| Sigla da Unidade da Federação do domicílio fiscal do importador/exportador.       	|
| silver_fato_comex    	| CO_VIA          	| INT          	| Código da via de transporte (modal logístico).                                    	|
| silver_fato_comex    	| CO_URF          	| INT          	| Código da Unidade da Receita Federal (Alfândega de despacho da mercadoria).       	|
| silver_fato_comex    	| QT_ESTAT        	| LONG         	| Quantidade da mercadoria na unidade de medida estatística.                        	|
| silver_fato_comex    	| KG_LIQUIDO      	| DOUBLE       	| Peso líquido total da mercadoria expresso em quilogramas (Kg).                    	|
| silver_fato_comex    	| VL_FOB          	| DOUBLE       	| Valor financeiro da mercadoria em Dólares Americanos (US$) sob o Incoterm FOB.    	|
| silver_fato_comex    	| TIPO_OPERACAO   	| STRING       	| Classificador do vetor comercial (EXP Exportação, IMP Importação).                	|
| silver_fato_comex    	| VL_FRETE        	| INT          	| Valor financeiro do frete internacional em Dólares Americanos (US$).              	|
| silver_fato_comex    	| VL_SEGURO       	| INT          	| Valor financeiro do seguro internacional em Dólares Americanos (US$).             	|

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

**5. Qualidade de Dados (Etapa 4.5)**

O perfilamento dos dados revelou instabilidades sistêmicas nos registros governamentais. Evidenciando a baixa qualidade na coleta da origem. Contextualizando, no comércio exterior real, muitas operações aduaneiras (especialmente importações de bens nacionalizados em portos genéricos ou compras governamentais) são registradas sem a declaração do estado de destino final. O sistema do Siscomex registra essas transações com a sigla "ND" (Não Declarado). Portanto, o alto volume reflete a realidade operacional imperfeita da balança comercial brasileira, onde a rastreabilidade regional possui furos. A auditoria de qualidade processou as cinco dimensões críticas de higienização durante a transição estrutural da Camada Silver para a Gold:

**Completude**: Detectou-se uma proporção significativa de valores classificados como "ND" (Não Declarado) na coluna de Unidades Federativas de destino/origem, caracterizando um furo na rastreabilidade do Siscomex. A mera supressão dessas linhas corromperia os totais financeiros globaisn não podendo ser adotada. Neste caso, a solução utilizada foi a inserção de um LEFT JOIN para preservar as transações orfãs e aplicou a função _coalesce_ para inserir "Estado Não Informado", mantendo a base analiticamente coesa e sem imputações enviesadas.

**Consistência**: Falhas críticas de encoding e tipagem. A base nativa utiliza _latin1_, gerando corrupção de caracteres especiais se lida em UTF-8. Além disso, aspas mal formatadas no dicionário NCM deslocavam colunas, causando erros críticos no cruzamento de dados (CAST_INVALID_INPUT). Para isso, foi realizada a inserção do parâmetro encoding="latin1" na ingestão (Bronze) e conversão explícita forçada para texto (.cast("string")) em todas as chaves relacionais na Camada Gold.

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

A balança comercial evidencia uma assimetria perigosa. A consolidação dos 5 eixos de vulnerabilidade demonstra o quanto de capital é imobilizado na sustentação estrutural do país.

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

O ranking de alocação financeira estadual para importação de infraestrutura moderna (células fotovoltaicas, aerogeradores, baterias de lítio e semicondutores).

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

A análise qualitativa das trocas comerciais com as 10 maiores economias parceiras, contrastando o saldo absoluto com a proporção de produtos primários e rudimentares exportados (soja, minério, petróleo bruto, carnes in natura).

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





