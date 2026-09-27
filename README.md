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

<img width="362" height="325" alt="tabelas-mvp" src="https://github.com/user-attachments/assets/64e4ace4-8b45-4417-a666-65f078747040" />

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


**4. Pipeline de Dados (Etapa 4.4)**




































**Validação Automatizada de Qualidade (Databricks Lakehouse Monitoring)**

| Métrica Avaliada             	| Coluna(s) de Referência          	| Resultado                                   	| Evidência da Transformação                                                                                                                       	|
|------------------------------	|----------------------------------	|---------------------------------------------	|--------------------------------------------------------------------------------------------------------------------------------------------------	|
| Completude (Null Ratio)      	| estado_uf, pais, desc_produto    	| 0% de Nulos                                 	| Confirma a eficácia do uso da função coalesce para tratamento de registros órfãos, substituindo lacunas governamentais por descritivos literais. 	|
| Acurácia Numérica            	| valor_fob_dolar, peso_liquido_kg 	| 0% de Zeros                                 	| Valida a limpeza realizada na Camada Silver, que filtrou anomalias e registros aduaneiros sem impacto financeiro real.                           	|
| Consistência (Cardinalidade) 	| tipo_operacao                    	| 2 Valores                                   	| A exatidão de apenas dois vetores ('EXP' e 'IMP') comprova a ausência de ruídos ou erros de categorização na ingestão.                           	|
| Volumetria Global            	| Tabela Inteira (gold_comex)      	| (Preencher com o total de linhas) Registros 	| Demonstra a estabilidade do pipeline na consolidação integral da carga histórica (2024-2025).                                                    	|
