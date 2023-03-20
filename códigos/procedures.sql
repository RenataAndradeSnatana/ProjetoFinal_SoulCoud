-- PROCEDURE PARA ETL DA TABELA acoes
CREATE OR REPLACE PROCEDURE
etl_acoes()
language 'plpgsql'
AS
$$
    BEGIN
	-- CRIAÇÃO DA TABELA TEMPORARIA
    CREATE TEMP TABLE IF NOT EXISTS tmp_acoes(
        data_acao date,
        instituicao_financeira text,
        codigo_acao text,
        open double precision,
        high double precision,
        low double precision,
        close double precision,
        tipo text
    );
	-- LIMPANDO E TRANSFORMANDO OS DADOS ATRAVÉS DE UM SELECT
    INSERT INTO tmp_acoes SELECT TO_DATE(date,'YYYY-MM-DD'),instituicao_financeira,codigo_acao,open,high,low,close,tipo from acoes;
	CREATE TABLE dm_acoes AS SELECT * FROM tmp_acoes;
    END;
$$

-- PROCEDURE PARA ETL DA TABELA reclamacoes
CREATE OR REPLACE PROCEDURE
etl_reclamacoes()
LANGUAGE 'plpgsql'
AS 
$$
	BEGIN 
	-- CRIAÇÃO DA TABELA TEMPORARIA
	CREATE TEMP table if not exists tmp_reclamacoes(
		regiao char(2),
		faixa_etaria text,
		tempo_resposta integer,
		nome_fantasia text,
		assunto text,
		grupo_problema text,
		como_comprou_contratou text,
		procurou_empresa char(1),
		respondida char(1),
		situacao text,
		avaliacao_reclamacao text,
		nota_do_consumidor integer,
		ano_referencia text
    );
	-- LIMPANDO E TRANSFORMANDO OS DADOS ATRAVÉS DE UM SELECT
	INSERT INTO tmp_reclamacoes SELECT
		regiao,faixa_etaria,tempo_resposta,nome_fantasia,assunto,grupo_problema,como_comprou_contratou,procurou_empresa,respondida,
		situacao,avaliacao_reclamacao,nota_do_consumidor, ano_referencia
	FROM reclamacoes
	WHERE
    nome_fantasia LIKE '%Banco Itaú%' OR
	nome_fantasia LIKE '%Banco do Brasil%' OR
	nome_fantasia LIKE '%Banco Bradesco%' OR
    nome_fantasia LIKE '%Banco Santander' OR
    nome_fantasia LIKE '%Caixa Econômica%' OR
    nome_fantasia LIKE '%Nubank%' OR
    nome_fantasia LIKE '%Banco Inter%' OR
    nome_fantasia LIKE '%Banco Modal%' OR 
    nome_fantasia LIKE '%Banco Pan%' OR
		nome_fantasia = 'Banco BMG';
	CREATE TABLE dm_reclamacoes AS SELECT * FROM tmp_reclamacoes;
	END;	
$$

-- PROCEDURE PARA ETL DA TABELA op_indiretas_bndes
CREATE OR REPLACE PROCEDURE
etl_bndes()
LANGUAGE 'plpgsql'
AS 
$$
	BEGIN 
	-- CRIAÇÃO DA TABELA TEMPORARIA
	CREATE TEMP table if not exists tmp_bndes(
		uf char(2),
		data_da_contratacao date,
		valor_da_operacao_em_reais integer,
		valor_desembolsado_reais  double precision,
		juros  double precision,
		prazo_carencia_meses integer,
		inovacao text,		
		subsetor_bndes text,
		porte_do_cliente text,
		natureza_do_cliente text,
		instituicao_financeira_credenciada text,
		situacao_da_operacao text
		);
	-- LIMPANDO E TRANSFORMANDO OS DADOS ATRAVÉS DE UM SELECT
	INSERT INTO tmp_bndes SELECT REPLACE(uf,' ',''),TO_DATE(data_da_contratacao, 'YYYY-MM-DD'), valor_da_operacao_em_reais,
		CAST(REPLACE(valor_desembolsado_reais,',','.') as double precision),CAST(REPLACE(juros,',','.') as  double precision), prazo_carencia_meses, 
		inovacao, subsetor_bndes, porte_do_cliente, natureza_do_cliente, 
		instituicao_financeira_credenciada, situacao_da_operacao 
		FROM bndes
		WHERE instituicao_financeira_credenciada = 'BANCO BMG SA' or 
		instituicao_financeira_credenciada = 'BANCO DO BRASIL SA' or 
		instituicao_financeira_credenciada = 'BANCO SANTANDER (BRASIL) S.A.' or 
		instituicao_financeira_credenciada = 'ITAU UNIBANCO S.A.' or 
		instituicao_financeira_credenciada = 'BANCO BRADESCO S.A.' or 
		instituicao_financeira_credenciada = 'CAIXA ECONOMICA FEDERAL' or 
		instituicao_financeira_credenciada = 'BANCO MODAL S.A.';

		CREATE TABLE dm_bndes AS SELECT * FROM tmp_bndes;
	END;
$$