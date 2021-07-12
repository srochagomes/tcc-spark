package br.com.tcc.conf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.util.List;

@Component
public class SparkDatasets {

    private SparkSession spark;
    private Dataset<Row> dataset;
    private Dataset<Row> datasetRoot;



    @PostConstruct
    private void createDataset(){
        try{
            Logger.getLogger("org.apache").setLevel(Level.WARN);

            var warehouseLocation = new File("/databases").getAbsolutePath();

            this.spark = SparkSession.builder()
                    .appName("tcc-datascience-stock")
                    .master("local[*]")
                    .config("spark.sql.warehouse.dir",warehouseLocation)
//                    .config("spark.dynamicAllocation.enabled",true)
//                    .config("spark.driver.memory","12g")
//                    .config("spark.dynamicAllocation.maxExecutors","100")
                    .config("spark.sql.inMemoryColumnarStorage.batchSize",100000)

                    .getOrCreate();

            var databasePath =
                    "/home/programmer/desenvolvimento/tcc/datascience/projeto/tcc-datascience/";

            var pathDataNota = databasePath.concat("headers/header_*.csv");
            var pathDataItem = databasePath.concat("items/item_*.csv");

            Dataset<Row> datasetNotaFiscal = spark.read()
                    .option("header", true)
                    .option("delimiter",";")
                    .csv(pathDataNota);

            Dataset<Row> datasetItens = spark.read()
                    .option("header", true)
                    .option("delimiter",";")
                    .csv(pathDataItem);


            // Dataset<Row> modernArtResults = datasetNotaFiscal.filter("select * from students where subject = 'Modern Art' AND year >= 2007 ");

            datasetNotaFiscal.createOrReplaceTempView("header");
            datasetItens.createOrReplaceTempView("itens");

            //Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
            //Dataset<Row> resultsNFs = spark.sql("select * from vendas ");
            //dataset = spark.sql("select * from vendas join itens on vendas.IDENTIFICADOR_NOTA_FISCAL = itens.IDENTIFICADOR_NOTA_FISCAL ");
            this.datasetRoot = spark.sql("select header.IDENTIFICADOR_NOTA_FISCAL,\n" +
                    "header.DATA_EMISSAO AS DATA_EMISSAO, " +
                    "header.OPERACAO_FISCAL AS OPERACAO_FISCAL, " +
                    "header.TIPO_TRANSACAO AS  TIPO_TRANSACAO, " +
                    "header.CODIGO_PEDIDO_MULTICANAL AS CODIGO_PEDIDO_MULTICANAL, " +
                    "header.CODIGO_FILIAL AS CODIGO_FILIAL, " +
                    "header.TRANSACAO_CLIENTE AS TRANSACAO_CLIENTE, " +
                    "header.CODIGO_NOTA_FISCAL_ORIGEM AS CODIGO_NOTA_FISCAL_ORIGEM, " +
                    "header.FORMA_PAGAMENTO AS FORMA_PAGAMENTO, " +
                    "header.VALOR_NOTA_FISCAL AS VALOR_NOTA_FISCAL,  " +
                    "header.NUMERO_NOTA_FISCAL AS NUMERO_NOTA_FISCAL,  " +
                    "header.DATA_ATUALIZACAO AS DATA_ATUALIZACAO, " +
                    "header.LOJA_24H AS  LOJA_24H, " +
                    "header.ESTADO AS ESTADO, " +
                    "header.ENDERECO AS ENDERECO, " +
                    "header.BAIRRO AS BAIRRO, " +
                    "header.CIDADE AS CIDADE, " +
                    "header.CEP AS CEP, " +
                    "itens.NUMERO_ITEM AS NUMERO_ITEM, " +
                    "itens.CODIGO_PRODUTO AS  CODIGO_PRODUTO, " +
                    "itens.QUANTIDADE AS QUANTIDADE,  " +
                    "itens.VALOR_UNITARIO AS VALOR_UNITARIO, " +
                    "itens.VALOR_TOTAL  AS VALOR_TOTAL" +
                    " from header join itens on header.IDENTIFICADOR_NOTA_FISCAL = itens.IDENTIFICADOR_NOTA_FISCAL").cache();
            datasetRoot.createOrReplaceTempView("vendas");
            this.datasetRoot.show();
        }catch (Throwable e){
            e.printStackTrace();
            throw  e;
        }

    }

    @PreDestroy
    private void destroyDataset(){
        spark.close();
    }

    public List<String> getResult(){

        return spark.sql("select vendas.CODIGO_FILIAL, sum(vendas.VALOR_NOTA_FISCAL) from vendas group by vendas.CODIGO_FILIAL").cache().toJSON().collectAsList();
    }

}
