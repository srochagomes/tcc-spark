package br.com.tcc.conf;

import br.com.tcc.controller.stocks.Search;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Component
public class SparkDatasets {

    public static final String HOME_RESULTADO = "/home/programmer/desenvolvimento/tcc/datascience/projeto/resultado/";
    public static final String DATABASE_PATH  = "/home/programmer/desenvolvimento/tcc/datascience/projeto/tcc-datascience/";
    private SparkSession spark;
    private Dataset<Row> dataset;
    private Dataset<Row> datasetRoot;



    @PostConstruct
    private void createDataset(){
        try{
            Logger.getLogger("org.apache").setLevel(Level.WARN);

            var warehouseLocation = new File("/spark-databases").getAbsolutePath();

            this.spark = SparkSession.builder()
                    .appName("tcc-datascience-stock")
                    .master("local[*]")
                    .config("spark.sql.warehouse.dir",warehouseLocation)
                    .config("spark.dynamicAllocation.enabled",true)
                    .config("spark.driver.memory","16g")
//                    .config("spark.dynamicAllocation.maxExecutors","100")
                    //.config("spark.sql.shuffle.partitions",50)
                    //.config("spark.sql.inMemoryColumnarStorage.batchSize",100000)

                    .getOrCreate();


            var pathDataNota = DATABASE_PATH.concat("headers/header_*.csv");
            var pathDataItem = DATABASE_PATH.concat("items/item_*.csv");

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


        }catch (Throwable e){
            e.printStackTrace();
            throw  e;
        }

    }

    @PreDestroy
    private void destroyDataset(){
        spark.close();
    }

    public List<String> getCreateStockBy(Integer product){
        //Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
        //Dataset<Row> resultsNFs = spark.sql("select * from vendas ");
        //dataset = spark.sql("select * from vendas join itens on vendas.IDENTIFICADOR_NOTA_FISCAL = itens.IDENTIFICADOR_NOTA_FISCAL ");
        this.datasetRoot = spark.sql("select header.IDENTIFICADOR_NOTA_FISCAL, " +
                "header.DATA_EMISSAO AS DATA_EMISSAO, " +
                //"header.OPERACAO_FISCAL AS OPERACAO_FISCAL, " +
                //"header.TIPO_TRANSACAO AS  TIPO_TRANSACAO, " +
                //"header.CODIGO_PEDIDO_MULTICANAL AS CODIGO_PEDIDO_MULTICANAL, " +
                "header.CODIGO_FILIAL AS CODIGO_FILIAL, " +
                //"header.TRANSACAO_CLIENTE AS TRANSACAO_CLIENTE, " +
                //"header.CODIGO_NOTA_FISCAL_ORIGEM AS CODIGO_NOTA_FISCAL_ORIGEM, " +
                //"header.FORMA_PAGAMENTO AS FORMA_PAGAMENTO, " +
                //"header.VALOR_NOTA_FISCAL AS VALOR_NOTA_FISCAL,  " +
                //"header.NUMERO_NOTA_FISCAL AS NUMERO_NOTA_FISCAL,  " +
                //"header.DATA_ATUALIZACAO AS DATA_ATUALIZACAO, " +
                //"header.LOJA_24H AS  LOJA_24H, " +
                //"header.ESTADO AS ESTADO, " +
                //"header.ENDERECO AS ENDERECO, " +
                //"header.BAIRRO AS BAIRRO, " +
                //"header.CIDADE AS CIDADE, " +
                //"header.CEP AS CEP, " +
                //"itens.NUMERO_ITEM AS NUMERO_ITEM, " +
                "itens.CODIGO_PRODUTO AS  CODIGO_PRODUTO, " +
                "itens.QUANTIDADE AS QUANTIDADE,  " +
                //"itens.VALOR_UNITARIO AS VALOR_UNITARIO, " +
                "itens.VALOR_TOTAL  AS VALOR_TOTAL" +
                " from header join itens on header.IDENTIFICADOR_NOTA_FISCAL = itens.IDENTIFICADOR_NOTA_FISCAL");
        datasetRoot.createOrReplaceTempView("vendas");

        spark.sql("select TO_DATE(CAST(UNIX_TIMESTAMP(vendas.DATA_EMISSAO, 'dd/MM/yyyy') AS TIMESTAMP)) AS DATA_EMISSAO, vendas.CODIGO_FILIAL, vendas.CODIGO_PRODUTO, sum(vendas.QUANTIDADE) AS QUANTIDADE " +
                        " from vendas " +
                "where vendas.CODIGO_PRODUTO = " + product + " " +
                "group by DATA_EMISSAO, vendas.CODIGO_FILIAL, vendas.CODIGO_PRODUTO " +
                "order by DATA_EMISSAO, vendas.CODIGO_FILIAL, vendas.CODIGO_PRODUTO ")
                .write().mode(SaveMode.Overwrite).json(HOME_RESULTADO +product);
        return Arrays.asList();
    }


    public List<String> getSelectByProduct(Search search){
        Dataset<Row> datasetStockProduct = spark.read()
                .option("header", true)
                .option("delimiter",";")
                .json(HOME_RESULTADO.concat(search.getIdProduct()+"/").concat("*"));

        datasetStockProduct.createOrReplaceTempView("stockProduct");

        String filtro = "where stockProduct.CODIGO_PRODUTO = " + search.getIdProduct();

        return spark.sql("select DATA_EMISSAO, " +
                " stockProduct.CODIGO_PRODUTO, sum(stockProduct.QUANTIDADE) AS QUANTIDADE" +
                " from stockProduct " + filtro +
                " group by DATA_EMISSAO, stockProduct.CODIGO_PRODUTO "+
                " order by DATA_EMISSAO ").toJSON().collectAsList();

    }

    public List<String> getSelectByProductGroupFilial(Search search){
        Dataset<Row> datasetStockProduct = spark.read()
                .option("header", true)
                .option("delimiter",";")
                .json(HOME_RESULTADO.concat(search.getIdProduct()+"/").concat("*"));

        datasetStockProduct.createOrReplaceTempView("stockProduct");

        String filtro = "where stockProduct.CODIGO_PRODUTO = " + search.getIdProduct();

        if (Objects.nonNull(search.getCodigoFilial())){
            filtro = filtro.concat(" AND stockProduct.CODIGO_FILIAL="+search.getCodigoFilial());
        }

        return spark.sql("select DATA_EMISSAO, " +
                "stockProduct.CODIGO_FILIAL, stockProduct.CODIGO_PRODUTO, sum(stockProduct.QUANTIDADE) AS QUANTIDADE" +
                " from stockProduct " + filtro +
                " group by DATA_EMISSAO, stockProduct.CODIGO_FILIAL, stockProduct.CODIGO_PRODUTO "+
                " order by DATA_EMISSAO ").toJSON().collectAsList();

    }


    public String getSelectProductByIdProduct(Search search) {

        Dataset<Row> datasetProduct = spark.read()
                .option("header", true)
                .option("delimiter",";")
                .csv(DATABASE_PATH.concat("products.csv"));

        datasetProduct.createOrReplaceTempView("product");

        String filtro = "where product.CODIGO_PRODUTO = " + search.getIdProduct();


        return spark.sql("select * " +
                " from product " + filtro ).toJSON().first();

    }
}
