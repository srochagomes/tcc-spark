package br.com.tcc.controller.stocks;

import br.com.tcc.conf.SparkDatasets;
import io.swagger.annotations.ApiOperation;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@AllArgsConstructor
@RestController
public class PostSales extends SalesRootController {

    private static List<Integer> inProcess = new ArrayList<>();

    private static final ExecutorService threadpool =
            Executors.newFixedThreadPool(3);

    @Autowired
    private SparkDatasets sparkDatasets;

    @ApiOperation("Processa as vendas por produtos através dos datasets HEADER e ITENS")
    @PostMapping(value = "v1/{idProduct}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<String> createProduct(@PathVariable("idProduct") Integer idProduct) {

        if (!inProcess.contains(idProduct)){
                inProcess.add(idProduct);
                this.process(idProduct);
                return ResponseEntity.ok().body("Processo iniciado para o produto "+idProduct);
        }
        return ResponseEntity.ok().body("Processo em andamento para o produto "+idProduct);
    }

    private void process(Integer idProduct) {
        System.out.println("Processando a tarefa ...");

//        sparkDatasets.getCreateStockBy(idProduct);
//        inProcess.remove(idProduct);

        Future<Integer> future = threadpool.submit(new Callable<Integer>(){
                @Override
                public Integer call() throws Exception {
                    sparkDatasets.getCreateStockBy(idProduct);
                    inProcess.remove(idProduct);
                    return 0;
                }
            });

            future.isDone();

    }

    @PreDestroy
    private void finalizeThread(){
        threadpool.shutdown();
    }


}
