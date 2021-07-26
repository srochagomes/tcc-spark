package br.com.tcc.controller.stocks;

import br.com.tcc.conf.SparkDatasets;
import io.swagger.annotations.ApiOperation;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.QueryParam;
import java.util.List;

@AllArgsConstructor
@RestController
public class GetSales extends SalesRootController {


    @Autowired
    private SparkDatasets sparkDatasets;

    @ApiOperation("Consulta das vendas por produto")
    @GetMapping(value = "v1/{idProduct}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<List<String>> getAllByProduct(@PathVariable("idProduct") Integer idProduct) {

        return ResponseEntity.ok().body(sparkDatasets.getSelectByProduct(Search.builder().idProduct(idProduct).build()));
    }

    @ApiOperation("Consulta das vendas de produto por filial")
    @GetMapping(value = "v1/{idProduct}/filial/{codigoFilial}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<List<String>> getAllByProductAndBranch(@PathVariable("idProduct") Integer idProduct,
                                                                 @PathVariable("codigoFilial") Integer codigoFilial) {
        return ResponseEntity.ok().body(sparkDatasets.getSelectByProductGroupFilial(
                Search.builder().idProduct(idProduct).codigoFilial(codigoFilial).build()));
    }

}
