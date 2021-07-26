package br.com.tcc.controller.products;

import br.com.tcc.conf.SparkDatasets;
import br.com.tcc.controller.stocks.SalesRootController;
import br.com.tcc.controller.stocks.Search;
import io.swagger.annotations.ApiOperation;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@AllArgsConstructor
@RestController
public class GetProducts extends ProductsRootController {


    @Autowired
    private SparkDatasets sparkDatasets;

    @ApiOperation("Consulta de produto")
    @GetMapping(value = "v1/{idProduct}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<String> getAllByProduct(@PathVariable("idProduct") Integer idProduct) {

        return ResponseEntity.ok().body(sparkDatasets.getSelectProductByIdProduct(Search.builder().idProduct(idProduct).build()));

    }

}
