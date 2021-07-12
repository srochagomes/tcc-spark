package br.com.tcc.controller.stocks;

import br.com.tcc.conf.SparkDatasets;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@AllArgsConstructor
@RestController
public class GetSales extends SalesRootController {


    @Autowired
    private SparkDatasets sparkDatasets;

    @GetMapping(value = "v1", produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<List<String>> getAllAccount() {

        return ResponseEntity.ok().body(sparkDatasets.getResult());
    }


}
