package br.com.tcc.controller.stocks;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Search {

    private Integer idProduct;
    private Integer codigoFilial;
}
