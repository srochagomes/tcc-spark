package br.com.tcc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("br.com.tcc")
public class TccSparkStockApplication {

	public static void main(String[] args) {
		SpringApplication.run(TccSparkStockApplication.class, args);
	}

}
