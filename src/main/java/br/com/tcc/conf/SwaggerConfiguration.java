package br.com.tcc.conf;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.http.MediaType;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.*;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spi.service.contexts.SecurityContext;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author lgdamy@raiadrogasil.com on 13/03/2021
 */
@Configuration
@EnableSwagger2
public class SwaggerConfiguration implements WebMvcConfigurer {

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(new ApiInfo("TCC Spark Stock", "Microsserviço ", "1.0", "urn:tos",
                        new Contact("TCC","","exemplo@exemplo.com"), "Apache 2.0", "http://www.apache.org/licenses/LICENSE-2.0", new ArrayList<VendorExtension>()))
                .tags(new Tag("Sales", "Serviços de mineração dos dados de venda com uso de Spark"))
                .consumes(Set.of(MediaType.APPLICATION_JSON_VALUE))
                .produces(Set.of(MediaType.APPLICATION_JSON_VALUE))
                //.ignoredParameterTypes(Pageable.class)
                .select()
                .apis(RequestHandlerSelectors.basePackage("br.com.tcc"))
                .paths(PathSelectors.any()).build()
                .pathMapping("/")
                .securityContexts(Collections.singletonList(SecurityContext.builder().securityReferences(globalAuth()).build()))
                ;
    }

    private List<SecurityReference> globalAuth() {
        AuthorizationScope[] authorizationScopes = new AuthorizationScope[2];
        authorizationScopes[0] = new AuthorizationScope("read", "Grants read access");
        authorizationScopes[1] = new AuthorizationScope("write", "Grants write access");
        return Collections.singletonList(new SecurityReference("JWT", authorizationScopes));
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/swagger").addResourceLocations("classpath:/META-INF/resources/");
        registry.addResourceHandler("/webjars/**").addResourceLocations("classpath:/META-INF/resources/webjars/");
    }
}
