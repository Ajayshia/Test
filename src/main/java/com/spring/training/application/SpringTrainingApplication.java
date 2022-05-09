package com.spring.training.application;

import org.springframework.boot.SpringApplication;
import springfox.documentation.builders.RequestHandlerSelectors;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
@EnableSwagger2
@ComponentScan({"com.spring.training.*"})
public class SpringTrainingApplication {

public static void main(String[] args) {
SpringApplication.run(SpringTrainingApplication.class, args);
}

@Bean
public Docket productApi() {
   return new Docket(DocumentationType.SWAGGER_2).select()
      .apis(RequestHandlerSelectors.basePackage("com.spring.training.Controller")).build();
}
}