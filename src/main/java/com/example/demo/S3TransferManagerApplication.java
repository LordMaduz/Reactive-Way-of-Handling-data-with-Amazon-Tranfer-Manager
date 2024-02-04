package com.example.demo;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@OpenAPIDefinition(info = @Info(title = "S3-Transfer-Manager-Service", version = "1.0", description = "S3-Transfer-Manager-Service"))
public class S3TransferManagerApplication {

	public static void main(String[] args) {
		SpringApplication.run(S3TransferManagerApplication.class, args);
	}

}
