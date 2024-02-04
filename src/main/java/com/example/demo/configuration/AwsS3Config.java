package com.example.demo.configuration;


import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@RequiredArgsConstructor
@Configuration
public class AwsS3Config {

    private final AwsProperties s3ConfigProperties;

    @Bean
    public S3AsyncClient s3AsyncClientCrt(AwsCredentialsProvider awsCredentialsProvider) {
        return S3AsyncClient.crtBuilder()
                .minimumPartSizeInBytes(s3ConfigProperties.getMultipartMinPartSize())
                .build();
    }

    @Bean
    public S3TransferManager transferManager(S3AsyncClient s3AsyncClientCrt) {
        return S3TransferManager.builder()
                .s3Client(s3AsyncClientCrt)
                .build();
    }

    @Bean
    AwsCredentialsProvider awsCredentialsProvider() {
        return () -> AwsBasicCredentials.create(s3ConfigProperties.getAccessKey(), s3ConfigProperties.getSecretKey());
    }

}
