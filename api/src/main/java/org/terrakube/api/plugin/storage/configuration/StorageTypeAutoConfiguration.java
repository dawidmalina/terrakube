package org.terrakube.api.plugin.storage.configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.terrakube.api.plugin.storage.StorageTypeService;
import org.terrakube.api.plugin.storage.aws.AwsStorageTypeProperties;
import org.terrakube.api.plugin.storage.aws.AwsStorageTypeServiceImpl;
import org.terrakube.api.plugin.storage.azure.AzureStorageTypeProperties;
import org.terrakube.api.plugin.storage.azure.AzureStorageTypeServiceImpl;
import org.terrakube.api.plugin.storage.gcp.GcpStorageTypeProperties;
import org.terrakube.api.plugin.storage.gcp.GcpStorageTypeServiceImpl;
import org.terrakube.api.plugin.storage.local.LocalStorageTypeServiceImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.terrakube.api.plugin.streaming.StreamingService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

@Configuration
@EnableConfigurationProperties({
        AzureStorageTypeProperties.class,
        StorageTypeProperties.class,
        GcpStorageTypeProperties.class
})
@ConditionalOnMissingBean(StorageTypeService.class)
@Slf4j
public class StorageTypeAutoConfiguration {

    @Bean
    public StorageTypeService terraformOutput(StreamingService streamingService, StorageTypeProperties storageTypeProperties, AzureStorageTypeProperties azureStorageTypeProperties, AwsStorageTypeProperties awsStorageTypeProperties, GcpStorageTypeProperties gcpStorageTypeProperties) {
        StorageTypeService storageTypeService = null;
        log.info("StorageType={}", storageTypeProperties.getType());
        switch (storageTypeProperties.getType()) {
            case AZURE:
                BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                        .connectionString(
                                String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
                                        azureStorageTypeProperties.getAccountName(),
                                        azureStorageTypeProperties.getAccountKey())
                        ).buildClient();

                storageTypeService = AzureStorageTypeServiceImpl.builder()
                        .blobServiceClient(blobServiceClient)
                        .build();
                break;
            case AWS:
                AwsBasicCredentials credentials = AwsBasicCredentials.create(
                        awsStorageTypeProperties.getAccessKey(),
                        awsStorageTypeProperties.getSecretKey()
                );

                S3Client s3client = null;
                if (awsStorageTypeProperties.getEndpoint() != "") {
                    ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder()
                            .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                            .build();
                    
                    s3client = S3Client.builder()
                            .endpointOverride(URI.create(awsStorageTypeProperties.getEndpoint()))
                            .credentialsProvider(StaticCredentialsProvider.create(credentials))
                            .overrideConfiguration(clientOverrideConfiguration)
                            .region(Region.of(awsStorageTypeProperties.getRegion()))
                            .build();
                } else
                    s3client = S3Client.builder()
                            .credentialsProvider(StaticCredentialsProvider.create(credentials))
                            .region(Region.of(awsStorageTypeProperties.getRegion()))
                            .build();

                storageTypeService = AwsStorageTypeServiceImpl.builder()
                        .s3client(s3client)
                        .bucketName(awsStorageTypeProperties.getBucketName())
                        .build();
                break;
            case GCP:
                log.info("GCP Base64 {} length", gcpStorageTypeProperties.getCredentials().length());
                Credentials gcpCredentials = null;
                try {
                    gcpCredentials = GoogleCredentials.fromStream(
                            new ByteArrayInputStream(
                                    Base64.decodeBase64(gcpStorageTypeProperties.getCredentials())
                            )
                    );
                    Storage gcpStorage = StorageOptions.newBuilder()
                            .setCredentials(gcpCredentials)
                            .setProjectId(gcpStorageTypeProperties.getProjectId())
                            .build()
                            .getService();

                    storageTypeService = GcpStorageTypeServiceImpl.builder()
                            .storage(gcpStorage)
                            .bucketName(gcpStorageTypeProperties.getBucketName())
                            .build();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }

                break;
            default:
                storageTypeService = LocalStorageTypeServiceImpl.builder().build();
        }
        return storageTypeService;
    }
}
