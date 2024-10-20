package org.terrakube.executor.plugin.tfstate.configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.terrakube.client.TerrakubeClient;
import org.terrakube.executor.plugin.tfstate.TerraformState;
import org.terrakube.executor.plugin.tfstate.TerraformStatePathService;
import org.terrakube.executor.plugin.tfstate.aws.AwsTerraformStateImpl;
import org.terrakube.executor.plugin.tfstate.aws.AwsTerraformStateProperties;
import org.terrakube.executor.plugin.tfstate.azure.AzureTerraformStateImpl;
import org.terrakube.executor.plugin.tfstate.azure.AzureTerraformStateProperties;
import org.terrakube.executor.plugin.tfstate.gcp.GcpTerraformStateImpl;
import org.terrakube.executor.plugin.tfstate.gcp.GcpTerraformStateProperties;
import org.terrakube.executor.plugin.tfstate.local.LocalTerraformStateImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

@Slf4j
@AllArgsConstructor
@Configuration
@EnableConfigurationProperties({
        TerraformStateProperties.class,
        AzureTerraformStateProperties.class,
        AwsTerraformStateProperties.class,
        GcpTerraformStateProperties.class
})
@ConditionalOnMissingBean(TerraformState.class)
public class TerraformStateAutoConfiguration {

    @Bean
    public TerraformState terraformState(TerrakubeClient terrakubeClient, TerraformStateProperties terraformStateProperties, AzureTerraformStateProperties azureTerraformStateProperties, AwsTerraformStateProperties awsTerraformStateProperties, GcpTerraformStateProperties gcpTerraformStateProperties, TerraformStatePathService terraformStatePathService) {
        TerraformState terraformState = null;

        if (terraformStateProperties != null)
            switch (terraformStateProperties.getType()) {
                case AzureTerraformStateImpl:
                    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                            .connectionString(
                                    String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
                                            azureTerraformStateProperties.getStorageAccountName(),
                                            azureTerraformStateProperties.getStorageAccessKey())
                            ).buildClient();

                    terraformState = AzureTerraformStateImpl.builder()
                            .resourceGroupName(azureTerraformStateProperties.getResourceGroupName())
                            .storageAccountName(azureTerraformStateProperties.getStorageAccountName())
                            .storageContainerName(azureTerraformStateProperties.getStorageContainerName())
                            .storageAccessKey(azureTerraformStateProperties.getStorageAccessKey())
                            .blobServiceClient(blobServiceClient)
                            .terrakubeClient(terrakubeClient)
                            .terraformStatePathService(terraformStatePathService)
                            .build();
                    break;
                case AwsTerraformStateImpl:
                    AwsBasicCredentials credentials = AwsBasicCredentials.create(
                            awsTerraformStateProperties.getAccessKey(),
                            awsTerraformStateProperties.getSecretKey()
                    );

                    S3Client s3client = null;
                    if (awsTerraformStateProperties.getEndpoint() != "") {
                        s3client = S3Client.builder()
                                .region(Region.of(awsTerraformStateProperties.getRegion()))
                                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                                .serviceConfiguration(S3Configuration.builder()
                                        .pathStyleAccessEnabled(true)
                                        .build())
                                .endpointOverride(URI.create(awsTerraformStateProperties.getEndpoint()))
                                .build();
                    } else
                        s3client = S3Client.builder()
                                .region(Region.of(awsTerraformStateProperties.getRegion()))
                                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                                .build();

                    terraformState = AwsTerraformStateImpl.builder()
                            .s3client(s3client)
                            .endpoint(awsTerraformStateProperties.getEndpoint() != "" ? awsTerraformStateProperties.getEndpoint(): null)
                            .bucketName(awsTerraformStateProperties.getBucketName())
                            .accessKey(awsTerraformStateProperties.getAccessKey())
                            .secretKey(awsTerraformStateProperties.getSecretKey())
                            .region(Region.of(awsTerraformStateProperties.getRegion()))
                            .includeBackendKeys(awsTerraformStateProperties.isIncludeBackendKeys())
                            .terrakubeClient(terrakubeClient)
                            .terraformStatePathService(terraformStatePathService)
                            .build();
                    break;
                case GcpTerraformStateImpl:
                    try {
                        log.info("GCP Credentials Base64 {} length", gcpTerraformStateProperties.getCredentials().length());
                        Credentials gcpCredentials = GoogleCredentials.fromStream(
                                new ByteArrayInputStream(
                                        Base64.decodeBase64(gcpTerraformStateProperties.getCredentials())
                                )
                        );
                        Storage gcpStorage = StorageOptions.newBuilder()
                                .setCredentials(gcpCredentials)
                                .setProjectId(gcpTerraformStateProperties.getProjectId())
                                .build()
                                .getService();

                        terraformState = GcpTerraformStateImpl.builder().storage(gcpStorage)
                                .terraformStatePathService(terraformStatePathService)
                                .bucketName(gcpTerraformStateProperties.getBucketName())
                                .credentials(gcpTerraformStateProperties.getCredentials())
                                .terrakubeClient(terrakubeClient)
                                .build();
                    } catch (IOException e) {
                        log.error(e.getMessage());
                    }
                    break;
                default:
                    terraformState = LocalTerraformStateImpl.builder()
                            .terrakubeClient(terrakubeClient)
                            .terraformStatePathService(terraformStatePathService)
                            .build();
            }
        else
            terraformState = LocalTerraformStateImpl.builder()
                    .terrakubeClient(terrakubeClient)
                    .terraformStatePathService(terraformStatePathService)
                    .build();
        return terraformState;
    }
}
