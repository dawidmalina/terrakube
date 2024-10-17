package org.terrakube.registry.plugin.storage.aws;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.terrakube.registry.plugin.storage.StorageService;
import org.terrakube.registry.service.git.GitService;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.IOException;

@Slf4j
@Builder
public class AwsStorageServiceImpl implements StorageService {

    private static String BUCKET_ZIP_MODULE_LOCATION = "registry/%s/%s/%s/%s/module.zip";
    private static String BUCKET_DOWNLOAD_MODULE_LOCATION = "%s/terraform/modules/v1/download/%s/%s/%s/%s/module.zip";

    @NonNull
    private S3Client s3client;

    @NonNull
    private String bucketName;

    @NonNull
    GitService gitService;

    @NonNull
    String registryHostname;

    private boolean doesObjectExist(String key) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            HeadObjectResponse headObjectResponse = s3client.headObject(headObjectRequest);
            return headObjectResponse != null;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public String searchModule(String organizationName, String moduleName, String providerName, String moduleVersion,
            String source, String vcsType, String vcsConnectionType, String accessToken, String tagPrefix,
            String folder) {
        String blobKey = String.format(BUCKET_ZIP_MODULE_LOCATION, organizationName, moduleName, providerName,
                moduleVersion);
        log.info("Checking Aws S3 Object exist {}", blobKey);

        if (!doesObjectExist(blobKey)) {
            File gitCloneDirectory = gitService.getCloneRepositoryByTag(source, moduleVersion, vcsType,
                    vcsConnectionType, accessToken, tagPrefix, folder);
            File moduleZip = new File(gitCloneDirectory.getAbsolutePath() + ".zip");
            ZipUtil.pack(gitCloneDirectory, moduleZip);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .build();

            s3client.putObject(putObjectRequest, RequestBody.fromFile(moduleZip));

            log.info("Upload Aws S3 Object completed", blobKey);
            try {
                FileUtils.cleanDirectory(gitCloneDirectory);
                if (FileUtils.deleteQuietly(moduleZip))
                    log.info("Successfully delete folder");
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }

        return String.format(BUCKET_DOWNLOAD_MODULE_LOCATION, registryHostname, organizationName, moduleName,
                providerName, moduleVersion);
    }

    @Override
    public byte[] downloadModule(String organizationName, String moduleName, String providerName,
            String moduleVersion) {
        byte[] data;
        log.info("Searching: /registry/{}/{}/{}/{}/module.zip", organizationName, moduleName, providerName,
                moduleVersion);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(String.format(BUCKET_ZIP_MODULE_LOCATION, organizationName, moduleName, providerName, moduleVersion))
                .build();
        ResponseBytes<GetObjectResponse> s3object = s3client.getObject(getObjectRequest, ResponseTransformer.toBytes());
        data = s3object.asByteArray();
        return data;
    }

}
