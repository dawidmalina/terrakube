package org.terrakube.api.plugin.storage.aws;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.StringUtils;
import org.terrakube.api.plugin.storage.StorageTypeService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Builder
public class AwsStorageTypeServiceImpl implements StorageTypeService {

    private static final String TERRAFORM_PLAN_FILE = "terraformLibrary.tfPlan";
    private static final String BUCKET_LOCATION_OUTPUT = "tfoutput/%s/%s/%s.tfoutput";
    private static final String BUCKET_STATE_LOCATION = "tfstate/%s/%s/%s/%s/" + TERRAFORM_PLAN_FILE;

    private static final String BUCKET_STATE_JSON = "tfstate/%s/%s/state/%s.json";
    private static final String CONTEXT_JSON = "tfoutput/context/%s/context.json";

    private static final String S3_ERROR_LOG = "S3 Not found: {}";

    private static final String TERRAFORM_TAR_GZ = "content/%s/terraformContent.tar.gz";

    @NonNull
    private S3Client s3client;

    @NonNull
    private String bucketName;

    @Override
    public byte[] getStepOutput(String organizationId, String jobId, String stepId) {
        byte[] data;
        try {
            log.info("Searching: tfoutput/{}/{}/{}.tfoutput", organizationId, jobId, stepId);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(String.format(BUCKET_LOCATION_OUTPUT, organizationId, jobId, stepId))
                    .build();
    
            ResponseBytes<GetObjectResponse> objectBytes = s3client.getObject(getObjectRequest, ResponseTransformer.toBytes());
            data = objectBytes.asByteArray();
        } catch (Exception e) {
            log.error(S3_ERROR_LOG, e.getMessage());
            data = new byte[0];
        }
        return data;
    }

    @Override
    public byte[] getTerraformPlan(String organizationId, String workspaceId, String jobId, String stepId) {
        byte[] data;
        try {
            log.info("Searching: tfstate/{}/{}/{}/{}/terraformLibrary.tfPlan", organizationId, workspaceId, jobId, stepId);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(String.format(BUCKET_STATE_LOCATION, organizationId, workspaceId, jobId, stepId))
                    .build();
            
            ResponseBytes<GetObjectResponse> objectBytes = s3client.getObject(getObjectRequest, ResponseTransformer.toBytes());
            data = objectBytes.asByteArray();
        } catch (Exception e) {
            log.error(S3_ERROR_LOG, e.getMessage());
            data = new byte[0];
        }
        return data;
    }

    @Override
    public byte[] getTerraformStateJson(String organizationId, String workspaceId, String stateFileName) {
        byte[] data;
        try {
            log.info("Searching: tfstate/{}/{}/state/{}.json", organizationId, workspaceId, stateFileName);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(String.format(BUCKET_STATE_JSON, organizationId, workspaceId, stateFileName))
                    .build();
            
            ResponseBytes<GetObjectResponse> objectBytes = s3client.getObject(getObjectRequest, ResponseTransformer.toBytes());
            data = objectBytes.asByteArray();
        } catch (Exception e) {
            log.error(S3_ERROR_LOG, e.getMessage());
            data = new byte[0];
        }
        return data;
    }

    @Override
    public void uploadTerraformStateJson(String organizationId, String workspaceId, String stateJson, String stateJsonHistoryId) {
        String blobKey = String.format("tfstate/%s/%s/state/%s.json", organizationId, workspaceId, stateJsonHistoryId);
        log.info("terraformJsonStateFile: {}", blobKey);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();

        s3client.putObject(putObjectRequest, RequestBody.fromString(stateJson));
    }

    @Override
    public byte[] getCurrentTerraformState(String organizationId, String workspaceId) {
        byte[] data;
        try {
            log.info("Searching: tfstate/{}/{}/terraform.tfstate", organizationId, workspaceId);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(String.format("tfstate/%s/%s/terraform.tfstate", organizationId, workspaceId))
                    .build();
            
            ResponseBytes<GetObjectResponse> objectBytes = s3client.getObject(getObjectRequest, ResponseTransformer.toBytes());
            data = objectBytes.asByteArray();
        } catch (Exception e) {
            log.error(S3_ERROR_LOG, e.getMessage());
            data = new byte[0];
        }
        return data;
    }

    @Override
    public void uploadState(String organizationId, String workspaceId, String terraformState, String historyId) {
        String blobKey = String.format("tfstate/%s/%s/terraform.tfstate", organizationId, workspaceId);
        String rawBlobKey = String.format("tfstate/%s/%s/state/%s.raw.json", organizationId, workspaceId, historyId);
        log.info("terraformStateFile: {}", blobKey);
        log.info("terraformRawStateFile: {}", rawBlobKey);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        s3client.putObject(putObjectRequest, RequestBody.fromString(terraformState));

        putObjectRequest = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(rawBlobKey)
            .build();
        s3client.putObject(putObjectRequest, RequestBody.fromString(terraformState));
    }

    @Override
    public String saveContext(int jobId, String jobContext) {
        String blobKey = String.format(CONTEXT_JSON, jobId);
        log.info("context file: {}", String.format(CONTEXT_JSON, jobId));

        byte[] bytes = StringUtils.getBytesUtf8(jobContext);
        String utf8EncodedString = StringUtils.newStringUtf8(bytes);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .build();
        s3client.putObject(putObjectRequest, RequestBody.fromString(utf8EncodedString));

        return jobContext;
    }

    @Override
    public String getContext(int jobId) {
        String data;
        try {
            log.info("Searching: /tfoutput/context/{}/context.json", jobId);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(String.format(CONTEXT_JSON, jobId))
                    .build();
            
            ResponseBytes<GetObjectResponse> objectBytes = s3client.getObject(getObjectRequest, ResponseTransformer.toBytes());
            data = objectBytes.asByteArray().toString();

        } catch (Exception e) {
            log.error(S3_ERROR_LOG, e.getMessage());
            data = "{}";
        }
        return data;
    }

    @Override
    public void createContentFile(String contentId, InputStream inputStream) {
        String blobKey = String.format(TERRAFORM_TAR_GZ, contentId);
        log.info("context file: {}", String.format(TERRAFORM_TAR_GZ, contentId));

        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(blobKey)
                .contentType("application/gzip")
                .build();

            s3client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, inputStream.available()));
        } catch (IOException e) {
            log.error("Error uploading content file: {}", e.getMessage());
        }

    }

    @Override
    public byte[] getContentFile(String contentId) {
        byte[] data;
        try {
            log.info("Searching: content/{}/terraformContent.tar.gz", contentId);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(String.format(TERRAFORM_TAR_GZ, contentId))
                    .build();
            
            ResponseBytes<GetObjectResponse> objectBytes = s3client.getObject(getObjectRequest, ResponseTransformer.toBytes());
            data = objectBytes.asByteArray();
        } catch (Exception e) {
            log.error(S3_ERROR_LOG, e.getMessage());
            data = "".getBytes(Charset.defaultCharset());
        }
        return data;
    }

    @Override
    public void deleteModuleStorage(String organizationName, String moduleName, String providerName) {
        String registryPath = String.format("registry/%s/%s/%s/", organizationName, moduleName, providerName);
        deleteFolderFromBucket(registryPath);
    }

    @Override
    public void deleteWorkspaceOutputData(String organizationId, List<Integer> jobList) {
        for (Integer jobId: jobList){
            String workspaceOutputFolder = String.format("tfoutput/%s/%s/", organizationId, jobId);
            deleteFolderFromBucket(workspaceOutputFolder);
        }
    }

    @Override
    public void deleteWorkspaceStateData(String organizationId, String workspaceId) {
        String workspaceStateFolder = String.format("tfstate/%s/%s/", organizationId, workspaceId);
        deleteFolderFromBucket(workspaceStateFolder);
    }

    private void deleteFolderFromBucket(String prefix) {
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        ListObjectsV2Response listObjectsResponse = s3client.listObjectsV2(listObjectsRequest);
        
        for (S3Object s3Object : listObjectsResponse.contents()) {
            log.warn("File {} will be deleted.", s3Object.key());
        }

        List<ObjectIdentifier> objects = listObjectsResponse.contents().stream()
                .map(S3Object::key)
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());

        Delete delete = Delete.builder()
                .objects(objects)
                .build();

        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder().bucket(bucketName).delete(delete).build();
        s3client.deleteObjects(deleteObjectsRequest);
    }
}
