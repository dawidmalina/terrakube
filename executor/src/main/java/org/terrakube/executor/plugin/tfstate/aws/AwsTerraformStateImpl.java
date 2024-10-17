package org.terrakube.executor.plugin.tfstate.aws;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.text.TextStringBuilder;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.terrakube.client.TerrakubeClient;
import org.terrakube.client.model.organization.workspace.history.History;
import org.terrakube.client.model.organization.workspace.history.HistoryAttributes;
import org.terrakube.client.model.organization.workspace.history.HistoryRequest;
import org.terrakube.executor.plugin.tfstate.TerraformState;
import org.terrakube.executor.plugin.tfstate.TerraformStatePathService;
import org.terrakube.executor.service.mode.TerraformJob;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Builder
@Getter
@Setter
@Slf4j
public class AwsTerraformStateImpl implements TerraformState {

    private static final String TERRAFORM_PLAN_FILE = "terraformLibrary.tfPlan";
    private static final String BACKEND_FILE_NAME = "azure_backend_override.tf";

    @NonNull
    private S3Client s3client;

    @NonNull
    private String bucketName;

    private Region region;

    private String accessKey;

    private String secretKey;

    private String endpoint;

    private boolean includeBackendKeys;

    @NonNull
    TerrakubeClient terrakubeClient;

    @NonNull
    TerraformStatePathService terraformStatePathService;

    @Override
    public String getBackendStateFile(String organizationId, String workspaceId, File workingDirectory, String terraformVersion) {
        log.info("Generating backend override file for terraform {}", terraformVersion);
        ComparableVersion version = new ComparableVersion(terraformVersion);

        String awsBackend = BACKEND_FILE_NAME;
        try {
            TextStringBuilder awsBackendHcl = new TextStringBuilder();
            awsBackendHcl.appendln("terraform {");
            awsBackendHcl.appendln("  backend \"s3\" {");
            awsBackendHcl.appendln("    bucket     = \"" + bucketName + "\"");
            awsBackendHcl.appendln("    region     = \"" + region.id() + "\"");
            awsBackendHcl.appendln("    key        = \"tfstate/" + organizationId + "/" + workspaceId + "/terraform.tfstate" + "\"");
            if(includeBackendKeys) {
                log.info("Including backend information");
                awsBackendHcl.appendln("    access_key = \"" + accessKey + "\"");
                awsBackendHcl.appendln("    secret_key = \"" + secretKey + "\"");
            } else {
              log.warn("No including backend information");
            }

            if(endpoint != null){
                if(version.compareTo(new ComparableVersion("1.6.0")) < 0){
                    awsBackendHcl.appendln("    endpoint  = \"" + endpoint + "\"");
                    awsBackendHcl.appendln("    force_path_style             = true");
                } else {
                    awsBackendHcl.appendln("    endpoints = {");
                    awsBackendHcl.appendln("       s3 = \"" + endpoint + "\"");
                    awsBackendHcl.appendln("    }");
                    awsBackendHcl.appendln("    skip_requesting_account_id = true");
                    awsBackendHcl.appendln("    skip_s3_checksum = true");
                    awsBackendHcl.appendln("    use_path_style = true");
                }

                awsBackendHcl.appendln("    skip_credentials_validation  = true");
                awsBackendHcl.appendln("    skip_metadata_api_check      = true");
                awsBackendHcl.appendln("    skip_region_validation       = true");

            }

            awsBackendHcl.appendln("  }");
            awsBackendHcl.appendln("}");

            File awsBackendFile = new File(
                    FilenameUtils.separatorsToSystem(
                            workingDirectory.getAbsolutePath().concat("/").concat(BACKEND_FILE_NAME)
                    )
            );
            FileUtils.writeStringToFile(awsBackendFile, awsBackendHcl.toString(), Charset.defaultCharset());

        } catch (IOException e) {
            log.error(e.getMessage());
            awsBackend = null;
        }
        return awsBackend;
    }

    @Override
    public String saveTerraformPlan(String organizationId, String workspaceId, String jobId, String stepId, File workingDirectory) {

        String blobKey = "tfstate/" + organizationId + "/" + workspaceId + "/" + jobId + "/" + stepId + "/" + TERRAFORM_PLAN_FILE;
        log.info("terraformStateFile: {}", blobKey);

        File tfPlanContent = new File(workingDirectory.getAbsolutePath() + "/" + TERRAFORM_PLAN_FILE);
        log.info("terraformStateFile Path: {} {}", workingDirectory.getAbsolutePath() + "/" + TERRAFORM_PLAN_FILE, tfPlanContent.exists());
        if (tfPlanContent.exists()) {
            s3client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(blobKey)
                            .build(),
                    RequestBody.fromFile(tfPlanContent)
            );

            return String.format("https://%s.s3.%s.amazonaws.com/%s", bucketName, region.id(), blobKey);
        } else {
            return null;
        }
    }

    @Override
    public boolean downloadTerraformPlan(String organizationId, String workspaceId, String jobId, String stepId, File workingDirectory) {
        AtomicBoolean planExists = new AtomicBoolean(false);
        Optional.ofNullable(terrakubeClient.getJobById(organizationId, jobId).getData().getAttributes().getTerraformPlan())
                .ifPresent(stateUrl -> {
                    try {
                        log.info("Downloading state from {}", stateUrl);

                        //log.info("Buket location: {}", new URL(stateUrl).getPath().replace(endpoint != null ? bucketName + "/tfstate" :"/tfstate","tfstate").substring(1));
                        log.info("Buket location: {}", "tfstate/" + new URL(stateUrl).getPath().split("/tfstate/")[1]);

                        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key("tfstate/" + new URL(stateUrl).getPath().split("/tfstate/")[1])
                                .build();
                        ResponseBytes<GetObjectResponse> objectBytes = s3client.getObjectAsBytes(getObjectRequest);
                        byte[] data = objectBytes.asByteArray();

                        FileUtils.copyToFile(
                                new ByteArrayInputStream(data),
                                new File(workingDirectory.getAbsolutePath() + "/" + TERRAFORM_PLAN_FILE));
                        planExists.set(true);
                    } catch (IOException e) {
                        log.error(e.getMessage());
                    }
                });
        return planExists.get();
    }

    @Override
    public void saveStateJson(TerraformJob terraformJob, String applyJSON, String rawState) {
        if (applyJSON != null) {
            String stateFilename = UUID.randomUUID().toString();
            String blobKey = "tfstate/" + terraformJob.getOrganizationId() + "/" + terraformJob.getWorkspaceId() + "/state/" + stateFilename + ".json";
            String blobKeyRaw = "tfstate/" + terraformJob.getOrganizationId() + "/" + terraformJob.getWorkspaceId() + "/state/" + stateFilename + ".raw.json";
            log.info("terraformStateFile: {}", blobKey);
            log.info("terraformRawStateFile: {}", blobKeyRaw);

            byte[] bytes = StringUtils.getBytesUtf8(applyJSON);
            byte[] rawBytes = StringUtils.getBytesUtf8(rawState);
            String utf8EncodedString = StringUtils.newStringUtf8(bytes);
            String rawUtf8EncodedString = StringUtils.newStringUtf8(rawBytes);

            s3client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(blobKey)
                            .build(),
                    RequestBody.fromString(utf8EncodedString)
            );

            s3client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(blobKeyRaw)
                            .build(),
                    RequestBody.fromString(rawUtf8EncodedString)
            );

            String stateURL = terraformStatePathService.getStateJsonPath(terraformJob.getOrganizationId(), terraformJob.getWorkspaceId(), stateFilename);

            HistoryRequest historyRequest = new HistoryRequest();
            History newHistory = new History();
            newHistory.setType("history");
            HistoryAttributes historyAttributes = new HistoryAttributes();
            historyAttributes.setOutput(stateURL);
            historyAttributes.setSerial(1);
            historyAttributes.setMd5("0");
            historyAttributes.setLineage("0");
            historyAttributes.setJobReference(terraformJob.getJobId());
            newHistory.setAttributes(historyAttributes);
            historyRequest.setData(newHistory);

            terrakubeClient.createHistory(historyRequest, terraformJob.getOrganizationId(), terraformJob.getWorkspaceId());
        }
    }

}