package com.sks.minioclient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.minio.MinioClient;
import io.minio.ObjectStat;
import io.minio.PutObjectOptions;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidBucketNameException;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.RegionConflictException;
import io.minio.errors.XmlParserException;

public class MinIOClientDemonstrator {
    private enum MetaDataMapper {
        METAONE("x-amz-meta-metaone"), METATWO("x-amz-meta-metatwo");
        private final String key;

        MetaDataMapper(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

    private final String bucketName = "my-bucketname-one";
    private final String objectName = "my-objectName-one";

    private void uploadObject() {
        System.out.println(String.format("demonstrate - uploadObject - started"));
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();

            boolean bucketExists = client.bucketExists(bucketName);
            if (!bucketExists) {
                System.out.println(String.format("demonstrate - going to create bucket: " + bucketName));
                client.makeBucket(bucketName);
            } else {
                System.out.println(String.format("demonstrate - bucket exists already: " + bucketName));
            }
            // Create some content for the object.
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                builder.append("I am the objects repeated data at iteration: " + i);
                builder.append("---\n");
            }

            // Create a InputStream for object upload.
            ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));

            // Create User specific meta data
            Map<String, String> headerMap = new HashMap<>();
            headerMap.put(MetaDataMapper.METAONE.toString(), "Meta data one data pushed via PutObjectOptions");
            headerMap.put(MetaDataMapper.METATWO.toString(), "Meta data two data pushed via PutObjectOptions");
            PutObjectOptions options = new PutObjectOptions(bais.available(), -1);
            options.setHeaders(headerMap);

            client.putObject(bucketName, objectName, bais, options);
            bais.close();
            System.out.println("Uploaded successfully object: " + objectName);
        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | ErrorResponseException
                | IllegalArgumentException | InsufficientDataException | InternalException | InvalidBucketNameException
                | InvalidResponseException | NoSuchAlgorithmException | XmlParserException | IOException
                | RegionConflictException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("demonstrate - uploadObject - finished"));
    }

    private void getObject() {
        System.out.println(String.format("demonstrate - getObject - started"));
        InputStream in = null;
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();
            in = client.getObject(bucketName, objectName);
            byte[] buffer = new byte[16 * 1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer, 0, buffer.length)) >= 0) {
                System.out.println(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
            }
        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | ErrorResponseException
                | IllegalArgumentException | InsufficientDataException | InternalException | InvalidBucketNameException
                | InvalidResponseException | NoSuchAlgorithmException | XmlParserException | IOException e) {
            e.printStackTrace();
        } finally {
            if(in!=null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(String.format("demonstrate - getObject - finished"));
    }

    private void getMetaData() {
        System.out.println(String.format("demonstrate - getMetaData - started"));
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();

            ObjectStat objectStat = client.statObject(bucketName, objectName);
            Map<String, List<String>> metadataMap = objectStat.httpHeaders();
            {
                List<String> data1 = metadataMap.get(MetaDataMapper.METAONE.getKey());
                if (data1 == null) {
                    throw new Exception("Missing meta information for METAONE");
                }
                String value1 = data1.get(0);
                System.out
                        .println(String.format("Meta data key: %s value: %s", MetaDataMapper.METATWO.getKey(), value1));
            }
            {
                List<String> data2 = metadataMap.get(MetaDataMapper.METATWO.getKey());
                if (data2 == null) {
                    throw new Exception("Missing meta information for METATWO");
                }
                String value2 = data2.get(0);
                System.out
                        .println(String.format("Meta data key: %s value: %s", MetaDataMapper.METATWO.getKey(), value2));
            }

        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | ErrorResponseException
                | IllegalArgumentException | InsufficientDataException | InternalException | InvalidBucketNameException
                | InvalidResponseException | NoSuchAlgorithmException | XmlParserException | IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("demonstrate - getMetaData - finished"));
    }

    public void demonstrate() {
        uploadObject();
        getObject();
        getMetaData();
    }
}
