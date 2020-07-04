package com.sks.minioclient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.minio.ErrorCode;
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
        METAONE("x-amz-meta-metaone"), METATWO("x-amz-meta-metatwo"), METATHREE("x-amz-meta-metathree");
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

    private final String bucketNameTwo = "hughes-redo-bucket";
    private final String objectNameTwo = "my-objectName-two";

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
            headerMap.put(MetaDataMapper.METATHREE.toString(), "1");
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
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(String.format("demonstrate - getObject - finished"));
    }

    private void getObjectWithExistenceCheck() {
        System.out.println(String.format("demonstrate - getObject - started"));
        InputStream in = null;
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();
            in = client.getObject(bucketName, "someobject");
            byte[] buffer = new byte[16 * 1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer, 0, buffer.length)) >= 0) {
                System.out.println(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
            }
        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | IllegalArgumentException
                | InsufficientDataException | InternalException | InvalidBucketNameException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | IOException e) {
            e.printStackTrace();
        } catch (ErrorResponseException e) {
            ErrorCode code = e.errorResponse().errorCode();
            if (code == ErrorCode.NO_SUCH_OBJECT) {
                System.out.println(String.format("demonstrate - getObject - NO_SUCH_OBJECT"));
            } else {
                e.printStackTrace();
            }
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(String.format("demonstrate - getObject - finished"));
    }

    private void uploadCollectionDataAsObject() {
        System.out.println(String.format("demonstrate - uploadCollectionDataAsObject - started"));
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();

            boolean bucketExists = client.bucketExists(bucketNameTwo);
            if (!bucketExists) {
                System.out.println(String.format("demonstrate - going to create bucket: " + bucketNameTwo));
                client.makeBucket(bucketNameTwo);
            } else {
                System.out.println(String.format("demonstrate - bucket exists already: " + bucketNameTwo));
            }

            Set<Integer> set = new HashSet<Integer>();
            set.add(1);
            set.add(2);
            set.add(3);
            set.add(4);
            set.add(5);
            Collection<Integer> col = set;

            ArrayList<Integer> alist = new ArrayList<>(col);
            byte[] bArray = null;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream objOstream = new ObjectOutputStream(baos);
            objOstream.writeObject(alist);
            // objOstream.close();
            bArray = baos.toByteArray();
            baos.close();

            InputStream is = new ByteArrayInputStream(bArray);
            PutObjectOptions options = new PutObjectOptions(is.available(), -1);
            client.putObject(bucketNameTwo, objectNameTwo, is, options);
            is.close();

            System.out.println("Uploaded successfully object: " + objectNameTwo);
        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | ErrorResponseException
                | IllegalArgumentException | InsufficientDataException | InternalException | InvalidBucketNameException
                | InvalidResponseException | NoSuchAlgorithmException | XmlParserException | IOException
                | RegionConflictException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("demonstrate - uploadCollectionDataAsObject - finished"));
    }

    private void getObjectAndModifyCollectionData() {
        System.out.println(String.format("demonstrate - getObjectAndModifyCollection - started"));
        InputStream in = null;
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();
            in = client.getObject(bucketNameTwo, objectNameTwo);

            ObjectInputStream ois = new ObjectInputStream(in);

            ArrayList<Integer> arrayRemoveList = new ArrayList<Integer>();
            arrayRemoveList.add(1);
            arrayRemoveList.add(2);

            ArrayList<Integer> arraylist = new ArrayList<Integer>();
            arraylist = (ArrayList) ois.readObject();
            Set set = new HashSet(arraylist);
            set.add(1);
            set.add(6);
            set.removeAll(arrayRemoveList);
            System.out.println(set);

        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | IllegalArgumentException
                | InsufficientDataException | InternalException | InvalidBucketNameException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | IOException e) {
            e.printStackTrace();
        } catch (ErrorResponseException e) {
            ErrorCode code = e.errorResponse().errorCode();
            if (code == ErrorCode.NO_SUCH_OBJECT) {
                System.out.println(String.format("demonstrate - getObjectAndModifyCollection - NO_SUCH_OBJECT"));
            } else {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(String.format("demonstrate - getObject - finished"));
    }

    private void uploadCollectionDataAsObjectSet() {
        System.out.println(String.format("demonstrate - uploadCollectionDataAsObject - started"));
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();

            boolean bucketExists = client.bucketExists(bucketNameTwo);
            if (!bucketExists) {
                System.out.println(String.format("demonstrate - going to create bucket: " + bucketNameTwo));
                client.makeBucket(bucketNameTwo);
            } else {
                System.out.println(String.format("demonstrate - bucket exists already: " + bucketNameTwo));
            }

            Set<Integer> set = new HashSet<Integer>();
            set.add(1);
            set.add(2);
            set.add(3);
            set.add(4);
            set.add(5);
            Collection<Integer> col = set;

            byte[] bArray = null;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream objOstream = new ObjectOutputStream(baos);
            objOstream.writeObject(set);
            // objOstream.close();
            bArray = baos.toByteArray();
            baos.close();

            InputStream is = new ByteArrayInputStream(bArray);
            PutObjectOptions options = new PutObjectOptions(is.available(), -1);
            client.putObject(bucketNameTwo, objectNameTwo, is, options);
            is.close();

            System.out.println("Uploaded successfully object: " + objectNameTwo);
        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | ErrorResponseException
                | IllegalArgumentException | InsufficientDataException | InternalException | InvalidBucketNameException
                | InvalidResponseException | NoSuchAlgorithmException | XmlParserException | IOException
                | RegionConflictException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("demonstrate - uploadCollectionDataAsObject - finished"));
    }

    private void getObjectAndModifyCollectionDataSet() {
        System.out.println(String.format("demonstrate - getObjectAndModifyCollection - started"));
        InputStream in = null;
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();
            in = client.getObject(bucketNameTwo, objectNameTwo);

            ObjectInputStream ois = new ObjectInputStream(in);

            ArrayList<Integer> arrayRemoveList = new ArrayList<Integer>();
            arrayRemoveList.add(1);
            arrayRemoveList.add(2);

            Set<Integer> set = new HashSet();
            set = (Set) ois.readObject();
            set.add(1);
            set.removeAll(arrayRemoveList);
            System.out.println(set);

        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | IllegalArgumentException
                | InsufficientDataException | InternalException | InvalidBucketNameException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | IOException e) {
            e.printStackTrace();
        } catch (ErrorResponseException e) {
            ErrorCode code = e.errorResponse().errorCode();
            if (code == ErrorCode.NO_SUCH_OBJECT) {
                System.out.println(String.format("demonstrate - getObjectAndModifyCollection - NO_SUCH_OBJECT"));
            } else {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(String.format("demonstrate - getObject - finished"));
    }

    private void modifyMetaObject() {
        System.out.println(String.format("demonstrate - modifyMetaObject - started"));
        InputStream in = null;
        try {
            MinioClient client = MinIOClientProvider.getInstance().getClient();
            in = client.getObject(bucketName, objectName);

            ObjectStat objectStat = client.statObject(bucketName, objectName);
            Map<String, List<String>> metadataMap = objectStat.httpHeaders();

            List<String> data1 = metadataMap.get(MetaDataMapper.METAONE.getKey());
            if (data1 == null) {
                throw new Exception("Missing meta information for METAONE");
            }
            String value1 = data1.get(0);
            System.out.println(String.format("Meta data key: %s value: %s", MetaDataMapper.METATWO.getKey(), value1));

            List<String> data2 = metadataMap.get(MetaDataMapper.METATWO.getKey());
            if (data2 == null) {
                throw new Exception("Missing meta information for METATWO");
            }
            String value2 = data2.get(0);
            System.out.println(String.format("Meta data key: %s value: %s", MetaDataMapper.METATWO.getKey(), value2));

            List<String> data3 = metadataMap.get(MetaDataMapper.METATHREE.getKey());
            if (data3 == null) {
                throw new Exception("Missing meta information for METATWO");
            }
            String value3 = data3.get(0);
            System.out.println(String.format("Meta data key: %s value: %s", MetaDataMapper.METATHREE.getKey(), value3));
            Long newValue3 = Long.parseLong(value3) + 1;

            PutObjectOptions options = new PutObjectOptions(in.available(), -1);
            Map<String, String> headerMap = new HashMap<String, String>();
            headerMap.put(MetaDataMapper.METAONE.toString(), value1);
            headerMap.put(MetaDataMapper.METATWO.toString(), value2);
            headerMap.put(MetaDataMapper.METATHREE.toString(), String.valueOf(newValue3));
            options.setHeaders(headerMap);
            client.putObject(bucketName, objectName, in, options);

        } catch (InvalidEndpointException | InvalidPortException | InvalidKeyException | ErrorResponseException
                | IllegalArgumentException | InsufficientDataException | InternalException | InvalidBucketNameException
                | InvalidResponseException | NoSuchAlgorithmException | XmlParserException | IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(String.format("demonstrate - modifyMetaObject - finished"));
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
            {
                List<String> data3 = metadataMap.get(MetaDataMapper.METATHREE.getKey());
                if (data3 == null) {
                    throw new Exception("Missing meta information for METATWO");
                }
                String value3 = data3.get(0);
                System.out.println(
                        String.format("Meta data key: %s value: %s", MetaDataMapper.METATHREE.getKey(), value3));
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
        getObjectWithExistenceCheck();
        uploadCollectionDataAsObject();
        getObjectAndModifyCollectionData();
        uploadCollectionDataAsObjectSet();
        getObjectAndModifyCollectionDataSet();
        modifyMetaObject();
        getMetaData();
    }
}
