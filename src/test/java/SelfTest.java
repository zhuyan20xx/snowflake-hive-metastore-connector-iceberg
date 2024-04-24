import org.apache.hadoop.hive.ql.secrets.AWSSecretsManagerSecretSource;

import java.net.URI;

public class SelfTest {



    public static void main(String[] args) throws Exception{
            String fullPath = "viewfs://iubeta/apps/hive/warehouse/icebergdemo.db/test_iceberg_tb/metadata/00025-53fb9577-225e-486d-ad5f-eda160ad4255.metadata.json";

            // Find the index of "/metadata/" in the string
            int metadataIndex = fullPath.indexOf("/metadata/");

            // Extract the substring from "/metadata/" to the end
            String relativePathStr = fullPath.substring(metadataIndex + 1);

            System.out.println(relativePathStr);
            AWSSecretsManagerSecretSource source = new AWSSecretsManagerSecretSource();

            String passwd =  source.getSecret(new URI("aws-sm:///test-env-sf-benny"));
        }






}
