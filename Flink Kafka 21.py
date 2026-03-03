Perfect 👍 Java 21 + Maven is fine.

We’ll build a proper shaded JAR compatible with Flink 1.18 (which runs on Java 11 runtime).
So we will compile with target 11 even if you have Java 21 installed.

Follow exactly step-by-step.

⸻

✅ STEP 1 — Create Maven Project

Open terminal:

mvn archetype:generate ^
  -DgroupId=com.test ^
  -DartifactId=flink-access-test ^
  -DarchetypeArtifactId=maven-archetype-quickstart ^
  -DinteractiveMode=false

Go inside:

cd flink-access-test


⸻

✅ STEP 2 — Replace pom.xml

Delete existing pom.xml.

Create new one and paste this FULL file:

<project xmlns="http://maven.apache.org/POM/4.0.0"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
 http://maven.apache.org/xsd/maven-4.0.0.xsd">

 <modelVersion>4.0.0</modelVersion>
 <groupId>com.test</groupId>
 <artifactId>flink-access-test</artifactId>
 <version>1.0</version>

 <properties>
   <maven.compiler.source>11</maven.compiler.source>
   <maven.compiler.target>11</maven.compiler.target>
 </properties>

 <dependencies>

   <!-- Flink -->
   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-streaming-java</artifactId>
     <version>1.18.0</version>
     <scope>provided</scope>
   </dependency>

   <!-- AWS SDK v2 -->
   <dependency>
     <groupId>software.amazon.awssdk</groupId>
     <artifactId>sts</artifactId>
     <version>2.25.20</version>
   </dependency>

   <dependency>
     <groupId>software.amazon.awssdk</groupId>
     <artifactId>glue</artifactId>
     <version>2.25.20</version>
   </dependency>

 </dependencies>

 <build>
   <plugins>

     <plugin>
       <groupId>org.apache.maven.plugins</groupId>
       <artifactId>maven-compiler-plugin</artifactId>
       <version>3.11.0</version>
       <configuration>
         <source>11</source>
         <target>11</target>
       </configuration>
     </plugin>

     <plugin>
       <groupId>org.apache.maven.plugins</groupId>
       <artifactId>maven-shade-plugin</artifactId>
       <version>3.4.1</version>
       <executions>
         <execution>
           <phase>package</phase>
           <goals>
             <goal>shade</goal>
           </goals>
           <configuration>
             <createDependencyReducedPom>false</createDependencyReducedPom>
             <transformers>
               <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                 <mainClass>com.test.App</mainClass>
               </transformer>
             </transformers>
           </configuration>
         </execution>
       </executions>
     </plugin>

   </plugins>
 </build>

</project>


⸻

✅ STEP 3 — Replace Java Code

Open:

src/main/java/com/test/App.java

Replace everything with:

package com.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.regions.Region;

public class App {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("===== STARTING FLINK ACCESS TEST =====");

        try {
            StsClient sts = StsClient.create();
            System.out.println("Caller Identity:");
            System.out.println(sts.getCallerIdentity());
        } catch (Exception e) {
            System.out.println("STS FAILED:");
            System.out.println(e.getMessage());
        }

        try {
            GlueClient glue = GlueClient.builder()
                    .region(Region.EU_WEST_2)
                    .build();

            glue.getDatabase(
                    GetDatabaseRequest.builder()
                            .name("fdk_uk_customer_db_iceberg")
                            .build()
            );

            System.out.println("SUCCESS: Database accessible");

        } catch (Exception e) {
            System.out.println("FAILED to access database:");
            System.out.println(e.getMessage());
        }

        env.execute("Flink IAM Access Test");
    }
}


⸻

✅ STEP 4 — Build the JAR

Run:

mvn clean package

After successful build, go to:

target/

You will see:

flink-access-test-1.0.jar

This is the shaded JAR (ready for Flink).

⸻

✅ STEP 5 — Upload to S3

Upload:

flink-access-test-1.0.jar

Directly in bucket root:

s3://flink-poc-app-nishikesh/

Do NOT put inside jars/ folder to avoid path mistakes.

⸻

✅ STEP 6 — Configure Managed Flink

In your Flink application:

Application code location:
	•	Bucket → flink-poc-app-nishikesh
	•	Path → flink-access-test-1.0.jar

Execution role:

svc-role-real-time-transformation-poc

Save.

Start application.

⸻

✅ STEP 7 — Check CloudWatch Logs

Go to:

Monitoring → CloudWatch Logs

You should see one of these:

⸻

✅ If Working

Caller Identity: assumed-role/svc-role-real-time-transformation-poc
SUCCESS: Database accessible


⸻

❌ If Lake Formation Block

AccessDeniedException


⸻

🎯 Now You Are Testing Exactly What You Wanted
	•	Real Flink runtime
	•	Real IAM execution role
	•	Real Glue call
	•	Real cross-account access

⸻

Once you build and deploy, send me:
	•	Build success message
	•	CloudWatch output

We’ll interpret it immediately.
