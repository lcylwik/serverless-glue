# Serverless Glue

Serverless-glue is an open source MIT licensed project, which has been able to grow thanks to the community. This project is the result of an idea that did not let it rest in oblivion and many hours of work after hours.

## Install

1. run `npm install --save-dev @lcylwik/serverless-glue`
2. add **@lcylwik/serverless-glue** in serverless.yml plugin section
    ```yml
    plugins:
        - "@lcylwik/serverless-glue"
    ```
## How it works

The plugin creates CloufFormation resources of your configuration before making the serverless deploy then add it to the serverless template.

So any glue-job deployed with this plugin is part of your stack too.

## How to configure your GlueJob(s)

Configure your glue jobs in the root of servelress.yml like this:

```yml
Glue:
  bucketDeploy: someBucket # Required
  createBucket: true # Optional, default = false
  createBucketConfig: # Optional 
    ACL: private # Optional, private | public-read | public-read-write | authenticated-read
    LocationConstraint: af-south-1
    GrantFullControl: 'STRING_VALUE' # Optional
    GrantRead: 'STRING_VALUE' # Optional
    GrantReadACP: 'STRING_VALUE' # Optional
    GrantWrite: 'STRING_VALUE' # Optional
    GrantWriteACP: 'STRING_VALUE' # Optional
    ObjectLockEnabledForBucket: true # Optional
    ObjectOwnership: BucketOwnerPreferred # Optional
  s3Prefix: some/s3/key/location/ # optional, default = 'glueJobs/'
  tempDirBucket: someBucket # optional, default = '{serverless.serviceName}-{provider.stage}-gluejobstemp'
  tempDirS3Prefix: some/s3/key/location/ # optional, default = ''. The job name will be appended to the prefix name
  jobs:
    - name: super-glue-job # Required
      scriptPath: src/script.py # Required script will be named with the name after '/' and uploaded to s3Prefix location
      Description: # Optional, string
      tempDir: true # Optional true | false
      type: spark # spark / pythonshell # Required
      glueVersion: python3-2.0 # Required python3-1.0 | python3-2.0 | python2-1.0 | python2-0.9 | scala2-1.0 | scala2-0.9 | scala2-2.0
      role: arn:aws:iam::000000000:role/someRole # Required
      MaxConcurrentRuns: 3 # Optional
      WorkerType: Standard # Optional, G.1X | G.2X
      NumberOfWorkers: 1 # Optional
      Connections: # Optional
        - some-conection-string
        - other-conection-string
      Timeout: # Optional, number
      MaxRetries: # Optional, number
      DefaultArguments: # Optional
        class: string # Optional
        scriptLocation: string # Optional
        extraPyFiles: string # Optional
        extraJars: string # Optional
        userJarsFirst: string # Optional
        usePostgresDriver: string # Optional
        extraFiles: string # Optional
        disableProxy: string # Optional
        jobBookmarkOption: string # Optional
        enableAutoScaling: string # Optional
        enableS3ParquetOptimizedCommitter: string # Optional
        enableRenameAlgorithmV2: string # Optional
        enableGlueDatacatalog: string # Optional
        enableMetrics: string # Optional
        enableContinuousCloudwatchLog: string # Optional
        enableContinuousLogFilter: string # Optional
        continuousLogLogGroup: string # Optional
        continuousLogLogStreamPrefix: string # Optional
        continuousLogConversionPattern: string # Optional
        enableSparkUi: string # Optional
        sparkEventLogsPath: string # Optional
        customArguments: # Optional; these are user-specified custom default arguments that are passed into cloudformation with a leading -- (required for glue)
          custom_arg_1: custom_value
          custom_arg_2: other_custom_value
      SupportFiles: # Optional
        - local_path: path/to/file/or/folder/ # Required if SupportFiles is given, you can pass a folder path or a file path
          s3_bucket: bucket-name-where-to-upload-files # Required if SupportFiles is given
          s3_prefix: some/s3/key/location/ # Required if SupportFiles is given
          execute_upload: True # Boolean, True to execute upload, False to not upload. Required if SupportFiles is given
      Tags:
        job_tag_example_1: example1
        job_tag_example_2: example2
  triggers:
    - name: some-trigger-name # Required
      Description: # Optional, string
      StartOnCreation: True # Optional, True or False
      schedule: 30 12 * * ? * # Optional, CRON expression. The trigger will be created with On-Demand type if the schedule is not provided.
      Tags:
        trigger_tag_example_1: example1     
      actions: # Required. One or more jobs to trigger
        - name: super-glue-job # Required
          args: # Optional
            custom_arg_1: custom_value
            custom_arg_2: other_custom_value
          timeout: 30 # Optional, if set, it overwrites specific jobs timeout when job starts via trigger

```

You can define a lot of jobs...

```yml
  Glue:
    bucketDeploy: someBucket
    jobs:
      - name: jobA
        scriptPath: scriptA
        ...
      - name: jobB
        scriptPath: scriptB
        ...

```

And a lot of triggers...

```yml
  Glue:
    triggers:
        - name:
            ...
        - name:
            ...

```

### Glue configuration parameters

|Parameter|Type|Description|Required|
|-|-|-|-|
|bucketDeploy|String|S3 Bucket name|true|
|createBucket|Boolean|If true, a bucket named as `bucketDeploy` will be created before. Helpful if you have not created the bucket first|false|
createBucketConfig|createBucketConfig| Bucket configuration for creation on S3 |false|
|s3Prefix|String|S3 prefix name|false|
|tempDirBucket|String|S3 Bucket name for Glue temporary directory. If dont pass argument the bucket'name will generates with pattern {serverless.serviceName}-{provider.stage}-gluejobstemp|false|
|tempDirS3Prefix|String|S3 prefix name for Glue temporary directory|false|
|jobs|Array|Array of glue jobs to deploy|true|

### CreateBucket confoguration parameters

|Parameter|Type|Description|Required|
|-|-|-|-|
|ACL|String|The canned ACL to apply to the bucket. Possible values include:<ul><li>private</li><li>public-read</li><li>public-read-write</li><li>authenticated-read</li></ul>|False|
|LocationConstraint|String| Specifies the Region where the bucket will be created. If you don't specify a Region, the bucket is created in the US East (N. Virginia) Region (us-east-1). Possible values are: <ul><li>af-south-1</li><li>ap-east-1</li><li>ap-northeast-1</li><li>ap-northeast-2</li><li>ap-northeast-3</li><li>ap-south-1</li><li>ap-southeast-1</li><li>ap-southeast-2</li><li>ca-central-1</li><li>cn-north-1</li><li>cn-northwest-1</li><li>EU</li><li>eu-central-1</li><li>eu-north-1</li><li>eu-south-1</li><li>eu-west-1</li><li>eu-west-2</li><li>eu-west-3</li><li>me-south-1</li><li>sa-east-1</li><li>us-east-2</li><li>us-gov-east-1</li><li>us-gov-west-1</li><li>us-west-1</li><li>us-west-2</li></ul>|false|
|GrantFullControl|String|Allows grantee the read, write, read ACP, and write ACP permissions on the bucket.|false|
|GrantRead|(String|Allows grantee to list the objects in the bucket.|false|
|GrantReadACP|String|Allows grantee to read the bucket ACL.|false|
|GrantWrite|String|Allows grantee to create new objects in the bucket. For the bucket and object owners of existing objects, also allows deletions and overwrites of those objects.|false|
|GrantWriteACP|String|Allows grantee to write the ACL for the applicable bucket.|false|
|ObjectLockEnabledForBucket|Boolean|Specifies whether you want S3 Object Lock to be enabled for the new bucket.|false|
|ObjectOwnership|String|The container element for object ownership for a bucket's ownership controls.Possible values include:<ul><li>BucketOwnerPreferred</li><li>ObjectWriter</li><li>BucketOwnerEnforced</li></ul>|false|

### Jobs configurations parameters

|Parameter|Type|Description|Required|
|-|-|-|-|
|name|String|name of job|true|
|Description|String|Description of the job|False|
|scriptPath|String|script path in the project|true|
|tempDir|Boolean|flag indicate if job required a temp folder, if true plugin create a bucket for tmp|false|
|type|String|Indicate if the type of your job. Values can use are : `spark` or  `pythonshell`|true|
|glueVersion|String|Indicate language and glue version to use ( `[language][version]-[glue version]`) the value can you use are: <ul><li>python3-1.0</li><li>python3-2.0</li><li>python2-1.0</li><li>python2-0.9</li><li>scala2-1.0</li><li>scala2-0.9</li><li>scala2-2.0</li></ul>|true|
|role|String| arn role to execute job|true|
|MaxConcurrentRuns|Double|max concurrent runs of the job|false|
|Connections|String|Database conections (for multiple connections use , for separetion)|false|
|WorkerType|String|The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X.|false|
|NumberOfWorkers|Integer|number of workers|false|
|Connections|List|a list of connections used by the job|false|
|DefaultArguments|object|Special Parameters Used by AWS Glue for mor information see this read the [AWS documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html)|false|
|SupportFiles|List|List of supporting files for the glue job that need upload to S3|false|
|Tags|JSON|The tags to use with this job. You may use tags to limit access to the job. For more information about tags in AWS Glue, see AWS Tags in AWS Glue in the developer guide.|false|

### Triggers configuration parameters

|Parameter|Type|Description|Required|
|-|-|-|-|
|name|String|name of the trigger|true|
|schedule|String|CRON expression|false|
|actions|Array|An array of jobs to trigger|true|
|Description|String|Description of the Trigger|False|
|StartOnCreation|Boolean|Whether the trigger starts when created. Not supperted for ON_DEMAND triggers|False|

Only On-Demand and Scheduled triggers are supported.

### Trigger job configuration parameters

|Parameter|Type|Description|Required|
|-|-|-|-|
|name|String|The name of the Glue job to trigger|true|
|timeout|Integer|Job execution timeout. It overwrites|false|
|args|Map|job arguments|false|
|Tags|JSON|The tags to use with this triggers. For more information about tags in AWS Glue, see AWS Tags in AWS Glue in the developer guide.|false|


## And now?...

Only run `serverless deploy`
---
