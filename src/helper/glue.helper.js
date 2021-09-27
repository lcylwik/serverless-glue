import { readFileSync } from 'fs'

import { getAWSCredentials } from "../util/serverless.util";
import GlueConnection from "../domain/glue-connection";
import GlueJob from "../domain/glue-job";
import GlueTrigger from "../domain/glue-trigger";
import GlueTriggerAction from "../domain/glue-trigger-action";
import { makeS3service } from "../util/aws.util";
import { toPascalCase } from '../util/string.util'

export default class GlueHelper {
    constructor(serverless) {
        this.serverless = serverless;
        this.tempDir = false;
    }

    /**
     * Upload Script to s3 bucket and return file destination
     * @param {string} fileScriptPath file path in proyect
     * @param {*} bucket bucket name
     * @param {*} keyPath key path in S3 Bucket
     */
    async uploadGlueScriptToS3(fileScriptPath, bucket, keyPath = '') {

        const credentials = getAWSCredentials(this.serverless)
        const s3Service = makeS3service(credentials);

        const fileName = fileScriptPath.split('/').pop();

        const params = {
            Body: readFileSync(`./${fileScriptPath}`),
            Bucket: bucket,
            Key: `${keyPath}${fileName}`
        }
        this.serverless.cli.log("Upload GlueJob Script to Bucket...");
        await s3Service.upload(params).promise();
        this.serverless.cli.log("Upload GlueJob Script to Bucket Done...");
        return `s3://${bucket}/${keyPath}${fileName}`
    }

    getPluginConfig() {
        return this.serverless.service.custom.Glue
    }

    getAccountId() {
        return this.serverless.service.custom.accountId
    }

    /**
     * Get GlueJobs configured in serverless.yml
     * @param {Object} config plugin config
     */
    async getGlueJobs(config) {
        const arrayJobsJSON = config.jobs;
        const s3KeyPrefix = config.s3Prefix ? config.s3Prefix : 'glueJobs/';

        const tempDirBucket = config.tempDirBucket;
        const tempDirS3Prefix = config.tempDirS3Prefix;

        const jobs = [];
        for (const job of arrayJobsJSON) {
            const _job = job.job
            const glueJob = new GlueJob(_job.name, _job.script);
            const s3Url = await this.uploadGlueScriptToS3(_job.script, config.bucketDeploy, s3KeyPrefix);
            glueJob.setS3ScriptLocation(s3Url);
            glueJob.setGlueVersion(_job.glueVersion);
            glueJob.setRole(_job.role);
            glueJob.setType(_job.type);
            glueJob.setCommandName(_job.type);
            if (_job.MaxConcurrentRuns) {
                glueJob.setMaxConcurrentRuns(_job.MaxConcurrentRuns)
            }
            if (_job.WorkerType) {
                glueJob.setWorkerType(_job.WorkerType);
            }
            if (_job.NumberOfWorkers) {
                glueJob.setNumberOfWorkers(_job.NumberOfWorkers)
            }
            if (_job.Connections) {
                glueJob.setConnections(_job.Connections.split(","));
            }
            if (_job.tempDir) {
                this.tempDir = true;

                // use the provided temp dir bucket if configured
                const jobTempDirBucket = tempDirBucket || { "Ref": "GlueJobTempBucket" };

                // use the provided s3 prefix if configured
                let jobTempDirS3Prefix = "";
                if (tempDirS3Prefix) {
                    jobTempDirS3Prefix += `/${tempDirS3Prefix}`
                }
                jobTempDirS3Prefix += `/${_job.name}`;

                glueJob.setTempDir({
                    "Fn::Join": [
                        "", ["s3://", jobTempDirBucket, jobTempDirS3Prefix]
                    ]
                })
            }

            jobs.push(glueJob);
        }
        return jobs;
    }

    /**
     * Get Glue Connections configured in serverless.yml
     * @param {Object} config plugin config
     */
    async getGlueConnections(config) {
        const accountId = this.getAccountId();
        const arrayConnectionsJSON = config.connections;
        this.serverless.cli.log("Get glue connections config", arrayConnectionsJSON);

        const connections = [];
        for (const item of arrayConnectionsJSON) {
            const _connection = item.connection
            const glueConnection = new GlueConnection(_connection.name, accountId);
            glueConnection.setType(_connection.connectionType);
            glueConnection.setName(_connection.name);

            glueConnection.setDBUri(_connection.dbUri);
            glueConnection.setDBUsername(_connection.dbUsername);
            glueConnection.setDBPassword(_connection.dbPassword);

            if(_connection.description) {
                glueConnection.setDescription(_connection.description);
            }

            if (_connection.MatchCriteria) {
                glueConnection.setMatchCriteria(_connection.MatchCriteria.split(","));
            }

            if(_connection.securityGroupIdList) {
                glueConnection.setSecurityGroup(_connection.securityGroupIdList.split(","));
            }

            if(_connection.subnetId) {
                glueConnection.setDescription(_connection.subnetId);
            }

            connections.push(glueConnection);
        }
        return connections;
    }

    /**
     * Get GlueJobTriggers configured in serverless.yml
     * @param {Object} config plugin config
     */
    async getGlueTriggers(config) {
        let triggers = [];
        try {
            let arrayTriggersJSON = config.triggers;

            for (let trigger of arrayTriggersJSON) {
                let _trigger = trigger.trigger;
                let glueTrigger = new GlueTrigger(_trigger.name, _trigger.schedule);
                let glueTriggerActions = []
                for (let job of _trigger.jobs) {
                    let _job = job.job;
                    const triggerAction = new GlueTriggerAction(_job.name);
                    if (_job.args) {
                        triggerAction.setArguments(_job.args);
                    }
                    if (_job.timeout) {
                        triggerAction.setTimeout(_job.timeout);
                    }
                    glueTriggerActions.push(triggerAction);
                }
                glueTrigger.setActions(glueTriggerActions);
                triggers.push(glueTrigger);
            }
        } catch (err) {
            console.log(`No Trigger configuration`);
        } finally {
            return triggers;
        }
    }

    async run() {
        const config = this.getPluginConfig();

        const connections = await this.getGlueConnections(config);
        const jobs = await this.getGlueJobs(config);
        const triggers = await this.getGlueTriggers(config);

        const template = this.serverless.service.provider.compiledCloudFormationTemplate.Resources;
        const outputs = this.serverless.service.provider.compiledCloudFormationTemplate.Outputs;
        this.serverless.cli.log("Building GlueJobs CloudFormation");
        for (const connection of connections) {
            template[toPascalCase(connection.name)] = connection.getCFGlueConnection();
        }
        for (const job of jobs) {
            template[toPascalCase(job.name)] = job.getCFGlueJob();
        }
        for (const trigger of triggers) {
            template[toPascalCase(trigger.name)] = trigger.getCFGlueTrigger();
        }

        if (this.tempDir && !config.tempDirBucket) {
            this.serverless.cli.log("Building S3 Temp Bucket CloudFormation");
            const tempBucket = {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": `${this.serverless.service.service}-${this.serverless.service.provider.stage}-gluejobstemp`
                }
            }

            template[`GlueJobTempBucket`] = tempBucket;
            outputs[`GlueJobTempBucketName`] = { "Value": "GlueJobTempBucket" }
        }
    }
}
