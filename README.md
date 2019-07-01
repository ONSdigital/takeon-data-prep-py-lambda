Data prep lambda in python.

The environment variables required are  - 
DATABASE_USER
DATABASE_PASSWORD
DATABASE_HOST
DATABASE_NAME
WRANGLER_NAME

The expected format for the incoming json is:
{
  "reference": "value1",
  "period": "value2",
  "survey": "value3",
  "instance": "instanceId"
}


The following processings are involved in this Lambda

   1. With the environment variables information it tries to connect to aws Postgres database
   2. If connection is successful, it will get the questionCode and response values from the database against the provided input information. Then it will update the input json with questionCode and response info.
      If connection is not successful, it will log the error and exits.
   3. Then it calls the wrangler lambda with the updated input information generated at step 2.
   

Deployment:

This lambda is currently deployed to: https://eu-west-2.console.aws.amazon.com/lambda/home?region=eu-west-2#/functions/takeon-data-prep-py-lambda-dev-run_data_prep?tab=graph
   
This lambda can be auto deployed with serverless framework with commands - sls deploy (from the local git repo)

To deploy with serverless, it will require a config.dev.json file that should hold the following variables' values -
LAYER_NAME
ROLE
SECURITY_GROUP_ID
SUBNET_ID
DB_USER
DB_PW
DB_HOST
DB_NAME

With the above variables value, it will automatically deploy by using the corresponding Lambda Layer, role, VPC, subnets and security groups.
