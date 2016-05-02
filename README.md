# flowScale
A serverless utility for workflow-based scaling of AWS DynamoDB provisioned throughput.

[Dynamic DynamoDB](https://github.com/sebdah/dynamic-dynamodb) is a great tool for automating the throughput scaling of DynamoDB tables, but it is aimed more at scaling based on dynamic traffic and configured ratios of consumed : provisioned throughput values.

For our project, we realized a need for a more specific utility to enable scaling of DynamoDB provisioned throughput based on workflow steps.  This tool allows for:

 - workflow step specific throughput values grouped and defined as 'scenarios' that are configured in simple JSON

 - transitioning from one workflow scenario to the next by a simple REST call to AWS API Gateway, or calling an AWS Lambda function,  directly from inside the workflow runner.
 - a configuration value that disallows scale up events when the number of scale downs per UTC day has already reached 4.  This essentially limits the number of complete workflows per day to 4, (an inherent limitation of DynamoDB), but also effectively eliminates the following problems:

     - the cost waste caused by unused scaled-up tables hanging out until the UTC rollover before scaling down
     - (by extension) starting each day with a wasted scaled down on each table.
