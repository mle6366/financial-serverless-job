# README #

Financial Portfolio Planning & Management Tools. 

This AWS Lambda function is responsible for building the portfolio from current holdings data stored in s3.


## Summary

This is one component of a larger, on-going financial data project. 
The larger project leverages multiple interconnected serverless components deployed in AWS. 

This portfolio function is deployed via the [serverless](https://serverless.com/framework/) framework and is scheduled to
execute at an interval via [Cloudwatch Events Scheduler](https://docs.aws.amazon.com/lambda/latest/dg/with-scheduled-events.html). 
(See serverless.yml line 83 for Event Scheduler)

Logging and errors can be view in Cloudwatch.

## Technologies
* Python 3
* [Pandas](https://pandas.pydata.org/), [NumPy](http://www.numpy.org/)
* [Serverless Framework](https://serverless.com/framework/)
* AWS Lambda 
* [Cloudwatch Events Scheduler](https://docs.aws.amazon.com/lambda/latest/dg/with-scheduled-events.html)

* Dependencies and Prerequisites: 
    * python 3
    * node version 8
    * valid AWS creds located in `~/.aws/credentials`. Checkout out this project's serverless.yml for roles.
 
## How do I get set up? ##

```
npm i -g serverless

sls plugin install -n serverless-python-requirements

cd {project-directory}
make

```

    
* How to run tests
    ```bash
    make clean init test
    ```
* Deployment instructions
```bash

make deploy

```

### Contribution guidelines ###

* Writing tests
    * Tests are written with [pytest](https://docs.pytest.org/en/latest/)
    * All major functions/methods should be tested
* Code review
    * All changes are incorporated into `master` via a pull request
* Other guidelines
