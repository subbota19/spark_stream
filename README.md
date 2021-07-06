## Spark Stream HM

* Main code is located in `src/main/python`

# Cluster config

For setting cluster config variables please edit terraform scripts by path : `terraform/variables.tf`

# Terraform

1) First install CLI for Azure according to the [link](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)

2) Run below command for Azure auth: 
```
az login
```
3) Run below commands for deployment infrastructure::
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
```
4) Open the browser and insert a link from the block after above command


