#!/bin/bash
mockgen --destination=./mockclient/client.go -source=../client.go -self_package="github.com/matrixorigin/matrixcube/components/prophet/mock/mockclient" -package="mockclient" 
#!/bin/bash
mockgen --destination=./mockjob/job.go -source=../config/config_job.go -self_package="github.com/matrixorigin/matrixcube/components/prophet/mock/mockjob" -package="mockjob" 