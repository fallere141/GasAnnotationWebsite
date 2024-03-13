#!/bin/bash
source /home/ec2-user/mpcs-cc/bin/activate
mkdir -p /home/ec2-user/.aws
echo "
[default]
region = us-east-1
output = json
"> /home/ec2-user/.aws/config
echo "[default]
aws_access_key_id = <key>
aws_secret_access_key = <chain>
" > /home/ec2-user/.aws/credentials
mkdir -p  /home/ec2-user/mpcs-cc/gas
aws s3 cp s3://mpcs-cc-students/zhangzx/gas_web_server.zip /home/ec2-user/mpcs-cc
unzip /home/ec2-user/mpcs-cc/gas_web_server.zip -d /home/ec2-user/mpcs-cc/
sudo chown -R ec2-user:ec2-user /home/ec2-user/mpcs-cc/gas
sudo chown -R ec2-user:ec2-user /home/ec2-user/mpcs-cc/gas
aws s3 cp s3://mpcs-cc-resources/ssl/privkey.pem /home/ec2-user/mpcs-cc
aws s3 cp s3://mpcs-cc-resources/ssl/fullchain.pem /home/ec2-user/mpcs-cc
sudo -u ec2-user /home/ec2-user/mpcs-cc/gas/web/run_gas.sh &
