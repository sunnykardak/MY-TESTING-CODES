AWSTemplateFormatVersion: '2010-09-09'
Description: EC2 instance to test Kafka connectivity

Parameters:
  KeyName:
    Description: Existing EC2 KeyPair name
    Type: AWS::EC2::KeyPair::KeyName

Resources:
  KafkaTestInstance:
    Type: AWS::EC2::Instance
    Properties:
      InstanceType: t2.micro
      ImageId: ami-0c1c30571d2dae5c9   # Amazon Linux 2 (eu-west-1)
      KeyName: !Ref KeyName
      NetworkInterfaces:
        - AssociatePublicIpAddress: true
          DeviceIndex: 0
          SubnetId: subnet-0cddb06678a20d32f   # replace with your subnet
          GroupSet:
            - sg-0ac8dfc095eb5a903   # flink-bsp-kafka-sg

Outputs:
  InstancePublicIP:
    Description: Public IP of test instance
    Value: !GetAtt KafkaTestInstance.PublicIp
