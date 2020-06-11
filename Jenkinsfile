import java.text.SimpleDateFormat
pipeline {
 agent any
 environment {
  DATE = sh(script: "echo `date +%Y-%m-%d-%H-%M-%S`", returnStdout: true).trim()
  registry = "015146638904.dkr.ecr.eu-west-1.amazonaws.com/lynx-be"
  CLUSTER = "lynx-ecs"
  SERVICE_NAME = "backend"
  AWS_REGION = "eu-west-1"
  ECS_TASK = "lynx-be"
 }
 parameters {
  gitParameter branchFilter: 'origin/(.*)', defaultValue: 'master', name: 'BRANCH', type: 'PT_BRANCH'
 }
 stages {
  
  stage('Clone repository') {
   steps {
    echo "Branch name: ${params.BRANCH}"
    checkout([$class: 'GitSCM',
     branches: [
      [name: "${params.BRANCH}"]
     ],
     doGenerateSubmoduleConfigurations: false,
     extensions: [
      [$class: 'CleanCheckout']
     ],
     submoduleCfg: [],
     userRemoteConfigs: [
      [url: 'git@bitbucket.org:lynxmd/lynx-be.git']
     ]
    ])
   }
  }
  stage('Check migration') {
   steps {
    script {
     if ("${migration}" == "true") {
      displayMessage ("Installing requirements")
      sh 'export ENV=prod'
      sh 'pip3 install -r requirements.txt'
      displayMessage ("Finished Installation of requirements")
     }
    }
    steps {
        script {
        displayMessage("Running migration")
        sh 'python3 manage.py migrate'
        }
    }  
  }
  }
  stage('Building image') {
   steps {
    script {
     dockerImage = docker.build("${env.registry}:${env.DATE}")
    }
   }
  }
  stage('Push image') {
   steps {
    script {
     withAWS(region: 'eu-west-1') {
      def login = ecrLogin()
      sh login
      dockerImage.push("${env.DATE}")
      dockerImage.push("latest")
     }
    }
   }
  }
  stage('Update service') {
   steps {
    script {
     withAWS(region: 'eu-west-1') {
      sh "aws ecs update-service  --region ${env.AWS_REGION}  --cluster ${env.CLUSTER} --service ${env.SERVICE_NAME} --task-definition ${env.ECS_TASK} --force-new-deployment"
     }
    }
   }
  }
 }
}



def displayMessage(message) {
 ansiColor('xterm') {
  echo "\033[44m  ${message} \033[0m"
 }
}