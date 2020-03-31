import java.text.SimpleDateFormat
import groovy.transform.Field


node {

    def DOCKER_IMAGE="lynx-be"
    def registryurl = "015146638904.dkr.ecr.eu-west-1.amazonaws.com/lynx-be"
    def AWS_REGION="eu-west-1"
    def ECS_CLUSTER="lynx-ecs"
    def ECS_TASK="lynx-be"
    def ECS_SERVICE="backend"

    stage('Clone repository') {
        /* Let's make sure we have the repository cloned to our workspace */
        checkout scm
    }

    stage('Building image') {
          script {
		        displayMessage("Building Docker ${dockerProject}")
                dockerImage = docker.build registryurl

          }

   }

   stage('Push image') {
       script {
            String currDate = current_timestamp().toString()
            displayMessage("Pushing docker image for ${dockerProject}")
            withDockerRegistry([url: registryurl]) {
	                     dockerImage.push("latest")
        	             dockerImage.push(currDate)
              }
    }
   }
   stage('Deployment') {
       script {
			displayMessage("Processing deployment")
			sh '''
		    aws ecs update-service  --cluster ${ECS_CLUSTER} --service ${ECS_SERVICE} --task-definition ${ECS_TASK} --force-new-deployment --region eu-central-1
		    '''
            }
        }


}

def displayMessage(message) {
    ansiColor('xterm') {
        echo "\033[44m  ${message} \033[0m"
    }
}


def current_timestamp() {
   def date = new Date()
   currDate = new SimpleDateFormat("ddMMyyyyHHmm")
   out = currDate.format(date)
   return out
}
