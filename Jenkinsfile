failMessage = ""
pipeline {
  agent any
  environment {
    DEPLOY_TARGET = 'devops-worker'
  }
  stages {
    stage('Config System') {
      steps {
        echo 'Setup the system'
        echo 'wget, curl, java, sbt and spark are now installed by Config Management system :)'
      }
    }
    stage('Test the System') {
      steps {
        sh 'java -version'
        sh 'sbt about'
      }
    }
    stage('Unit Tests') {
      steps {
        sh 'sbt clean test'
        archiveArtifacts(artifacts: 'target/test-reports/*.xml', fingerprint: true)
        junit(testResults: 'target/test-reports/ETLSpec.xml', allowEmptyResults: true)
      }
    }
    stage('Build') {
      steps {
        sh 'sbt clean compile package assembly'
        archiveArtifacts 'target/scala-*/*.jar'
      }
    }
    stage('Staging Deploy') {
      steps {
        sh 'sudo cp target/*/*.jar /opt/deploy/realtime_etl'
        sh 'sudo cp -Rf conf/* /opt/deploy/realtime_etl'
        sh 'sudo cp target/*/*.jar /opt/staging/IntegrationStagingProject/lib'
      }
    }
    stage('Integration Tests') {
      steps {
        sh 'cd /opt/staging/IntegrationStagingProject/ && sbt clean test'
      }
    }
    stage('Deploy ?') {
      steps {
        script {
          header = "Job <${env.JOB_URL}|${env.JOB_NAME}> <${env.JOB_DISPLAY_URL}|(Blue)>"
          header += " build <${env.BUILD_URL}|${env.BUILD_DISPLAY_NAME}> <${env.RUN_DISPLAY_URL}|(Blue)>:"
          message = "${header}\n"
          author = sh(script: "git log -1 --pretty=%an", returnStdout: true).trim()
          commitMessage = sh(script: "git log -1 --pretty=%B", returnStdout: true).trim()
          message += " Commit by <@${author}> (${author}): ``` ${commitMessage} ``` "
          message += "--------------------------------------------------------------------"
          message += "\nThe new **Real Time ETL** commit pass Unit and Integration tests"
          message += "\nThis session will be available for 60 second, make a CHOICE!"
          message += "\nPlease <${env.RUN_DISPLAY_URL}|Manual Deploy> it if you want!"
          color = '#36ABCC'
          slackSend(message: message, baseUrl: 'https://devops-pasquali-cm.slack.com/services/hooks/jenkins-ci/', color: color, token: 'ihoCVUPB7hqGz2xI1htD8x0F')

          script{

            try {
              timeout(time: 60, unit: 'SECONDS') { // change to a convenient timeout for you
                userInput = input(
                        id: 'DeployRTETL', message: 'Deploy in Production??')
              }
            } catch(err) { // timeout reached or input false
              failMessage = "Deploy session expired or aborted"
              error("Deploy session expired or aborted")
            }
          }
        }
      }
    }
    stage('Production Deploy') {
      steps {
        echo 'Safe to Deploy in Production, Great Job :D'
        sh "sudo ansible-playbook -i \'${DEPLOY_TARGET},\' --private-key=/home/xxpasquxx/.ssh/ansible_rsa_key /opt/DevOpsProduction-Orchestrator/ansible/deploy/realtime_etl_deploy.yml  -e \'ansible_ssh_user=xxpasquxx\' -e \'host_key_checking=False\'"
      }
    }
  }
  post {
    success {
      script {
        header = "Job <${env.JOB_URL}|${env.JOB_NAME}> <${env.JOB_DISPLAY_URL}|(Blue)>"
        header += " build <${env.BUILD_URL}|${env.BUILD_DISPLAY_NAME}> <${env.RUN_DISPLAY_URL}|(Blue)>:"
        message = "${header}\n :smiley: New Batch ETL release deployed in Production"

        author = sh(script: "git log -1 --pretty=%an", returnStdout: true).trim()
        commitMessage = sh(script: "git log -1 --pretty=%B", returnStdout: true).trim()
        message += " Commit by <@${author}> (${author}): ``` ${commitMessage} ``` "
        color = '#00CC00'
      }

      echo "Message ${message}"
      slackSend(message: message, baseUrl: 'https://devops-pasquali-cm.slack.com/services/hooks/jenkins-ci/', color: color, token: 'ihoCVUPB7hqGz2xI1htD8x0F')

    }

    failure {
      script {
        header = "Job <${env.JOB_URL}|${env.JOB_NAME}> <${env.JOB_DISPLAY_URL}|(Blue)>"
        header += " build <${env.BUILD_URL}|${env.BUILD_DISPLAY_NAME}> <${env.RUN_DISPLAY_URL}|(Blue)>:"
        message = "${header}\nThe Build Failed, Release not ready for production!: ``` ${failMessage} ```\n"

        author = sh(script: "git log -1 --pretty=%an", returnStdout: true).trim()
        commitMessage = sh(script: "git log -1 --pretty=%B", returnStdout: true).trim()
        message += " Commit by <@${author}> (${author}): ``` ${commitMessage} ``` "
        color = '#990000'
      }

      echo "Message ${message}"
      slackSend(message: message, baseUrl: 'https://devops-pasquali-cm.slack.com/services/hooks/jenkins-ci/', color: color, token: 'ihoCVUPB7hqGz2xI1htD8x0F')

    }
  }
}