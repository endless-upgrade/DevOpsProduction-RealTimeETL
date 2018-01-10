pipeline {
  agent any
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
    stage('Test scalatest') {
      steps {
        sh 'sbt clean test'
        archiveArtifacts 'target/test-reports/*.xml'
      }
    }
    stage('Build') {
      steps {
        sh 'sbt clean assembly'
      }
    }
    stage('Deploy') {
      steps {
        echo 'Soooooooooo lets deploy this shit'
        sh 'sudo cp target/*/*.jar /opt/deploy/realTimeETL/'
        sh 'sudo cp -Rf conf/* /opt/deploy/realTimeETL/'
      }
    }
  }
}