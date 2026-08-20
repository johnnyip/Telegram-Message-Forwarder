pipeline {
  agent any
  options { skipDefaultCheckout(true); disableConcurrentBuilds(); timestamps(); timeout(time: 25, unit: 'MINUTES'); buildDiscarder(logRotator(numToKeepStr: '30')) }
  parameters { booleanParam(name: 'PUBLISH_IMAGES', defaultValue: true, description: 'Push latest and immutable image tags.') }
  environment { IMAGE = 'johnnyip/telegram-message-forwarder' }
  stages {
    stage('Checkout') { steps { script { def v = checkout scm; env.SHORT_SHA = sh(script: 'git rev-parse --short=12 HEAD', returnStdout: true).trim(); env.SOURCE_BRANCH = (env.BRANCH_NAME ?: v.GIT_BRANCH ?: env.GIT_BRANCH ?: 'main').replaceFirst(/^origin\//, ''); currentBuild.description = "${env.SOURCE_BRANCH} @ ${env.SHORT_SHA}${params.PUBLISH_IMAGES ? ' · publish' : ' · validate'}" } } }
    stage('Build Image') { steps { sh 'docker build --pull --build-arg TELETHON_NO_UPDATES_TIMEOUT=10 -t "$IMAGE:ci-$BUILD_NUMBER" .' } }
    stage('Publish Image') {
      when { expression { return params.PUBLISH_IMAGES } }
      steps { withCredentials([usernamePassword(credentialsId: 'dockerhub-johnnyip', usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_TOKEN')]) { sh '''set -eu
      printf '%s' "$DOCKER_TOKEN" | docker login -u "$DOCKER_USER" --password-stdin
      docker tag "$IMAGE:ci-$BUILD_NUMBER" "$IMAGE:latest"
      docker tag "$IMAGE:ci-$BUILD_NUMBER" "$IMAGE:$SHORT_SHA"
      docker push "$IMAGE:latest"
      docker push "$IMAGE:$SHORT_SHA"
      ''' } }
    }
  }
  post { always { step([$class: 'Mailer', recipients: 'johnny@johnnyip.com', notifyEveryUnstableBuild: false, sendToIndividuals: false]); sh 'docker logout >/dev/null 2>&1 || true; docker image rm "$IMAGE:ci-$BUILD_NUMBER" 2>/dev/null || true'; cleanWs deleteDirs: true, disableDeferredWipeout: true, notFailBuild: true } }
}
