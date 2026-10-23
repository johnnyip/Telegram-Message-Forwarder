pipeline {
  agent any
  options { skipDefaultCheckout(true); disableConcurrentBuilds(); timestamps(); timeout(time: 25, unit: 'MINUTES'); buildDiscarder(logRotator(numToKeepStr: '30')) }
  parameters {
    booleanParam(name: 'PUBLISH_IMAGES', defaultValue: false, description: 'Publish a source-labelled release tag after offline validation.')
    booleanParam(name: 'PROMOTE_LATEST', defaultValue: false, description: 'Explicitly promote the validated release to latest; requires PUBLISH_IMAGES.')
  }
  environment { IMAGE = 'johnnyip/telegram-message-forwarder' }
  stages {
    stage('Checkout') { steps { script {
      def v = checkout scm
      env.SHORT_SHA = sh(script: 'git rev-parse --short=12 HEAD', returnStdout: true).trim()
      env.SOURCE_BRANCH = (env.BRANCH_NAME ?: v.GIT_BRANCH ?: env.GIT_BRANCH ?: 'main').replaceFirst(/^origin\//, '')
      currentBuild.description = "${env.SOURCE_BRANCH} @ ${env.SHORT_SHA}${params.PUBLISH_IMAGES == true ? ' · publish' : ' · validate'}"
    } } }
    stage('Release Metadata') { steps { script {
      env.PUBLISH_ENABLED = (params.PUBLISH_IMAGES == true).toString()
      env.PROMOTE_LATEST_ENABLED = (params.PROMOTE_LATEST == true).toString()
      if (env.PROMOTE_LATEST_ENABLED == 'true' && env.PUBLISH_ENABLED != 'true') { error('PROMOTE_LATEST requires PUBLISH_IMAGES') }
      env.SOURCE_SHA = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
      env.SOURCE_URL = sh(script: 'git config --get remote.origin.url', returnStdout: true).trim()
      env.RELEASE_TAG = "sha-${env.SOURCE_SHA}-build-${env.BUILD_NUMBER}"
    } } }
    stage('Build and Offline Tests') { steps { sh '''set -eu
      docker build --pull --build-arg TELETHON_NO_UPDATES_TIMEOUT=10 \
        --label "org.opencontainers.image.revision=$SOURCE_SHA" \
        --label "org.opencontainers.image.source=$SOURCE_URL" \
        --label "org.opencontainers.image.ref.name=$SOURCE_BRANCH" \
        --label "org.openclaw.source.branch=$SOURCE_BRANCH" \
        -t "$IMAGE:ci-$BUILD_NUMBER" .
      # Dockerfile runs the offline suite while building; these checks are a second gate.
      docker run --rm --network none --entrypoint python "$IMAGE:ci-$BUILD_NUMBER" -m unittest discover -s tests -v
      docker run --rm --network none --entrypoint python "$IMAGE:ci-$BUILD_NUMBER" -m pip check
      ''' } } }
    stage('Publish Image') {
      when { expression { return params.PUBLISH_IMAGES == true } }
      steps { withCredentials([usernamePassword(credentialsId: 'dockerhub-johnnyip', usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_TOKEN')]) { sh '''set -eu
      mkdir -p .publish
      DOCKER_CONFIG=$(mktemp -d)
      export DOCKER_CONFIG
      trap 'rm -rf "$DOCKER_CONFIG"' EXIT
      printf '%s' "$DOCKER_TOKEN" | docker login -u "$DOCKER_USER" --password-stdin
      docker tag "$IMAGE:ci-$BUILD_NUMBER" "$IMAGE:$RELEASE_TAG"
      set +e
      timeout 5m docker push "$IMAGE:$RELEASE_TAG" > .publish/immutable-push.log 2>&1
      push_status=$?
      set -e
      cat .publish/immutable-push.log
      [ "$push_status" -eq 0 ] || exit "$push_status"
      docker pull "$IMAGE:$RELEASE_TAG"
      printf 'source_sha=%s\nsource_branch=%s\nrelease=%s\n' "$SOURCE_SHA" "$SOURCE_BRANCH" "$IMAGE:$RELEASE_TAG" > .publish/release-receipt.txt
      docker image inspect "$IMAGE:$RELEASE_TAG" --format 'image_id={{.Id}}{{println}}{{range .RepoDigests}}repo_digest={{.}}{{println}}{{end}}{{range $key, $value := .Config.Labels}}label_{{$key}}={{$value}}{{println}}{{end}}' >> .publish/release-receipt.txt
      grep -q "repo_digest=$IMAGE@sha256:" .publish/release-receipt.txt
      grep -q "label_org.opencontainers.image.revision=$SOURCE_SHA" .publish/release-receipt.txt
      grep -q "label_org.openclaw.source.branch=$SOURCE_BRANCH" .publish/release-receipt.txt
      ''' } }
    }
    stage('Promote Latest') {
      when { expression { return params.PUBLISH_IMAGES == true && params.PROMOTE_LATEST == true } }
      steps { withCredentials([usernamePassword(credentialsId: 'dockerhub-johnnyip', usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_TOKEN')]) { sh '''set -eu
      mkdir -p .publish
      DOCKER_CONFIG=$(mktemp -d)
      export DOCKER_CONFIG
      trap 'rm -rf "$DOCKER_CONFIG"' EXIT
      printf '%s' "$DOCKER_TOKEN" | docker login -u "$DOCKER_USER" --password-stdin
      docker tag "$IMAGE:$RELEASE_TAG" "$IMAGE:latest"
      set +e
      timeout 5m docker push "$IMAGE:latest" > .publish/latest-push.log 2>&1
      push_status=$?
      set -e
      cat .publish/latest-push.log
      [ "$push_status" -eq 0 ] || exit "$push_status"
      printf 'promoted_latest=true\n' >> .publish/release-receipt.txt
      ''' } }
    }
    stage('Publication Complete') {
      when { expression { return params.PUBLISH_IMAGES == true } }
      steps { sh '''set -eu
      printf 'completed=true\ncompleted_source_sha=%s\n' "$SOURCE_SHA" >> .publish/release-receipt.txt
      cp .publish/release-receipt.txt release-receipt.txt
      ''' }
    }
  }
  post {
    always {
      archiveArtifacts artifacts: '.publish/**,release-receipt.txt', allowEmptyArchive: true
      step([$class: 'Mailer', recipients: 'johnny@johnnyip.com', notifyEveryUnstableBuild: false, sendToIndividuals: false])
      sh 'docker logout >/dev/null 2>&1 || true; docker image rm "$IMAGE:ci-$BUILD_NUMBER" 2>/dev/null || true'
      cleanWs deleteDirs: true, disableDeferredWipeout: true, notFailBuild: true
    }
  }
}
