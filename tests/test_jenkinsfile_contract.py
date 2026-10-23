"""Static safety contracts for Jenkins image publication."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = (ROOT / "Jenkinsfile").read_text()
README = (ROOT / "README.md").read_text()


class JenkinsPublicationContracts(unittest.TestCase):
    def test_latest_is_opt_in_and_legacy_missing_parameter_fails_closed(self):
        self.assertRegex(PIPELINE, r"booleanParam\(name: 'PROMOTE_LATEST', defaultValue: false")
        self.assertIn("params.PROMOTE_LATEST == true", PIPELINE)
        self.assertIn("env.PROMOTE_LATEST_ENABLED = (params.PROMOTE_LATEST == true).toString()", PIPELINE)
        self.assertIn("params.PUBLISH_IMAGES == true && params.PROMOTE_LATEST == true", PIPELINE)
        self.assertIn("if (env.PROMOTE_LATEST_ENABLED == 'true' && env.PUBLISH_ENABLED != 'true')", PIPELINE)
        self.assertLess(PIPELINE.index("stage('Publish Image')"), PIPELINE.index("stage('Promote Latest')"))

    def test_full_source_provenance_and_receipts_are_required(self):
        self.assertIn("git rev-parse HEAD", PIPELINE)
        self.assertIn("sha-\u0024{env.SOURCE_SHA}-build-\u0024{env.BUILD_NUMBER}", PIPELINE)
        for label in (
            "org.opencontainers.image.revision=$SOURCE_SHA",
            "org.opencontainers.image.source=$SOURCE_URL",
            "org.opencontainers.image.ref.name=$SOURCE_BRANCH",
            "org.openclaw.source.branch=$SOURCE_BRANCH",
        ):
            self.assertIn(label, PIPELINE)
        self.assertIn("repo_digest=", PIPELINE)
        self.assertIn("image_id=", PIPELINE)
        self.assertIn("completed_source_sha=%s", PIPELINE)
        self.assertIn("archiveArtifacts artifacts: '.publish/**,release-receipt.txt'", PIPELINE)
        self.assertIn("grep -q", PIPELINE)

    def test_publish_keeps_credentials_out_of_receipt_and_bounds_push(self):
        self.assertIn("credentialsId: 'dockerhub-johnnyip'", PIPELINE)
        self.assertIn("--password-stdin", PIPELINE)
        self.assertIn("DOCKER_CONFIG=$(mktemp -d)", PIPELINE)
        self.assertIn("trap 'rm -rf \"$DOCKER_CONFIG\"' EXIT", PIPELINE)
        self.assertIn("timeout 5m docker push", PIPELINE)
        self.assertIn("push_status=$?", PIPELINE)
        receipt_writers = ''.join(line for line in PIPELINE.splitlines() if '.publish/release-receipt.txt' in line)
        self.assertNotIn("DOCKER_TOKEN", receipt_writers)

    def test_readme_describes_completion_and_fail_closed_latest(self):
        self.assertIn("PROMOTE_LATEST=false", README)
        self.assertIn("missing legacy persisted", README)
        self.assertIn("full-SHA revision", README)
        self.assertIn("completion marker", README)
        self.assertIn("timeout", README)


if __name__ == "__main__":
    unittest.main()
