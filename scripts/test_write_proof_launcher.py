"""Local launcher must use the same canonical DER contract as the Rust services."""
import base64
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

class WriteProofLauncher(unittest.TestCase):
    def test_generated_and_supplied_keys_and_rejected_mismatch(self):
        source = Path(__file__).with_name("start_server.sh").read_text()
        function = source[source.index("load_write_proof_keys() {"):source.index("\n# Push Server", source.index("load_write_proof_keys() {"))]
        with tempfile.TemporaryDirectory() as directory:
            def run(private=None, public=None):
                env = dict(os.environ, LOGS_DIR=directory)
                env.pop("FLARE_WRITE_PROOF_PRIVATE_KEY", None)
                env.pop("FLARE_WRITE_PROOF_PUBLIC_KEY", None)
                if private is not None: env["FLARE_WRITE_PROOF_PRIVATE_KEY"] = private
                if public is not None: env["FLARE_WRITE_PROOF_PUBLIC_KEY"] = public
                return subprocess.run(["bash", "-c", function + '\nload_write_proof_keys || exit 1\nprintf "%s\\n%s\\n" "$CORE_WRITE_PROOF_PRIVATE_KEY" "$CORE_WRITE_PROOF_PUBLIC_KEY"'], env=env, capture_output=True, text=True)
            generated = run()
            self.assertEqual(generated.returncode, 0, generated.stderr)
            private, public = generated.stdout.splitlines()
            for encoded, arguments in ((private, []), (public, ["-pubin"])):
                der = base64.b64decode(encoded, validate=True)
                verified = subprocess.run(["openssl", "pkey", "-inform", "DER", *arguments, "-text", "-noout"], input=der, capture_output=True)
                self.assertEqual(verified.returncode, 0)
                self.assertIn(b"ED25519", verified.stdout)
            self.assertEqual(run(private, public).returncode, 0)
            self.assertNotEqual(run(private, public + "\n").returncode, 0)
            self.assertNotEqual(run(private, None).returncode, 0)
            subprocess.run(["openssl", "genpkey", "-algorithm", "Ed25519", "-out", directory + "/other.pem"], check=True, capture_output=True)
            other = subprocess.check_output(["openssl", "pkey", "-in", directory + "/other.pem", "-pubout", "-outform", "DER"])
            self.assertNotEqual(run(private, base64.b64encode(other).decode()).returncode, 0)

if __name__ == "__main__": unittest.main()
