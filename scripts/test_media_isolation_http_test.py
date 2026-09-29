import unittest
from email.message import Message
from unittest.mock import patch

import test_media_isolation_http as gate


class HttpGateSafetyTests(unittest.TestCase):
    def test_origins_reject_credentials_and_remote_plaintext(self):
        for url in ["file:///etc/passwd", "https://user:password@example.test", "http://example.test", "https://example.test/#fragment"]:
            with self.subTest(url=url), self.assertRaises(gate.GateFailure):
                gate.origin(url)
        self.assertEqual(gate.origin("http://127.0.0.1:9000/x?signature=secret"), ("http", "127.0.0.1", 9000))

    def test_redirects_are_not_automatically_followed(self):
        self.assertIsNone(gate.NoRedirect().redirect_request(None, None, 307, "redirect", {}, "https://foreign.test"))

    def test_denials_must_not_contain_success_data_or_redirect(self):
        gate.denied((403, {}, b'{"code":403,"data":null}'))
        gate.denied((500, {}, b'{"code":500,"data":null}'))
        for response in [(200, {}, b'{}'), (403, {"Location": "https://private.test"}, b'{}'), (500, {}, b'{"data":{"url":"private"}}'), (403, {}, b'{"code":200}'), (403, {}, b'not-json')]:
            with self.subTest(status=response[0]), self.assertRaises(gate.GateFailure):
                gate.denied(response)

    def test_success_requires_real_envelope(self):
        self.assertEqual(gate.successful_data((200, {}, b'{"code":200,"data":{"file_id":"fixture"}}')), {"file_id": "fixture"})
        with self.assertRaises(gate.GateFailure):
            gate.successful_data((200, {}, b'{"code":500,"data":null}'))

    def test_application_request_never_follows_storage_redirect(self):
        response = unittest.mock.MagicMock()
        response.status = 307
        response.headers = Message()
        response.headers["Location"] = "https://storage.test/object?sensitive=secret"
        response.read.return_value = b""
        response.__enter__.return_value = response
        with patch("urllib.request.build_opener") as opener:
            opener.return_value.open.return_value = response
            result = gate.request("https://api.test/files/fixture", token="fixture-bearer")
            self.assertEqual(result[0], 307)
            self.assertEqual(opener.return_value.open.call_count, 1)


if __name__ == "__main__":
    unittest.main()
