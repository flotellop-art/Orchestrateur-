import unittest

from chat_agent import chat_page


class ChatPageSecurityTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_chat_shell_has_browser_security_headers(self):
        response = await chat_page()

        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["x-frame-options"], "DENY")
        self.assertEqual(response.headers["referrer-policy"], "no-referrer")
        policy = response.headers["content-security-policy"]
        for directive in (
            "default-src 'self'",
            "connect-src 'self'",
            "object-src 'none'",
            "base-uri 'none'",
            "frame-ancestors 'none'",
        ):
            with self.subTest(directive=directive):
                self.assertIn(directive, policy)


if __name__ == "__main__":
    unittest.main()
