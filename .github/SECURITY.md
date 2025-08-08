# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it by emailing hiring@devdolphins.com.

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will respond within 48 hours and provide updates on the resolution timeline.

## Security Measures

- All sensitive data is encrypted in transit and at rest
- Environment variables are used for credentials (never hardcoded)
- Dependencies are regularly scanned for vulnerabilities
- Access controls are implemented for all external services
- Logging excludes sensitive information

## Best Practices

- Keep dependencies updated
- Use strong passwords and API keys
- Enable MFA on all cloud accounts
- Regularly rotate credentials
- Monitor access logs