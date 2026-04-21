# Security Policy

## Reporting Security Vulnerabilities

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them to the FlashDB maintainers:

1. **Private Security Report**: Use GitHub's private vulnerability reporting feature
   - Go to the repository's Security tab
   - Click "Report a vulnerability"
   - Provide detailed information about the vulnerability

2. **Email** (if you prefer):
   - Send details to [maintainer email if provided]
   - Include vulnerability description, affected versions, and reproduction steps
   - Include a proposed fix if possible

## What to Include

When reporting a vulnerability, please provide:

- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Potential impact
- Suggested fix (if available)

## Response Timeline

We aim to:
- Acknowledge receipt within 24 hours
- Provide initial assessment within 2 business days
- Release a fix within 7 days for critical vulnerabilities
- Provide regular updates for non-critical vulnerabilities

## Vulnerability Disclosure

After a fix is available and deployed:

1. Credit will be given to the reporter (unless they prefer anonymity)
2. A security advisory will be published
3. The fix will be released in a new version

## Security Best Practices

When using FlashDB:

- Keep dependencies updated: `go get -u ./...`
- Use the latest version of FlashDB
- Follow the security guidelines in [DOCUMENTATION.md](DOCUMENTATION.md)
- Report any suspicious activity immediately

## Scope

This security policy applies to:

- FlashDB core library
- Official implementations and examples
- Supported versions (typically the last 2 minor versions)

## Out of Scope

Security issues in:
- Third-party dependencies (report to the dependency maintainer)
- User application code
- Older unsupported versions

## Support

For questions about security:

- Check the [Security Advisories](../../security/advisories)
- Review [DOCUMENTATION.md](DOCUMENTATION.md)
- Open a discussion (not for disclosing vulnerabilities)

## Appreciation

We appreciate the security research community's efforts to improve FlashDB's security. Thank you for helping keep this project safe!
