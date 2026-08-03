# Security Policy

## Reporting a Vulnerability

Please report security vulnerabilities through [GitHub's private vulnerability reporting](https://github.com/spotify/scio/security/advisories/new). This ensures the report reaches the maintainers directly and allows us to collaborate on a fix before public disclosure. We aim to acknowledge reports within 7 business days.

This policy covers the Scio library itself. Vulnerabilities in upstream dependencies such as Apache Beam should be reported to the [Apache Security Team](https://www.apache.org/security/).

If you have questions about a potential vulnerability, you can also reach out to the maintainers via [GitHub Discussions](https://github.com/spotify/scio/discussions).

## What to Expect

Confirmed vulnerabilities are handled through [GitHub Security Advisories](https://github.com/spotify/scio/security/advisories). The typical process is:

1. Maintainers triage the report and create a draft security advisory.
2. A fix is developed in a [private fork](https://docs.github.com/en/code-security/tutorials/fix-reported-vulnerabilities/collaborate-in-a-fork), keeping the vulnerability details confidential until a patch is available.
3. The fix is released in a new version of Scio.
4. The security advisory is published with credit to the reporter.
