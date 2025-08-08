# Contributing to PySpark Transaction Processor

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/pyspark-transaction-processor.git`
3. Create a feature branch: `git checkout -b feature-name`
4. Make your changes
5. Run tests: `pytest tests/`
6. Commit changes: `git commit -m "Add feature"`
7. Push to branch: `git push origin feature-name`
8. Create a Pull Request

## Development Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install pytest flake8 black

# Run tests
pytest tests/ -v

# Format code
black src/ tests/

# Lint code
flake8 src/ tests/
```

## Code Standards

- Follow PEP 8 style guidelines
- Add docstrings to all functions
- Write tests for new features
- Keep functions focused and small
- Use meaningful variable names

## Testing

- Write unit tests for all new code
- Ensure all tests pass before submitting PR
- Include integration tests for major features
- Test with different Python versions (3.8, 3.9, 3.10)

## Pull Request Process

1. Update documentation if needed
2. Add tests for new functionality
3. Ensure CI pipeline passes
4. Request review from maintainers
5. Address feedback promptly

## Reporting Issues

- Use GitHub Issues for bug reports
- Include steps to reproduce
- Provide system information
- Include relevant logs (without sensitive data)

## Questions?

Contact: hiring@devdolphins.com