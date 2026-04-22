# Contributing to FlashDB

Thank you for your interest in contributing to FlashDB! This document provides guidelines and instructions for contributing.

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Be patient and helpful with new contributors

## Getting Started

### Prerequisites

- Go 1.22.2 or later
- Git
- Make (for running common tasks)

### Development Setup

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/yourusername/flash-db.git
   cd flash-db
   ```

2. Set up development dependencies:
   ```bash
   make setup
   ```

3. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Workflow

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run quick tests
make test-short
```

### Linting and Formatting

```bash
# Format code
make fmt

# Run linters
make lint

# Run linters with automatic fixes
make lint-fix

# Run go vet
make vet

# Check for vulnerabilities
make vuln
```

### Building

```bash
# Build the project
make build

# Run the project
make run

# Clean build artifacts
make clean

# Full pipeline (clean, fmt, lint, test, build)
make all
```

## Code Style Guide

### General Guidelines

- Follow the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Write clear, descriptive commit messages
- Add comments for exported functions and complex logic
- Keep functions focused and reasonably sized (aim for < 100 lines)

### Naming Conventions

- Use camelCase for variable and function names
- Use PascalCase for exported functions and types
- Use UPPER_CASE for constants
- Use descriptive names that indicate purpose

### Documentation

- Add package documentation to each package
- Document all exported functions and types
- Include examples in comments when helpful
- Update README.md if adding new features

### Error Handling

- Always check and handle errors explicitly
- Use meaningful error messages
- Wrap errors with context using `fmt.Errorf`

## Commit Guidelines

- Write clear commit messages following this format:
  ```
  <type>(<scope>): <subject>
  
  <body>
  ```

- Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`
- Scope: Package or component name
- Subject: Present tense, imperative mood
- Example:
  ```
  feat(wal): implement group commit batching
  
  Implement WAL group commit to reduce fsync overhead by batching
  multiple writes into a single fsync operation.
  ```

## Pull Request Process

1. Update code and tests
2. Run the full test suite: `make all`
3. Update documentation as needed
4. Commit with clear messages
5. Push to your fork
6. Create a pull request with:
   - Clear description of changes
   - Reference to any related issues
   - Summary of testing performed

### PR Checklist

- [ ] Tests pass locally (`make test`)
- [ ] Code is formatted (`make fmt`)
- [ ] Linters pass (`make lint`)
- [ ] No vulnerabilities (`make vuln`)
- [ ] Documentation is updated
- [ ] Commit messages are clear
- [ ] New features have corresponding tests

## Testing

### Writing Tests

- Test files should use `_test.go` suffix
- Use table-driven tests for multiple cases
- Use descriptive test names: `TestFunctionName_Scenario`
- Aim for high coverage, especially for critical paths

Example:
```go
func TestMemTable_Put(t *testing.T) {
    tests := []struct {
        name    string
        key     []byte
        value   []byte
        wantErr bool
    }{
        {"empty key", []byte(""), []byte("value"), false},
        {"valid entry", []byte("key"), []byte("value"), false},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            m := NewMemTable()
            err := m.Put(tt.key, tt.value, 1)
            if (err != nil) != tt.wantErr {
                t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

## Bug Reports

When reporting bugs, please include:

- Go version and OS
- Minimal reproducible example
- Expected vs actual behavior
- Steps to reproduce
- Any relevant logs or error messages

## Feature Requests

When suggesting features:

- Describe the use case
- Explain expected behavior
- Discuss potential implementation approach
- Consider performance implications

## Documentation

- Update README.md for user-facing changes
- Update DOCUMENTATION.md for architectural changes
- Add comments to complex code sections
- Update this CONTRIBUTING.md if needed

## Performance Considerations

- Benchmark critical paths
- Profile before optimizing
- Document performance trade-offs
- Avoid premature optimization

## Resources

- [Go Documentation](https://golang.org/doc)
- [Effective Go](https://golang.org/doc/effective_go)
- [Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [FlashDB Documentation](DOCUMENTATION.md)
- [FlashDB Wiki](https://github.com/Ari-Ghosh/flash-db/wiki)

## Questions?

- Check existing issues and discussions
- Review DOCUMENTATION.md for architecture details
- Ask in pull request discussions

Thank you for contributing to FlashDB!
