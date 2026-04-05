# Unit Test Strategy

## 1. Test Layout
tests/
  - unit/
  - integration/
  - fixtures/
  - data/

## 2. What to Test
- Pure functions → full coverage
- Services → mock external dependencies
- API → use test client
- CLI → use CliRunner or subprocess

## 3. Test Style
- pytest
- Arrange / Act / Assert
- Parametrize where possible
- Use fixtures for setup
- Avoid mocking internal logic

## 4. Coverage Goals
- 90%+ for utils and core
- 70%+ for services
- 100% for critical functions

## 5. Test Generation Prompts
- “Generate pytest tests for this module”
- “Add edge cases”
- “Add failure mode tests”
- “Add integration tests for this service”
