# Danaverse Project Guidelines

## Project Overview

Danaverse is a desktop-based trading and market data analysis application built in Kotlin/Java using Swing for the UI. The system provides stock market data visualization, CSV import capabilities, and integration with the Polygon.io API for real-time market data.

### Technology Stack
- **Languages**: Kotlin 2.1 (primary), Java 21
- **UI Framework**: Swing
- **Build System**: Gradle 8.5
- **Data Serialization**: Jackson (JSON processing)
- **Persistence**: Custom MemoryMappedDocumentStore
- **Asynchronous Operations**: Kotlin Coroutines
- **Testing**: JUnit 5
- **External APIs**: Polygon.io for market data

### Project Structure
- **src/com/ddougher/** - Main source code
  - **market/** - Core market data functionality and application entry point
  - **market/application/** - UI components and user interactions
  - **market/data/** - Data models and core business entities
  - **market/polygon/** - Integration with Polygon.io API
  - **proxamic/** - Custom document store and data persistence
  - **buff/** - Buffer utilities
  - **util/** - General utilities
  - **remoting/** - Remote communication
  - **extensions/** - Kotlin extensions
  - **sidestream/** - Additional functionality
- **tst/com/ddougher/** - Test code
- **documentation/** - Project documentation

## Guidelines for Junie

### Code Style

1. **Language Preference**: Use Kotlin for new code when possible, Java is acceptable for compatibility with existing components.
2. **Naming Conventions**:
   - Use camelCase for variables, methods, and functions
   - Use PascalCase for classes and interfaces
   - Use UPPER_SNAKE_CASE for constants
3. **Documentation**: Add KDoc/JavaDoc comments for public APIs and complex functionality.
4. **Null Safety**: Leverage Kotlin's null safety features; avoid nullable types when possible.
5. **Coroutines**: Use coroutines for asynchronous operations instead of callbacks or threads.

### Testing Guidelines

1. **Basic Testing**: Write tests for critical functionality only.
2. **Test Location**: Place tests in the corresponding package under the `tst` directory.
3. **Running Tests**: Use the `run_test` command to execute tests for modified components.

### Build Process

1. **Build System**: The project uses Gradle for building and dependency management.
2. **Building the Project**: Run the `build` command to compile the project.
3. **Running Tests**: Use `run_test` to execute specific tests or test classes.
4. **Dependencies**: Add new dependencies to the `build.gradle.kts` file when necessary.

### Memory Mapped Document Store

When working with the Memory Mapped Document Store:

1. **Document Lifecycle**: Follow the proper document lifecycle (creation, retrieval, modification, persistence).
2. **Transactions**: Use transactions for multi-step operations to ensure data consistency.
3. **Observers**: Register and unregister observers properly to avoid memory leaks.
4. **Performance**: Be mindful of serialization costs; batch related document updates when possible.

### UI Components

When modifying UI components:

1. **Swing EDT**: Ensure UI updates happen on the Event Dispatch Thread.
2. **MVC Pattern**: Follow the Model-View-Controller pattern for UI components.
3. **Reactive Updates**: Use the Observable pattern for UI updates based on data changes.

### Documentation Guidelines

1. **Code Examples**: When including code examples in documentation:
   - Use collapsible sections with summary statements for code blocks
   - Provide a concise summary of what the code does in the summary tag
   - Include the full code implementation in the collapsible section
   - Format code blocks with appropriate language syntax highlighting
2. **Markdown Format**: Use GitHub-flavored Markdown for all documentation files.
3. **Documentation Structure**: Follow a consistent structure with clear headings and subheadings.
4. **Diagrams**: Include diagrams when they help clarify complex concepts or architectures.

### Making Changes

1. **Minimal Changes**: Make the minimal necessary changes to address the issue.
2. **Compatibility**: Ensure changes are compatible with existing code.
3. **Documentation**: Update documentation when making architectural changes.
4. **Testing**: Run relevant tests after making changes to verify functionality.
5. **Edge Cases**: Consider and handle edge cases in your implementation.
6. **Guidelines Updates**: Update the guidelines document whenever new unique functionality is added to the project.

### Submitting Changes

1. **Verification**: Verify your changes work as expected before submitting.
2. **Summary**: Provide a clear summary of the changes made.
3. **Status**: Indicate the final status of the issue.
4. **Submit**: Use the `submit` command to provide the complete response.