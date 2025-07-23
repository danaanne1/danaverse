# Danaverse

A Grid Server for Conversational Grid Computing, and Other things

## Project Migration: Maven to Gradle

This project has been migrated from Maven to Gradle build system. 

### Setup Instructions

1. To initialize the Gradle wrapper, run:
   ```
   gradle wrapper
   ```
   or use an existing Gradle installation:
   ```
   gradle wrapper --gradle-version 8.5
   ```

2. Build the project:
   ```
   ./gradlew build
   ```

3. Run the application:
   ```
   ./gradlew run
   ```

### Project Structure

- Source code: `src/`
- Test code: `tst/`

### Notes

- The Gradle build now uses Kotlin DSL instead of Groovy
- JVM target is set to 1.8 (same as the Maven configuration)
- All Maven dependencies have been migrated to Gradle format
