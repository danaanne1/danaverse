# System Design and Architecture Document - Danaverse Market Application

## Executive Summary
Danaverse is a desktop-based trading and market data analysis application built in Kotlin/Java using Swing for the UI. The system provides stock market data visualization, CSV import capabilities, and integration with the Polygon.io API for real-time market data.

## Architecture Overview

### High-Level Architecture
The application follows a **layered architecture** with clear separation of concerns:

1. **Presentation Layer**: Swing-based GUI components
2. **Application Layer**: Core application logic and services
3. **Data Layer**: Document store and data management
4. **Integration Layer**: External API integrations (Polygon.io)
5. **Utility Layer**: Common utilities and extensions

### Technology Stack
- **Language**: Kotlin 2.1 (primary), Java 21
- **UI Framework**: Swing
- **Build System**: Gradle 8.5
- **Data Serialization**: Jackson (JSON processing)
- **Persistence**: Custom MemoryMappedDocumentStore
- **Coroutines**: Kotlin Coroutines for async operations
- **Testing**: JUnit 5
- **External APIs**: Polygon.io for market data

## Module Structure

### Core Modules

#### 1. Market Module (`com.ddougher.market`)
**Purpose**: Core market data functionality and application entry point

**Key Components**:
- `Application.kt` - Main application entry point and lifecycle management
- `Constants.kt` - Application-wide constants
- `Utils.java` - Common utility functions
- `PreferencesEditor.kt` - User preferences management

**Responsibilities**:
- Application initialization and shutdown
- Preferences persistence using Java Preferences API
- Menu system and main window management
- Document store lifecycle management

#### 2. Market Application Module (`com.ddougher.market.application`)
**Purpose**: UI components and user interactions

**Key Components**:
- `StockDataBrowser.kt` - Stock data browsing interface
- `CSVImporter` - CSV data import functionality

**Features**:
- Equity list display with custom rendering
- Split-pane UI design for data browsing
- Interactive stock selection and viewing

#### 3. Market Data Module (`com.ddougher.market.data`)
**Purpose**: Data models and core business entities

**Key Components**:
- `Stocks` - Stock data management
- `Equity` - Individual stock/equity representation

#### 4. Polygon Integration Module (`com.ddougher.market.polygon`)
**Purpose**: Integration with Polygon.io API

**Key Components**:
- `BackfilTickers` - Ticker symbol backfilling functionality

**Features**:
- Common stock ticker retrieval
- API key management through preferences
- Asynchronous data fetching

#### 5. Proxamic Module (`com.ddougher.proxamic`)
**Purpose**: Custom document store and data persistence

**Key Components**:
- `MemoryMappedDocumentStore` - Custom persistence solution
- `ObservableDocumentStore` - Observable data store interface

**Features**:
- Memory-mapped file storage
- Serialization-based persistence
- Object retrieval by class type

#### 6. Supporting Modules
- **Buffer Module** (`com.ddougher.buff`) - Buffer utilities
- **Utilities Module** (`com.ddougher.util`) - General utilities
- **Remoting Module** (`com.ddougher.remoting`) - Remote communication
- **Extensions Module** (`com.ddougher.extensions`) - Kotlin extensions
- **Sidestream Module** (`com.ddougher.sidestream`) - Additional functionality

## Key Design Patterns

### 1. Model-View-Controller (MVC)
- **Model**: Data entities (Equity, Stocks) and document store
- **View**: Swing components (StockDataBrowser, main frame)
- **Controller**: Application class managing interactions

### 2. Observer Pattern
- `ObservableDocumentStore` for reactive data updates
- Swing list models for UI data binding

### 3. Factory Pattern
- Document store creation based on configuration
- UI component creation in Application.View

### 4. Strategy Pattern
- Custom cell renderers for different data types
- Pluggable import strategies (CSV, API)

## Data Flow

### Application Startup
1. Load user preferences from Java Preferences API
2. Initialize or load existing MemoryMappedDocumentStore
3. Create main UI components
4. Display main application window

### Data Import Flow
1. **CSV Import**: User initiates CSV import → CSVImporter processes file → Data stored in document store
2. **API Import**: User triggers ticker backfill → BackfilTickers queries Polygon.io → Results stored in document store

### Data Browsing Flow
1. User opens data browser
2. StockDataBrowser loads stock list from document store
3. Custom renderer displays formatted equity information
4. User selection triggers detail view updates

### Application Shutdown
1. Close document store connections
2. Serialize document store to disk
3. Save preferences
4. Dispose UI components

## Persistence Strategy

### Document Store Architecture
- **Type**: Custom MemoryMappedDocumentStore
- **Serialization**: Java Object Serialization
- **Storage**: File-based with configurable path
- **Access Pattern**: Type-based object retrieval

### Configuration Management
- **Preferences API**: Java built-in preferences system
- **Hierarchical Structure**: Organized by component (DocStore, Polygon)
- **Default Values**: Fallback configuration values

## External Integrations

### Polygon.io API
- **Purpose**: Real-time and historical market data
- **Authentication**: API key stored in preferences
- **Usage**: Ticker symbol backfilling and market data retrieval
- **Architecture**: Asynchronous calls using Kotlin coroutines

## Security Considerations

### API Key Management
- Stored in Java Preferences (platform-specific secure storage)
- Not hardcoded in application
- Configurable through preferences dialog

### Data Persistence
- Local file-based storage
- No network transmission of sensitive data
- Configurable storage location

## Scalability and Performance

### Concurrency
- Kotlin Coroutines for non-blocking operations
- GlobalScope usage for background tasks
- Swing EDT for UI operations

### Memory Management
- Memory-mapped document store for efficient data access
- Lazy loading of UI components
- Buffered I/O for serialization

### Data Organization
- Sorted ticker lists for efficient browsing
- Index-based list models for UI performance
- Type-safe document store retrieval

## Future Considerations

### Potential Improvements
1. **Database Integration**: Replace custom document store with proper database
2. **Real-time Updates**: WebSocket integration for live data
3. **Charting**: Integration of technical analysis charts
4. **Plugin Architecture**: Extensible module system
5. **Multi-threading**: Better separation of UI and data processing threads

### Architectural Enhancements
1. **Dependency Injection**: Framework like Koin or Dagger
2. **MVVM Pattern**: More sophisticated UI data binding
3. **Event Bus**: Decoupled component communication
4. **Configuration Management**: More robust configuration system

This architecture provides a solid foundation for a desktop trading application with clear separation of concerns, extensible design, and efficient data management capabilities.
