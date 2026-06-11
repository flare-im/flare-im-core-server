# Architecture Decision: Message Operation Handler

## Context
We needed to refactor the message operation handling in the message orchestrator to follow DDD+CQRS principles, moving message operation logic from the interface layer to the application layer.

## Decision
We decided to use a simplified architecture with a single canonical command structure in `message_operation_commands.rs` that includes both the application command definitions and the conversion methods from protobuf requests.

### Key Components

#### 1. MessageOperationCommands (Single Canonical Structure)
- Contains all message operation command definitions
- Includes `from_request()` methods for converting protobuf requests to application commands
- Eliminates redundancy of having separate app-layer commands

#### 2. MessageOperationHandler
- Handles all message operation business logic
- Directly processes protobuf requests by converting them to application commands
- Maintains separation of concerns between interface and application layers

#### 3. OperationMessageBuilder
- Builds operation messages from requests
- Provides methods for creating operation messages for different types of operations

## Rationale
- **Simplified Architecture**: Eliminated the dual command structure that would have increased complexity
- **Single Source of Truth**: Commands defined once with conversion methods in the same module
- **Reduced Maintenance**: Fewer files to maintain and keep synchronized
- **Clean Separation**: Interface layer (handler.rs) only handles protocol conversion; application layer handles business logic

## Benefits
1. Reduced cognitive load on developers
2. Decreased maintenance overhead
3. Consistent command structure across the application
4. Maintained DDD+CQRS principles
5. Proper separation between interface and application layers

## Migration Path
The implementation follows the simplified architecture pattern where:
1. Interface layer receives protobuf requests
2. Application commands are created directly from protobuf requests using `from_request()` methods
3. Business logic is handled in the application layer
4. Results are propagated back through the interface layer

## Industry Best Practices Alignment
This approach aligns with patterns used by large-scale IM systems like WhatsApp and Telegram, where canonical data models are used throughout the system with adapters for different protocol layers.