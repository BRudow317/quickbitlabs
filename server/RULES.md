# QuickBitLabs: System Rules & Architecture Guidelines

This document serves as the source of truth for contributing to the QuickBitLabs Data Integration Engine. Whether you are a human developer or an LLM assisting with code generation, **these rules are negotiable but should be followed unless there is a compelling reason to deviate.**

---

## 1. LLM & Developer Guidelines
- **Project guidance** The project is in a prototype phase, you should follow the architectural guidelines, and rules, however if there is an antipattern being implemented or a better way to implement something, you should suggest it and explain why it is better, and if the change is approved, you should implement it that way.
- **Import Rules:** Plugins should generally only import from the plugins directory and not anything higher in the project tree, to maintain the isolation of the plugin layer. The exception is the universal contract files in the root of the plugins directory (PluginModels, PluginProtocol, PluginResponse, PluginRegistry) which are designed to be imported by all plugins. Services can import from plugins if they are importing a specific plugin's facade to interact with it, but they should not import from other services or from the core API/models layer. The core API and models layer should not import from plugins or services at all to maintain a clean separation of concerns.

Anything outside of the plugin directory should typically only import the facade of a plugin if it needs to interact with that plugin, and should not import any internal components of the plugin to maintain the encapsulation and modularity of the plugin architecture.
- **Follow the Contract:** Do not invent new metadata models. If you are passing data, use `Records` (Iterable[dict]). If you are passing metadata, use `CatalogModel` or `EntityModel`. If you are querying, use `QueryModel`. 
- **Strict Typing:** All functions must have complete Python type hints. Be sure to use the types defined in `/server/plugins/PluginModels.py`, `PluginResponse.py`.
- **No Premature Defensiveness:** Do not write massive `try/except` blocks for edge cases like missing standard libraries or catastrophic OS failures. Catch expected operational errors (e.g., database timeouts, missing files) and return them cleanly via `PluginResponse.error()`.
- **Minimal Verbosity:** Keep the code lean. Overly defensive code, comments, or logging without a very good reason makes it time consuming to refactor and architect in the prototype phase.

---

## 2. The Macro Architecture: Orchestration vs. Execution

The system is strictly divided into **Orchestrators** and **Actors**.

* **Cross-System Orchestration lives in `/server/services/`.**
  
  * The Service layer acts as Air Traffic Control. It instantiates plugins via `PluginRegistry.get_plugin()`. It is **strictly forbidden** from writing SQL, crafting REST payloads, or managing file streams. It only juggles `CatalogModel` and `QueryModel` between plugins and expects `PluginResponse` objects back. It is the choreographer of data movement, but it does not know the implementation steps of any specific system.
  * **general high level system service process flow:** 
    - FastAPI endpoint routes requests to a service in `/server/services/`.
    - The high level carousel service might parse the incoming request and determine that its from the `MyCustomWebsitePlugin`, then parses the payload determining that this is a data request and the data is located in Salesforce plugin, and a JSON file a specific plugin owns.
    - The service might implement three CatalogModel objects, one for each of the data owners and one for the requester using the `get_plugin` function from the `PluginRegistry` and passes off the `CatalogModel` objects to each plugin facade calling the `get_records` function.
    - Next it might utilize the `PluginResponse` objects returned by each plugin to determine if the calls were successful, and if so, stream the records back to the requester plugin, along with the relevant `CatalogModel` metadata so the requester plugin can properly interpret the data and pass it back to the original caller in the correct format.
    - Then the the requester plugin `MyCustomWebsitePlugin` should know how to handle the incoming data and return a response to the original caller, how to combine the data, or pass back each stream separately and expect the caller knows how to handle it. 
  * **agnostic:** The key feature of the above workflow is the higher level service remains agnostic about the inner workings beyond the facade, and the inner workings of the plugin remain blind to anything above the facade. They should expect PluginResponse objects and handle them in a standardized way, without needing to know the internal workings of the plugins.
  * While a service may need to know about multiple plugins to orchestrate between them, it prevents the frontend in the example from needing to know anything about Salesforce or JSON files, and it prevents the plugins from needing to know about each other or the broader system, allowing for maximum modularity and flexibility as new plugins are added or existing ones are modified.
  * The goal is to keep the orchestration layer decoupled from the specific implementations of the plugins, allowing for maximum flexibility and maintainability as new plugins are added or existing ones are modified.
  * **kwargs:** can be used to pass plugin-specific paramters and arguments without the service layer needing to know about them, allowing for extensibility and customization of plugin behavior without coupling the service layer to specific plugin implementations.
* **Execution lives in `/server/plugins/`.**
  * Plugins are isolated sandboxes. A plugin knows how to talk to its specific system (Oracle, CSV, Salesforce) and pass that back to the facade which is the interface for the broader system. Plugins **must never** import each other. 
* *Examples:* Moving data from Salesforce to Oracle but a new object field was added by a salesforce developer, so first the service layer would implement the upsert column protocol and then pass the data. Or perhaps a service that attempts an upsert of Records to a target postgres database, and that system is down so the service redirects to the AWS dynamodb plugin as a backup.
---

## 3. Plugin Internal Anatomy

Every plugin must adhere to a strict internal layered architecture. Do not blur these lines.

### 1. The Facade (`<Plugin>.py`)
* **Rule:** Must be incredibly clean and thin.
* **Responsibility:** Strictly implements the `PluginProtocol`. It takes incoming Pydantic models and immediately routes them to the `<Plugin>Service`. It wraps all outgoing data in `PluginResponse`.
* **The Facade Menu** The facade should at minimum implement the PluginProtocol methods, but this is the minimum functionality it can provide as an interface, not the limit. The facade acts as a menu for higher level services to interact with the plugin, but it can also provide additional methods that are not part of the PluginProtocol if they are relevant to the plugin's functionality. For example, a plugin might have a method to refresh its internal metadata cache, or to perform a health check on its connection. These methods would not be part of the standard CRUD operations defined in the PluginProtocol, but they could be exposed through the facade for higher level services to utilize as needed. The key is that the facade should remain a thin layer that primarily routes calls to the service layer.
* **No Business Logic:** The facade should not contain any complex logic, data transformations, or system-specific code. It should only be responsible for receiving the standardized plugin calls and delegating them to the appropriate service methods.
* **Documentation:** The facade acts as the critical documentation layer as code. At a glance others should know the functionality provided by the plugin and how to call it.

### 2. The Service (`<Plugin>Services.py`)
* **Rule:** The Plugin Orchestrator. 
* **Responsibility:** Bridges the universal Pydantic models and the raw execution engine. It translates a `QueryModel` AST into a SQL string or a REST endpoint, and passes it down to the Engine.
* **No Direct System Calls:** The service should never directly call `oracledb`, `requests`, or file I/O. It should only interact with the Engine and Client layers.
* The service layer acts similar to a functional layer, organizing and orchestrating the steps needed to fulfill the plugin contract, but it does not perform the actual execution of commands or manage connections. It is the middleman that translates between the universal plugin protocol and the specific implementation details of the plugin's target system, which are handled by the Engine and Client layers respectively.

### 3. The Engine (`<Plugin>Engine.py`)
* **Rule:** The Muscle. Knows absolutely nothing about Pydantic, `CatalogModel`, or `PluginProtocol`.

* **Responsibility:** Executes raw commands. These commands can be orchestrated by the plugin service layer, but if there is no need to import other plugin components, this is probably the right layer to do it. If it requires type conversion or interaction with the PluginModels, the plugin service layer would be better.
* **No Pydantic Models:** The engine should not import or use any of the Pydantic models defined in `PluginModels.py`. It should only deal with raw data, SQL strings, REST payloads, and its plugin system specific types. This keeps the execution layer decoupled from the universal contract and allows it to focus solely on interacting with the target system.
* **Example:** Lets say you need to perform the upsert of column in an Oracle Database. Oracle doesn't have an upsert column command, so the plugin service layer might orchestrate the logic, such as comparing the incoming Records with the existing column type, and if there is a type mismatch, organizing a copy of the column, dropping the original, creating the new column with the old name, and new type, copying the data back, and then inserting the new records. The plugin service layer would orchestrate these steps, but the actual execution of the SQL commands to perform these operations would be handled by the plugin engine layer.

### 4. The Client (`<Plugin>Client.py`)
* **Rule:** The Vault. 
* **Responsibility:** Manages the physical connection (e.g., `oracledb` connection pools, `requests.Session`). 
* **No Floating Connection Strings:** Plugins internally handle their own connections, no passing around of credentials.

### 5. <Plugin>TypeMap.py
* **Rule:** The Translator.
* **Responsibility:** Translates between the universal data types defined in `PluginModels.py` and the system-specific data types (e.g., Oracle's VARCHAR2, Salesforce's String). This keeps type conversion logic centralized and reusable across the service layer. 
* The type map should typically only be imported by the service layer, as it is responsible for orchestrating the translation of data models and queries between the universal plugin protocol and the specific implementation details of the plugin's target system.

---