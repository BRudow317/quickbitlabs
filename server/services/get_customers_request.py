# services/GetCustomersService.py

from server.models.PluginModels import CatalogModel, EntityModel, QueryModel
from plugins.oracle import Oracle

def handle_get_customers_request(tenant_id: str, limit: int = 100):
    """
    The Orchestrator Flow: 
    Takes a web request, uses universal models, asks Oracle for data, returns JSON.
    """
    
    # 1. Build the Envelope (The strict Parent->Child container)
    cust_entity = EntityModel(source_name="CUSTOMERS")
    cust_catalog = CatalogModel(source_name="SALES_DB", entities=[cust_entity])
    
    # 2. Build the Query AST (What do we actually want?)
    query = QueryModel(
        entities=["CUSTOMERS"],
        limit=limit
    )

    # 3. Instantiate the Plugin (The Facade)
    oracle_plugin = Oracle(host="...", user=tenant_id, password="...")

    try:
        # 4. Ask the plugin for the data using our strict envelope and query
        # (Assuming the facade wraps the service layer we built earlier)
        records_generator = oracle_plugin.read.get_records(
            catalog=cust_catalog, 
            model_query=query
        )
        
        # 5. Consume the generator into a list for the JSON response
        customer_data = list(records_generator)
        
        # 6. Return the standard REST response
        return {
            "status": 200,
            "message": "Success",
            "metadata": {
                "source": cust_catalog.source_name,
                "table": cust_entity.source_name,
                "row_count": len(customer_data)
            },
            "data": customer_data
        }

    except Exception as e:
        # Handle 404s, DB connection failures, etc.
        return {
            "status": 500,
            "message": f"Failed to retrieve customers: {str(e)}",
            "data": []
        }