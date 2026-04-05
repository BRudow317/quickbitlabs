sobj = "Account"
entity_query = f"""SELECT Id, DurableId, QualifiedapiName, 
DeveloperName, MasterLabel, NamespacePrefix, 
EditUrl, NewUrl, DetailUrl, EditDefinitionUrl,
IsCustomizable, IsRetrievable, IsQueryable, 
IsSearchable, IsReplicatable, IsEverCreatable,
IsEverUpdatable, IsEverDeletable, IsDeprecatedAndHidden, 
IsInterface, ImplementsInterfaces, ImplementedBy, ExtendsInterfaces, ExtendsInterfaces, ExtendedBy,
DefaultImplementation, IsTriggerable, IsCustomSetting, 
IsCustomSetting
FROM EntityDefinition 
WHERE DeveloperName like '%{sobj}%'
"""