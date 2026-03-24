# FORMAT CONVERSION - JSON, XML, YAML, TOML, CSV, EXCEL

def to_json(
    data: Any,
    pretty: bool = True,
    ensure_ascii: bool = False,
    default: Optional[Callable] = None
) -> str:
    """
    5. to_json - Convert any data to JSON string
    
    Args:
        data: Data to convert
        pretty: Pretty print with indentation
        ensure_ascii: Escape non-ASCII characters
        default: Custom serializer for unknown types
    
    Returns:
        str: JSON string
    
    Example:
        json_str = to_json({"name": "John", "age": 30})
        json_str = to_json(my_dataclass)
    """
    def default_serializer(obj):
        if default:
            try:
                return default(obj)
            except TypeError:
                pass
        
        if is_dataclass(obj):
            return asdict(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode()
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, Path):
            return str(obj)
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
    
    return json.dumps(
        data,
        indent=2 if pretty else None,
        ensure_ascii=ensure_ascii,
        default=default_serializer
    )


def from_json(
    json_str: str,
    cls: Optional[Type] = None
) -> Any:
    """
    6. from_json - Parse JSON string to Python object
    
    Args:
        json_str: JSON string to parse
        cls: Optional class to instantiate (dataclass or regular class)
    
    Returns:
        Parsed object
    
    Example:
        data = from_json('{"name": "John"}')
        user = from_json(json_str, User)
    """
    data = json.loads(json_str)
    
    if cls is not None:
        if is_dataclass(cls):
            return cls(**data)
        elif hasattr(cls, "from_dict"):
            return cls.from_dict(data)
        else:
            obj = cls.__new__(cls)
            obj.__dict__.update(data)
            return obj
    
    return data


def json_to_xml(
    json_data: Union[str, Dict],
    root_name: str = "root",
    pretty: bool = True
) -> str:
    """
    7. json_to_xml - Convert JSON to XML
    
    Args:
        json_data: JSON string or dict
        root_name: Name of root XML element
        pretty: Pretty print output
    
    Returns:
        str: XML string
    
    Example:
        xml = json_to_xml({"user": {"name": "John", "age": 30}})
    """
    try:
        import xmltodict
    except ImportError:
        install_package("xmltodict")
        import xmltodict
    
    if isinstance(json_data, str):
        json_data = json.loads(json_data)
    
    # Wrap in root element
    wrapped = {root_name: json_data}
    
    xml_str = xmltodict.unparse(wrapped, pretty=pretty)
    return xml_str


def xml_to_json(
    xml_str: str,
    strip_root: bool = True
) -> Union[Dict, str]:
    """
    8. xml_to_json - Convert XML to JSON/dict
    
    Args:
        xml_str: XML string
        strip_root: Remove root element wrapper
    
    Returns:
        dict or JSON string
    
    Example:
        data = xml_to_json('<root><name>John</name></root>')
    """
    try:
        import xmltodict
    except ImportError:
        install_package("xmltodict")
        import xmltodict
    
    data = xmltodict.parse(xml_str)
    
    if strip_root and len(data) == 1:
        data = list(data.values())[0]
    
    return data


def json_to_yaml(
    json_data: Union[str, Dict],
    default_flow_style: bool = False
) -> str:
    """
    9. json_to_yaml - Convert JSON to YAML
    
    Args:
        json_data: JSON string or dict
        default_flow_style: Use flow style (inline) formatting
    
    Returns:
        str: YAML string
    
    Example:
        yaml_str = json_to_yaml({"database": {"host": "localhost", "port": 5432}})
    """
    try:
        import yaml
    except ImportError:
        install_package("pyyaml")
        import yaml
    
    if isinstance(json_data, str):
        json_data = json.loads(json_data)
    
    return yaml.dump(json_data, default_flow_style=default_flow_style, sort_keys=False)


def yaml_to_json(yaml_str: str) -> Dict:
    """
    10. yaml_to_json - Convert YAML to JSON/dict
    
    Args:
        yaml_str: YAML string
    
    Returns:
        dict
    
    Example:
        data = yaml_to_json("name: John\\nage: 30")
    """
    try:
        import yaml
    except ImportError:
        install_package("pyyaml")
        import yaml
    
    return yaml.safe_load(yaml_str)


def json_to_toml(json_data: Union[str, Dict]) -> str:
    """
    11. json_to_toml - Convert JSON to TOML
    
    Args:
        json_data: JSON string or dict
    
    Returns:
        str: TOML string
    
    Example:
        toml_str = json_to_toml({"server": {"host": "0.0.0.0", "port": 8000}})
    """
    try:
        import toml
    except ImportError:
        install_package("toml")
        import toml
    
    if isinstance(json_data, str):
        json_data = json.loads(json_data)
    
    return toml.dumps(json_data)


def toml_to_json(toml_str: str) -> Dict:
    """
    12. toml_to_json - Convert TOML to JSON/dict
    
    Args:
        toml_str: TOML string
    
    Returns:
        dict
    
    Example:
        data = toml_to_json('[server]\\nhost = "localhost"')
    """
    try:
        import toml
    except ImportError:
        install_package("toml")
        import toml
    
    return toml.loads(toml_str)


def json_to_csv(
    json_data: Union[str, List[Dict]],
    output_file: Optional[str] = None,
    delimiter: str = ","
) -> str:
    """
    13. json_to_csv - Convert JSON array to CSV
    
    Args:
        json_data: JSON string or list of dicts
        output_file: Optional file path to write
        delimiter: CSV delimiter
    
    Returns:
        str: CSV string
    
    Example:
        csv_str = json_to_csv([{"name": "John", "age": 30}, {"name": "Jane", "age": 25}])
    """
    if isinstance(json_data, str):
        json_data = json.loads(json_data)
    
    if not json_data:
        return ""
    
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=json_data[0].keys(), delimiter=delimiter)
    writer.writeheader()
    writer.writerows(json_data)
    
    csv_str = output.getvalue()
    
    if output_file:
        with open(output_file, "w", newline="") as f:
            f.write(csv_str)
    
    return csv_str


def csv_to_json(
    csv_data: str,
    delimiter: str = ",",
    has_header: bool = True
) -> List[Dict]:
    """
    14. csv_to_json - Convert CSV to JSON/list of dicts
    
    Args:
        csv_data: CSV string or file path
        delimiter: CSV delimiter
        has_header: Whether first row is header
    
    Returns:
        List of dicts
    
    Example:
        data = csv_to_json("name,age\\nJohn,30\\nJane,25")
    """
    # Check if it's a file path
    if os.path.exists(csv_data):
        with open(csv_data, "r") as f:
            csv_data = f.read()
    
    reader = csv.DictReader(StringIO(csv_data), delimiter=delimiter)
    return list(reader)


def json_to_excel(
    json_data: Union[str, List[Dict], Dict[str, List[Dict]]],
    output_file: str,
    sheet_name: str = "Sheet1"
) -> str:
    """
    15. json_to_excel - Convert JSON to Excel file
    
    Args:
        json_data: JSON data (list for single sheet, dict for multiple sheets)
        output_file: Output Excel file path
        sheet_name: Default sheet name for list input
    
    Returns:
        str: Output file path
    
    Example:
        json_to_excel(users, "users.xlsx")
        json_to_excel({"users": users, "orders": orders}, "data.xlsx")
    """
    try:
        import pandas as pd
    except ImportError:
        install_package("pandas")
        install_package("openpyxl")
        import pandas as pd
    
    if isinstance(json_data, str):
        json_data = json.loads(json_data)
    
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        if isinstance(json_data, list):
            df = pd.DataFrame(json_data)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        elif isinstance(json_data, dict):
            # Check if it's a multi-sheet structure
            if all(isinstance(v, list) for v in json_data.values()):
                for sheet, data in json_data.items():
                    df = pd.DataFrame(data)
                    df.to_excel(writer, sheet_name=sheet, index=False)
            else:
                # Single dict, convert to single row
                df = pd.DataFrame([json_data])
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    return output_file


def excel_to_json(
    excel_file: str,
    sheet_name: Optional[str] = None,
    all_sheets: bool = False
) -> Union[List[Dict], Dict[str, List[Dict]]]:
    """
    16. excel_to_json - Convert Excel file to JSON
    
    Args:
        excel_file: Excel file path
        sheet_name: Specific sheet to read
        all_sheets: Read all sheets into dict
    
    Returns:
        List of dicts (single sheet) or dict of lists (all sheets)
    
    Example:
        data = excel_to_json("data.xlsx")
        all_data = excel_to_json("data.xlsx", all_sheets=True)
    """
    try:
        import pandas as pd
    except ImportError:
        install_package("pandas")
        install_package("openpyxl")
        import pandas as pd
    
    if all_sheets:
        xl = pd.ExcelFile(excel_file)
        result = {}
        for sheet in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sheet)
            result[sheet] = df.to_dict(orient="records")
        return result
    else:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        return df.to_dict(orient="records")


def convert_format(
    content: str,
    from_format: str,
    to_format: str,
    **kwargs
) -> str:
    """
    convert_format - Universal format converter
    
    Args:
        content: Content string
        from_format: Source format (json, xml, yaml, toml, csv)
        to_format: Target format (json, xml, yaml, toml, csv)
        **kwargs: Additional arguments for converters
    
    Returns:
        str: Converted content
    
    Example:
        yaml_str = convert_format(json_str, "json", "yaml")
        json_str = convert_format(xml_str, "xml", "json")
    """
    # Parse to intermediate dict
    parsers = {
        "json": json.loads,
        "xml": xml_to_json,
        "yaml": yaml_to_json,
        "yml": yaml_to_json,
        "toml": toml_to_json,
        "csv": csv_to_json,
    }
    
    serializers = {
        "json": lambda d: to_json(d, **kwargs),
        "xml": lambda d: json_to_xml(d, **kwargs),
        "yaml": lambda d: json_to_yaml(d, **kwargs),
        "yml": lambda d: json_to_yaml(d, **kwargs),
        "toml": json_to_toml,
        "csv": json_to_csv,
    }
    
    from_format = from_format.lower()
    to_format = to_format.lower()
    
    if from_format not in parsers:
        raise ValueError(f"Unsupported source format: {from_format}")
    if to_format not in serializers:
        raise ValueError(f"Unsupported target format: {to_format}")
    
    data = parsers[from_format](content)
    return serializers[to_format](data)

