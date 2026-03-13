# Markdown & HTML Converters

def markdown_to_html(
    md_content: str,
    extensions: Optional[List[str]] = None,
    safe_mode: bool = False
) -> str:
    """
    18. markdown_to_html - Convert Markdown to HTML
    
    Args:
        md_content: Markdown string
        extensions: Markdown extensions to enable
        safe_mode: Escape raw HTML in input
    
    Returns:
        str: HTML string
    
    Example:
        html = markdown_to_html("# Hello\\n\\nThis is **bold**")
        html = markdown_to_html(md, extensions=["tables", "fenced_code"])
    """
    try:
        import markdown
    except ImportError:
        install_package("markdown")
        import markdown
    
    default_extensions = [
        "tables",
        "fenced_code",
        "codehilite",
        "toc",
        "nl2br",
        "sane_lists",
    ]
    
    extensions = extensions or default_extensions
    
    # Filter to only available extensions
    available_extensions = []
    for ext in extensions:
        try:
            markdown.markdown("test", extensions=[ext])
            available_extensions.append(ext)
        except Exception:
            pass
    
    md = markdown.Markdown(extensions=available_extensions)
    
    if safe_mode:
        md_content = md_content.replace("<", "&lt;").replace(">", "&gt;")
    
    return md.convert(md_content)


def html_to_markdown(
    html_content: str,
    strip_tags: Optional[List[str]] = None
) -> str:
    """
    19. html_to_markdown - Convert HTML to Markdown
    
    Args:
        html_content: HTML string
        strip_tags: Tags to completely remove
    
    Returns:
        str: Markdown string
    
    Example:
        md = html_to_markdown("<h1>Hello</h1><p>This is <strong>bold</strong></p>")
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        install_package("beautifulsoup4")
        from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Remove unwanted tags
    if strip_tags:
        for tag in strip_tags:
            for element in soup.find_all(tag):
                element.decompose()
    
    # Remove script and style tags by default
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    
    def convert_element(element) -> str:
        if element.name is None:
            return element.string or ""
        
        children = "".join(convert_element(child) for child in element.children)
        
        tag = element.name.lower()
        
        # Headings
        if tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
            level = int(tag[1])
            return f"\n{'#' * level} {children.strip()}\n\n"
        
        # Paragraphs
        if tag == "p":
            return f"\n{children.strip()}\n\n"
        
        # Bold
        if tag in ["strong", "b"]:
            return f"**{children}**"
        
        # Italic
        if tag in ["em", "i"]:
            return f"*{children}*"
        
        # Code
        if tag == "code":
            if element.parent and element.parent.name == "pre":
                return children
            return f"`{children}`"
        
        # Pre/code blocks
        if tag == "pre":
            code = element.find("code")
            lang = ""
            if code and code.get("class"):
                for cls in code.get("class", []):
                    if cls.startswith("language-"):
                        lang = cls[9:]
                        break
            content = code.get_text() if code else children
            return f"\n```{lang}\n{content.strip()}\n```\n\n"
        
        # Links
        if tag == "a":
            href = element.get("href", "")
            return f"[{children}]({href})"
        
        # Images
        if tag == "img":
            src = element.get("src", "")
            alt = element.get("alt", "")
            return f"![{alt}]({src})"
        
        # Lists
        if tag == "ul":
            return "\n" + children + "\n"
        if tag == "ol":
            return "\n" + children + "\n"
        if tag == "li":
            parent = element.parent
            if parent and parent.name == "ol":
                index = list(parent.children).index(element) + 1
                return f"{index}. {children.strip()}\n"
            return f"- {children.strip()}\n"
        
        # Blockquote
        if tag == "blockquote":
            lines = children.strip().split("\n")
            return "\n" + "\n".join(f"> {line}" for line in lines) + "\n\n"
        
        # Horizontal rule
        if tag == "hr":
            return "\n---\n\n"
        
        # Line break
        if tag == "br":
            return "  \n"
        
        # Divs and spans - just return content
        if tag in ["div", "span", "article", "section", "main"]:
            return children
        
        return children
    
    result = convert_element(soup)
    
    # Clean up extra whitespace
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result.strip()


def markdown_to_html_doc(
    md_content: str,
    title: str = "Document",
    css: Optional[str] = None
) -> str:
    """
    markdown_to_html_doc - Convert Markdown to complete HTML document
    
    Args:
        md_content: Markdown string
        title: Document title
        css: Optional custom CSS
    
    Returns:
        str: Complete HTML document
    
    Example:
        html_doc = markdown_to_html_doc(readme_content, title="README")
    """
    default_css = """
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            color: #333;
        }
        pre {
            background: #f4f4f4;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
        }
        code {
            background: #f4f4f4;
            padding: 2px 5px;
            border-radius: 3px;
            font-family: 'Consolas', 'Monaco', monospace;
        }
        pre code {
            background: none;
            padding: 0;
        }
        blockquote {
            border-left: 4px solid #ddd;
            margin: 0;
            padding-left: 20px;
            color: #666;
        }
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background: #f4f4f4;
        }
    """
    
    html_body = markdown_to_html(md_content)
    css = css or default_css
    
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
{css}
    </style>
</head>
<body>
{html_body}
</body>
</html>"""


def extract_markdown_toc(md_content: str) -> List[Dict[str, Any]]:
    """
    extract_markdown_toc - Extract table of contents from Markdown
    
    Args:
        md_content: Markdown string
    
    Returns:
        List of heading dicts with level, text, and slug
    
    Example:
        toc = extract_markdown_toc(readme)
        for item in toc:
            print("  " * item["level"] + item["text"])
    """
    headings = []
    
    # Match ATX-style headings (# Heading)
    pattern = r"^(#{1,6})\s+(.+)$"
    
    for line in md_content.split("\n"):
        match = re.match(pattern, line.strip())
        if match:
            level = len(match.group(1))
            text = match.group(2).strip()
            # Generate slug
            slug = re.sub(r"[^\w\s-]", "", text.lower())
            slug = re.sub(r"[-\s]+", "-", slug).strip("-")
            
            headings.append({
                "level": level,
                "text": text,
                "slug": slug
            })
    
    return headings