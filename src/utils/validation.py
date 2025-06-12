# Check if query won't break anything
ALLOWED_KEYWORDS = ("SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN")

def check_query(query: str) -> bool:
    """Check if a query is read-only by examining its first word.
    
    Args:
        query (str): The SQL query to be checked.
        
    Returns:
        bool: True if the query is allowed, False otherwise.
    """
    if not query.strip():
        return False
    first_word = query.strip().split()[0].upper()
    return first_word in ALLOWED_KEYWORDS