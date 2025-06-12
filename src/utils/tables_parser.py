from collections import defaultdict
import json
from utils.logger import get_logger


log = get_logger()
def parse_tables(query_results) -> defaultdict:
    """
       Parsing "information_schema.columns" query response into dict.
    """
    
    # Create a 3-level nested defaultdict
    def _nested_dict():
        return defaultdict(_nested_dict)

    db_structure = _nested_dict()

    try:
        for schema, table, column, dtype, nullable, char_len in query_results:
            db_structure[schema][table][column] = {
                "data_type": dtype,
                "is_nullable": nullable,
                "character_maximum_length": char_len
            }
        return json.loads(json.dumps(db_structure))
    except Exception as e:
        log.error(e)
