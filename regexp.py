from re import search, match

def parse( regexp, line, method=search ):
    """
    like what method does, but allows a single line 
    """
    m = method( regexp, line )
    if m:
        return m
    return None