import re

def s3_compatible_name(name):
    # Replace unsupported characters with an underscore
    safe_name = re.sub(r'[^a-zA-Z0-9\-_.\/]', '_', name)
    return safe_name


"""
Convert the given name to a format that's compatible with S3.

S3 generally supports Unicode characters in object key names. 
However, to be safe, we'll replace any character that's not alphanumeric, 
dash, underscore, period, or slash with an underscore.
"""