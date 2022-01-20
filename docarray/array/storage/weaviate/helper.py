import uuid

def wmap(doc_id: str):
    # TODO: although rare, it's possible for key collision to occur with SHA-1
    return str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id))
